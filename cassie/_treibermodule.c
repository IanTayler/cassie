#include <Python.h>

#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <time.h>

#define INIT_BACKOFF_NANOSECS 16
#define NANOSECONDS_IN_SECOND 1000000000
#define INIT_NODE_POOL_CAP 32
#define NODE_POOL_ENLARGE_MULTIPLIER 2
#define NO_NEXT_NODE -1

static PyObject *PyExc_queue_Empty;

typedef long node_idx_t;
typedef atomic_long atomic_node_idx_t;

typedef struct _backoff {
  long wait_time;
} Backoff;

// Return a stack-allocated Backoff.
static Backoff backoff_init(long init_wait_time) {
  Backoff backoff = {init_wait_time};
  return backoff;
}

// Wait and duplicate the waiting time for next time.
static void backoff_wait(Backoff *backoff) {
  struct timespec wait_time_arg;
  wait_time_arg.tv_sec = backoff->wait_time / NANOSECONDS_IN_SECOND;
  wait_time_arg.tv_nsec = backoff->wait_time % NANOSECONDS_IN_SECOND;
  nanosleep(&wait_time_arg, NULL);
  backoff->wait_time *= 2;
}

typedef struct _treibernode {
  // next gets reused. It's either the index to the node pool for the next
  // element in the stack or the index to the node pool for the next free node
  // in our linked list of reusable node objects.
  node_idx_t next;
  PyObject *value;
  atomic_long refcount;
} TreiberNode;

typedef struct _treiberstack {
  PyObject_HEAD atomic_node_idx_t head;
  pthread_mutex_t head_refcount_lock;
  sem_t sem;
  TreiberNode *node_pool;
  node_idx_t node_pool_head;
  node_idx_t node_pool_cap;
} TreiberStack;

static void node_incref(TreiberStack *ts, node_idx_t node_idx) {
  atomic_fetch_add(&ts->node_pool[node_idx].refcount, 1);
}

static void node_xincref(TreiberStack *ts, node_idx_t node_idx) {
  if (node_idx != NO_NEXT_NODE) {
    node_incref(ts, node_idx);
  }
}

static void node_decref(TreiberStack *ts, node_idx_t node_idx) {
  long old_value = atomic_fetch_sub(&ts->node_pool[node_idx].refcount, 1);
  if (old_value == 1) {
    // Add this node back to the linked list of free nodes.
    // Repurpose the "next" field.
    ts->node_pool[node_idx].next = ts->node_pool_head;
    ts->node_pool_head = node_idx;
  }
}

static void node_xdecref(TreiberStack *ts, node_idx_t node_idx) {
  if (node_idx != NO_NEXT_NODE) {
    node_decref(ts, node_idx);
  }
}

// Acquire the stack's lock/mutex for node refcounts.
static void treiber_stack_acquire_refcount_lock(TreiberStack *ts) {
  int mtx_code = pthread_mutex_lock(&ts->head_refcount_lock);
  if (mtx_code != 0) {
    PyErr_SetString(PyExc_RuntimeError, "pthread_mutex_lock failed.");
  }
}

// Release the stack's lock/mutex for node refcounts.
static void treiber_stack_release_refcount_lock(TreiberStack *ts) {
  int mtx_code = pthread_mutex_unlock(&ts->head_refcount_lock);
  if (mtx_code != 0) {
    PyErr_SetString(PyExc_RuntimeError, "pthread_mutex_unlock failed.");
  }
}

// node_new returns an index to the stack's node_pool for a new node in the
// stack. The head_refcount_lock should be procured before calling node_new.
static node_idx_t node_new(TreiberStack *ts, PyObject *v, node_idx_t next) {
  node_idx_t node_idx = NO_NEXT_NODE;
  // If the linked list of free nodes is empty, we realloc.
  if (ts->node_pool_head == NO_NEXT_NODE) {
    node_idx_t old_cap = ts->node_pool_cap;
    ts->node_pool_cap *= NODE_POOL_ENLARGE_MULTIPLIER;
    ts->node_pool =
        realloc(ts->node_pool, (sizeof *ts->node_pool) * ts->node_pool_cap);
    if (!ts->node_pool) {
      PyErr_NoMemory();
    }
    node_idx = old_cap;
    // Initialize the newly acquired nodes to serve as the linked list of free
    // nodes.
    ts->node_pool_head = old_cap + 1;
    for (node_idx_t i = old_cap + 1; i < ts->node_pool_cap; i++) {
      if (i == ts->node_pool_cap - 1) {
        ts->node_pool[i].next = NO_NEXT_NODE;
      } else {
        ts->node_pool[i].next = i + 1;
      }
    }
  } else {
    // If the linked list of free nodes isn't empty, just get the first one.
    node_idx = ts->node_pool_head;
    ts->node_pool_head = ts->node_pool[node_idx].next;
  }
  ts->node_pool[node_idx].refcount = 1;
  ts->node_pool[node_idx].value = v;
  ts->node_pool[node_idx].next = next;
  return node_idx;
}

// Vacate/release the stack's semaphore. Return true if successful.
static bool treiber_stack_sem_post(TreiberStack *ts) {
  int sem_code = sem_post(&ts->sem);
  if (sem_code != 0) {
    PyErr_SetString(PyExc_RuntimeError, "sem_post failed.");
    return false;
  }
  return true;
}

// Try to procure/acquire the stack's semaphore. Return true if successful.
static bool treiber_stack_sem_trywait(TreiberStack *ts) {
  int sem_code = sem_trywait(&ts->sem);
  if (sem_code != 0) {
    // We don't want to consider EAGAIN (couldn't acquire the semaphore) as an
    // error, as that's expected behaviour for trywait.
    if (errno != EAGAIN) {
      PyErr_SetString(PyExc_RuntimeError, "sem_trywait failed.");
    }
    return false;
  }
  return true;
}

// Wait to procure the stack's semaphore. Return true if successful.
static bool treiber_stack_sem_wait(TreiberStack *ts) {
  // First try once without releasing the GIL.
  if (treiber_stack_sem_trywait(ts)) {
    return true;
  }
  // If that fails, we have no choice but to release the GIL, which can be
  // expensive.
  int sem_code;
  Py_BEGIN_ALLOW_THREADS;
  sem_code = sem_wait(&ts->sem);
  Py_END_ALLOW_THREADS;
  if (sem_code != 0) {
    PyErr_SetString(PyExc_RuntimeError, "sem_wait failed.");
    return false;
  }
  return true;
}

static bool treiber_stack_sem_timedwait(TreiberStack *ts, long timeout_ns) {
  struct timespec abs_time;
  clock_gettime(CLOCK_REALTIME, &abs_time);
  // First try once without releasing the GIL.
  if (treiber_stack_sem_trywait(ts)) {
    return true;
  }
  // If that fails, we have no choice but to release the GIL, which can be
  // expensive.
  int sem_code;
  Py_BEGIN_ALLOW_THREADS;
  time_t extra_seconds = timeout_ns / NANOSECONDS_IN_SECOND;
  long extra_nanoseconds = timeout_ns % NANOSECONDS_IN_SECOND;
  abs_time.tv_sec += extra_seconds;
  abs_time.tv_nsec += extra_nanoseconds;
  if (abs_time.tv_nsec >= NANOSECONDS_IN_SECOND) {
    abs_time.tv_sec++;
    abs_time.tv_nsec -= NANOSECONDS_IN_SECOND;
  }
  sem_code = sem_timedwait(&ts->sem, &abs_time);
  Py_END_ALLOW_THREADS;
  if (sem_code != 0) {
    if (errno != ETIMEDOUT) {
      PyErr_SetString(PyExc_RuntimeError, "sem_timedwait failed.");
    }
    return false;
  }
  return true;
}

static PyObject *treiber_stack_new(PyTypeObject *type, PyObject *args,
                                   PyObject *kwds) {
  TreiberStack *self;
  self = (TreiberStack *)type->tp_alloc(type, 0);
  if (self != NULL) {
    self->head = NO_NEXT_NODE;
    self->node_pool_cap = INIT_NODE_POOL_CAP;
    self->node_pool = calloc(self->node_pool_cap, sizeof *self->node_pool);
    self->node_pool_head = 0;
    for (int i = 0; i < self->node_pool_cap; i++) {
      // The last node in the linked list points signals NO_NEXT_NODE. All
      // others signal the next node.
      if (i == self->node_pool_cap - 1) {
        self->node_pool[i].next = NO_NEXT_NODE;
      } else {
        self->node_pool[i].next = i + 1;
      }
    }
    int mtx_code = pthread_mutex_init(&self->head_refcount_lock, NULL);
    if (mtx_code != 0) {
      PyErr_SetString(PyExc_RuntimeError, "mtx_init for TreiberStack failed.");
      return NULL;
    }
    // Arguments set the semaphore as process-internal and set its initial
    // value to 0.
    int sem_code = sem_init(&self->sem, 0, 0);
    if (sem_code != 0) {
      PyErr_SetString(PyExc_RuntimeError, "sem_init for TreiberStack failed.");
      return NULL;
    }
  }
  return (PyObject *)self;
}

static void treiber_stack_dealloc(TreiberStack *self) {
  free(self->node_pool);
  pthread_mutex_destroy(&self->head_refcount_lock);
  sem_destroy(&self->sem);
  Py_TYPE(self)->tp_free((PyObject *)self);
}

void treiber_stack_push(TreiberStack *ts, PyObject *v) {
  Backoff backoff = backoff_init(INIT_BACKOFF_NANOSECS);
  treiber_stack_acquire_refcount_lock(ts);
  node_idx_t current_head_pos = ts->head;
  node_idx_t old_head_pos = current_head_pos;
  node_xincref(ts, current_head_pos);
  node_idx_t new_head_pos = node_new(ts, v, current_head_pos);
  treiber_stack_release_refcount_lock(ts);
  // The stack is implemented using CAS even if it's unnecessary for its  direct
  // usage in Python (due to the GIL). The idea is for the stack to be usable as
  // a building block for other C extensions that may want to release the GIL.
  while (!atomic_compare_exchange_weak(&ts->head, &current_head_pos,
                                       new_head_pos)) {
    // Here current_head_pos was updated to have the actual head value.
    backoff_wait(&backoff);
    treiber_stack_acquire_refcount_lock(ts);
    node_xdecref(ts, old_head_pos);
    // Update current_head_pos even though CAS already updated it because it's
    // likely to have changed again after the backoff.
    current_head_pos = ts->head;
    old_head_pos = current_head_pos;
    node_xincref(ts, current_head_pos);
    ts->node_pool[new_head_pos].next = current_head_pos;
    treiber_stack_release_refcount_lock(ts);
  }
  treiber_stack_acquire_refcount_lock(ts);
  node_xdecref(ts, current_head_pos);
  treiber_stack_release_refcount_lock(ts);
  treiber_stack_sem_post(ts);
}

static PyObject *treiber_stack_push_method(TreiberStack *self, PyObject *args) {
  PyObject *v;
  PyArg_ParseTuple(args, "O", &v);
  Py_INCREF(v);
  treiber_stack_push(self, v);
  Py_RETURN_NONE;
}

static PyObject *treiber_stack_put_method(TreiberStack *self, PyObject *args,
                                          PyObject *kwds) {
  static char *kwlist[] = {"item", "block", "timeout", NULL};
  PyObject *v, *_ignored_block, *_ignored_timeout;
  PyArg_ParseTupleAndKeywords(args, kwds, "O|OO", kwlist, &v, &_ignored_block,
                              &_ignored_timeout);
  Py_INCREF(v);
  treiber_stack_push(self, v);
  Py_RETURN_NONE;
}

typedef struct _try_pop_ret {
  PyObject *obj;
  bool was_empty;
} TryPopRet;

PyObject *try_pop_ret_to_py_pair(TryPopRet retval) {
  PyObject *was_empty_obj;
  if (retval.was_empty) {
    was_empty_obj = Py_True;
  } else {
    was_empty_obj = Py_False;
  }
  Py_INCREF(was_empty_obj);
  if (!retval.obj) {
    Py_INCREF(Py_None);
    return PyTuple_Pack(2, Py_None, was_empty_obj);
  }
  return PyTuple_Pack(2, retval.obj, was_empty_obj);
}

// Helper function used in try_pop and pop.
//
// Do not use by itself as it doesn't procure/acquire the semaphore!
static TryPopRet treiber_stack_single_pop(TreiberStack *ts) {
  node_idx_t old_head_pos = ts->head;
  if (old_head_pos == NO_NEXT_NODE) {
    // This shouldn't happen, it's an error.
    // single_pop should not be called without successfully procuring the
    // semaphore before and in that case old_head_pos should never be
    // NO_NEXT_NODE.
    PyErr_SetString(PyExc_RuntimeError,
                    "got an empty head in single_pop. Probably an error in the "
                    "implementation of the _treiber C extension.");
    TryPopRet ret = {NULL, true};
    return ret;
  }
  node_idx_t next_head_pos = ts->node_pool[old_head_pos].next;
  if (!atomic_compare_exchange_strong(&ts->head, &old_head_pos,
                                      next_head_pos)) {
    TryPopRet ret = {NULL, false};
    return ret;
  }
  treiber_stack_acquire_refcount_lock(ts);
  PyObject *return_object = ts->node_pool[old_head_pos].value;
  node_xdecref(ts, old_head_pos);
  treiber_stack_release_refcount_lock(ts);
  TryPopRet ret = {return_object, false};
  return ret;
}

TryPopRet treiber_stack_try_pop(TreiberStack *ts) {
  bool procured_sem = treiber_stack_sem_trywait(ts);
  if (!procured_sem) {
    TryPopRet ret = {NULL, true};
    return ret;
  }
  TryPopRet retval = treiber_stack_single_pop(ts);
  if (!retval.obj) {
    // If we fail getting an item, we have to vacate/signal/release the sem.
    treiber_stack_sem_post(ts);
  }
  return retval;
}

static PyObject *treiber_stack_try_pop_method(TreiberStack *self,
                                              PyObject *Py_UNUSED(ignored)) {
  TryPopRet retval = treiber_stack_try_pop(self);
  return try_pop_ret_to_py_pair(retval);
}

// Pop an item from the stack, retrying if popping fails and the stack is not
// empty.
TryPopRet treiber_stack_pop(TreiberStack *ts) {
  bool procured_sem = treiber_stack_sem_trywait(ts);
  if (!procured_sem) {
    TryPopRet ret = {NULL, true};
    return ret;
  }
  TryPopRet retval = treiber_stack_single_pop(ts);
  Backoff backoff = backoff_init(INIT_BACKOFF_NANOSECS);
  while (!retval.obj) {
    backoff_wait(&backoff);
    retval = treiber_stack_single_pop(ts);
  }
  return retval;
}

static PyObject *treiber_stack_pop_method(TreiberStack *self,
                                          PyObject *Py_UNUSED(ignored)) {
  TryPopRet retval = treiber_stack_pop(self);
  return try_pop_ret_to_py_pair(retval);
}

TryPopRet treiber_stack_pop_wait(TreiberStack *ts, bool timeout,
                                 long timeout_ns) {
  bool procured_sem;
  if (timeout) {
    procured_sem = treiber_stack_sem_timedwait(ts, timeout_ns);
  } else {
    procured_sem = treiber_stack_sem_wait(ts);
  }
  if (!procured_sem) {
    TryPopRet ret = {NULL, true};
    return ret;
  }
  TryPopRet retval = treiber_stack_single_pop(ts);
  Backoff backoff = backoff_init(INIT_BACKOFF_NANOSECS);
  while (!retval.obj) {
    backoff_wait(&backoff);
    retval = treiber_stack_single_pop(ts);
  }
  return retval;
}

static PyObject *treiber_stack_pop_wait_method(TreiberStack *self,
                                               PyObject *args) {
  bool timeout = false;
  double timeout_s = 0.0;
  PyArg_ParseTuple(args, "|pd", &timeout, &timeout_s);
  long timeout_ns = 0;
  if (timeout) {
    // Don't bother computing this if timeout is false.
    timeout_ns = (long)(timeout_s * NANOSECONDS_IN_SECOND);
  }
  TryPopRet retval = treiber_stack_pop_wait(self, timeout, timeout_ns);
  return try_pop_ret_to_py_pair(retval);
}

static PyObject *treiber_stack_get_method(TreiberStack *self, PyObject *args,
                                          PyObject *kwds) {
  static char *kwlist[] = {"block", "timeout", NULL};
  bool block = true;
  bool timeout;
  long timeout_ns = 1;
  PyObject *timeout_obj = Py_None;
  PyArg_ParseTupleAndKeywords(args, kwds, "|bO", kwlist, &block, &timeout_obj);
  TryPopRet retval;
  if (timeout_obj == Py_None) {
    timeout = false;
  } else {
    timeout = true;
    double timeout_s = PyFloat_AsDouble(timeout_obj);
    timeout_ns = (long)(timeout_s * NANOSECONDS_IN_SECOND);
  }
  if (block) {
    retval = treiber_stack_pop_wait(self, timeout, timeout_ns);
  } else {
    retval = treiber_stack_try_pop(self);
  }
  if (retval.was_empty) {
    PyErr_SetNone(PyExc_queue_Empty);
    return NULL;
  }
  return retval.obj;
}

static PyMethodDef treibermodulemethods[] = {
    {NULL, NULL, 0, NULL},
};

static struct PyModuleDef treibermodule = {
    PyModuleDef_HEAD_INIT, "_treiber", NULL, -1, treibermodulemethods,
};

static PyMethodDef treiberstackmethods[] = {
    {"pop_wait", (PyCFunction)treiber_stack_pop_wait_method, METH_VARARGS,
     "Pop, waiting if necessary. \n\nTakes a boolean timeout and a float "
     "timeout_time in seconds. Both are optional and default to False and "
     "0.0, respectively. If timeout is set to True, timeout_time should be "
     "set "
     "as well in almost all scenarios, though. If timeout is False, "
     "timeout_time is ignored."},
    {"pop", (PyCFunction)treiber_stack_pop_method, METH_NOARGS,
     "Wrapper around try_pop that retries on failure (but not if the stack "
     "is "
     "empty)."},
    {"try_pop", (PyCFunction)treiber_stack_try_pop_method, METH_NOARGS,
     "Typical treiber stack try_pop. Retuns (obj, was_empty) pair. \n\nobj "
     "is None on empty or failure. was_empty is a boolean indicating if the "
     "stack was empty when we tried popping."},
    {"push", (PyCFunction)treiber_stack_push_method, METH_VARARGS,
     "Treiber stack thread-safe push."},
    {"put", (PyCFunction)treiber_stack_put_method, METH_VARARGS | METH_KEYWORDS,
     "See push. Ignores block and timeout parameters."},
    {"get", (PyCFunction)treiber_stack_get_method, METH_VARARGS | METH_KEYWORDS,
     "Like pop_wait if block is True, like pop otherwise. Raises queue.Empty "
     "on timeout or if block=False and the queue is empty."},

    {NULL, NULL, 0, NULL},
};

static PyTypeObject TreiberStackType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_treiber.TreiberStack",
    .tp_doc = "CPython implementation of a Treiber Stack.",
    .tp_basicsize = sizeof(TreiberStack),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = treiber_stack_new,
    .tp_methods = treiberstackmethods,
    .tp_dealloc = (destructor)treiber_stack_dealloc,
};

PyMODINIT_FUNC PyInit__treiber(void) {
  PyObject *m;
  if (PyType_Ready(&TreiberStackType) < 0) {
    return NULL;
  }

  m = PyModule_Create(&treibermodule);
  if (m == NULL) {
    return NULL;
  }

  PyObject *queue_m = PyImport_ImportModule("queue");
  if (!queue_m) {
    return NULL;
  }
  PyObject *queue_m_dict = PyModule_GetDict(queue_m);
  PyExc_queue_Empty = PyDict_GetItemString(queue_m_dict, "Empty");
  Py_XDECREF(queue_m);

  Py_INCREF(&TreiberStackType);
  PyModule_AddObject(m, "TreiberStack", (PyObject *)&TreiberStackType);
  return m;
}
