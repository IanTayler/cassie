#include <Python.h>

#include <errno.h>
#include <semaphore.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <threads.h>
#include <time.h>

#define INIT_BACKOFF_NANOSECS 8
#define NANOSECONDS_IN_SECOND 1000000000

static PyObject *PyErr_queue_Empty;

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
  struct _treibernode *next;
  PyObject *value;
  atomic_long refcount;
} TreiberNode;

static TreiberNode *node_new(PyObject *v, TreiberNode *next) {
  TreiberNode *node = malloc(sizeof *node);
  if (!node) {
    PyErr_NoMemory();
  }
  node->value = v;
  node->next = next;
  node->refcount = 1;
  return node;
}

static void node_incref(TreiberNode *node) {
  atomic_fetch_add(&node->refcount, 1);
}

static void node_xincref(TreiberNode *node) {
  if (node) {
    node_incref(node);
  }
}

static void node_decref(TreiberNode *node) {
  long old_value = atomic_fetch_sub(&node->refcount, 1);
  if (old_value == 1) {
    free(node);
  }
}

static void node_xdecref(TreiberNode *node) {
  if (node) {
    node_decref(node);
  }
}

typedef struct _treiberstack {
  PyObject_HEAD _Atomic(TreiberNode *) head;
  mtx_t head_refcount_lock;
  sem_t sem;
} TreiberStack;

// Acquire the stack's lock/mutex for node refcounts.
static void treiber_stack_acquire_refcount_lock(TreiberStack *ts) {
  int mtx_code = mtx_lock(&ts->head_refcount_lock);
  if (mtx_code != thrd_success) {
    PyErr_SetString(PyExc_RuntimeError, "mtx_lock failed.");
  }
}

// Release the stack's lock/mutex for node refcounts.
static void treiber_stack_release_refcount_lock(TreiberStack *ts) {
  int mtx_code = mtx_unlock(&ts->head_refcount_lock);
  if (mtx_code != thrd_success) {
    PyErr_SetString(PyExc_RuntimeError, "mtx_unlock failed.");
  }
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
    int mtx_code = mtx_init(&self->head_refcount_lock, mtx_plain);
    if (mtx_code != thrd_success) {
      PyErr_SetString(PyExc_RuntimeError, "mtx_init for TreiberStack failed.");
      return NULL;
    }
    // Arguments set the semaphore as process-internal and set its initial value
    // to 0.
    int sem_code = sem_init(&self->sem, 0, 0);
    if (sem_code != 0) {
      PyErr_SetString(PyExc_RuntimeError, "sem_init for TreiberStack failed.");
      return NULL;
    }
  }
  return (PyObject *)self;
}

static void treiber_stack_dealloc(TreiberStack *self) {
  TreiberNode *curr_node = self->head;
  while (curr_node) {
    TreiberNode *next_node = curr_node->next;
    PyObject *value_tmp = curr_node->value;
    free(curr_node);
    Py_XDECREF(value_tmp);
    curr_node = next_node;
  }
  mtx_destroy(&self->head_refcount_lock);
  sem_destroy(&self->sem);
  Py_TYPE(self)->tp_free((PyObject *)self);
}

static void treiber_stack_push(TreiberStack *ts, PyObject *v) {
  Py_INCREF(v);
  Backoff backoff = backoff_init(INIT_BACKOFF_NANOSECS);
  treiber_stack_acquire_refcount_lock(ts);
  TreiberNode *current_head = ts->head;
  node_xincref(current_head);
  treiber_stack_release_refcount_lock(ts);
  TreiberNode *new_head = node_new(v, current_head);
  // Atomicity here may not be strictly necessary, as C code in extensions
  // should run atomically under normal circumstances.
  // But this gives us stronger guarantees in cases where some other C
  // extension released the GIL or other weird stuff is going on.
  while (!atomic_compare_exchange_weak(&ts->head, &current_head, new_head)) {
    backoff_wait(&backoff);
    treiber_stack_acquire_refcount_lock(ts);
    node_xdecref(current_head);
    current_head = ts->head;
    node_xincref(current_head);
    treiber_stack_release_refcount_lock(ts);
    new_head->next = current_head;
  }
  treiber_stack_acquire_refcount_lock(ts);
  node_xdecref(current_head);
  treiber_stack_release_refcount_lock(ts);
  treiber_stack_sem_post(ts);
}

static PyObject *treiber_stack_push_method(TreiberStack *self, PyObject *args) {
  PyObject *v;
  PyArg_ParseTuple(args, "O", &v);
  treiber_stack_push(self, v);
  Py_RETURN_NONE;
}

static PyObject *treiber_stack_put_method(TreiberStack *self, PyObject *args,
                                          PyObject *kwds) {
  static char *kwlist[] = {"item", "block", "timeout", NULL};
  PyObject *v, *_ignored_block, *_ignored_timeout;
  PyArg_ParseTupleAndKeywords(args, kwds, "O|OO", kwlist, &v, &_ignored_block,
                              &_ignored_timeout);
  treiber_stack_push(self, v);
  Py_RETURN_NONE;
}

typedef struct _try_pop_ret {
  PyObject *obj;
  bool was_empty;
} TryPopRet;

static PyObject *try_pop_ret_to_py_pair(TryPopRet retval) {
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
  TreiberNode *old_head = ts->head;
  if (!old_head) {
    // This shouldn't happen, it's an error.
    // single_pop should not be called without successfully procuring the
    // semaphore before and in that case old_head should never be NULL.
    PyErr_SetString(PyExc_RuntimeError,
                    "got a null head in single_pop. Probably an error in the "
                    "implementation of the _treiber C extension.");
    TryPopRet ret = {NULL, true};
    return ret;
  }
  TreiberNode *next_head = old_head->next;
  if (!atomic_compare_exchange_strong(&ts->head, &old_head, next_head)) {
    TryPopRet ret = {NULL, false};
    return ret;
  }
  PyObject *return_object = old_head->value;
  treiber_stack_acquire_refcount_lock(ts);
  node_xdecref(old_head);
  treiber_stack_release_refcount_lock(ts);
  TryPopRet ret = {return_object, false};
  return ret;
}

static TryPopRet treiber_stack_try_pop(TreiberStack *ts) {
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
static TryPopRet treiber_stack_pop(TreiberStack *ts) {
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

static TryPopRet treiber_stack_pop_wait(TreiberStack *ts, bool timeout,
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
  long timeout_ns = 0;
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
    PyErr_SetNone(PyErr_queue_Empty);
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
  PyErr_queue_Empty = PyDict_GetItemString(queue_m_dict, "Empty");
  Py_XDECREF(queue_m);

  Py_INCREF(&TreiberStackType);
  PyModule_AddObject(m, "TreiberStack", (PyObject *)&TreiberStackType);
  return m;
}
