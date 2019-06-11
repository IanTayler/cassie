from queue import Queue, SimpleQueue
import threading
import timeit

from cassie import TreiberStack


def bench(queuelike, thread_n, reps):
    threads = []

    def producer():
        for i in range(reps):
            queuelike.put(i)

    def consumer():
        for _ in range(reps):
            queuelike.get()

    for _ in range(thread_n):
        threads.append(threading.Thread(target=producer))
        threads.append(threading.Thread(target=consumer))
    for th in threads:
        th.start()
    for th in threads:
        th.join()


num_reps = 500
num_threads = 10
elems_per_thread = 500


def measure_queue_us(q):
    return int(
        timeit.timeit(lambda: bench(q, num_threads, elems_per_thread), number=num_reps)
        * 1e6
        / num_reps
    )


def run_benchmark(q):
    print(
        "{}: {}us per repetition for {} threads, {} elems per thread. Repeated {} times.".format(
            type(q).__name__,
            measure_queue_us(q),
            num_threads,
            elems_per_thread,
            num_reps,
        )
    )


for q in [TreiberStack(), SimpleQueue(), Queue()]:
    run_benchmark(q)
