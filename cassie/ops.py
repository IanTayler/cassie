"""Basic concurrent operations."""
from enum import Enum
import threading
import typing as T

from cassie import TreiberStack

SPECIAL_MESSAGES = Enum("SPECIAL_MESSAGES", ["POISON_PILL", "DONE_SIGNAL"])
MAX_DEFAULT_THREADS = 8


def concurrent_map(
    f: T.Callable[[T.Any], T.Any],
    inp: T.Sequence[T.Any],
    thread_number: T.Optional[int] = None,
) -> T.Sequence[T.Any]:
    """Map function f to sequence inp concurrently.

    Args:
        f: function to map.
        inp: sequence whose elements will be inputs to f.
        [thread_number]: number of threads used.
    """
    if thread_number is None:
        thread_number = min(len(inp), MAX_DEFAULT_THREADS)
    input_stack = TreiberStack()
    result_stack = TreiberStack()
    result = [None] * len(inp)

    def thread_worker():
        while True:
            popped, _ = input_stack.pop_wait()
            if popped is SPECIAL_MESSAGES.POISON_PILL:
                break
            pos, elem = popped
            result_stack.push((pos, f(elem)))

    threads = []
    for _ in range(thread_number):
        thread = threading.Thread(target=thread_worker)
        thread.start()
        threads.append(thread)
    for pos, elem in enumerate(inp):
        input_stack.push((pos, elem))
    for _ in range(len(inp)):
        (pos, item), _ = result_stack.pop_wait()
        result[pos] = item
    for _ in range(thread_number):
        input_stack.push(SPECIAL_MESSAGES.POISON_PILL)
    return result


def concurrent_apply(
    f: T.Callable[[T.Any], T.Any],
    inp: T.Sequence[T.Any],
    thread_number: T.Optional[int] = None,
):
    """Apply function f to sequence inp concurrently.

    Args:
        f: function to apply.
        inp: sequence whose elements will be inputs to f.
        [thread_number]: number of threads used.
    """
    if thread_number is None:
        thread_number = min(len(inp), MAX_DEFAULT_THREADS)
    input_stack = TreiberStack()
    done_stack = TreiberStack()

    def thread_worker():
        while True:
            popped, _ = input_stack.pop_wait()
            if popped is SPECIAL_MESSAGES.POISON_PILL:
                break
            f(popped)
            done_stack.push(SPECIAL_MESSAGES.DONE_SIGNAL)

    threads = []
    for _ in range(thread_number):
        thread = threading.Thread(target=thread_worker)
        thread.start()
        threads.append(thread)
    for elem in inp:
        input_stack.push(elem)
    for _ in range(len(inp)):
        done_stack.pop_wait()
    for _ in range(thread_number):
        input_stack.push(SPECIAL_MESSAGES.POISON_PILL)
