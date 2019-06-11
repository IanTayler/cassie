"""Python code implementing the interface for TreiberStack."""
import queue

import _treiber


class TreiberStack(_treiber.TreiberStack):  # pylint: disable=c-extension-no-member
    """Treiber stack backed by a C implementation.

    Works as a drop-in replacement of queue.Queue, except for the max_size init
    argument, join and task_done."""

    def put(self, item, block=True, timeout=None):  # pylint: disable=unused-argument
        """Put an item to the stack.

        It has "block" and "timeout" arguments for compatibility with
        queue.Queue and multiprocessing.Queue, but it will never block.
        """
        self.push(item)

    def put_nowait(self, item):
        """In this implementation, equivalent to put(item)."""
        self.push(item)

    def get(self, block=True, timeout=None):
        """Get an item from the top of the stack.

        block and timeout arguments work just like queue.Queue.get block and
        timeout arguments."""
        if block:
            should_timeout = timeout is not None and timeout >= 0.0
            timeout = timeout or 0.0
            popped_val, was_empty = self.pop_wait(should_timeout, timeout)
        else:
            popped_val, was_empty = self.pop()
        if was_empty:
            raise queue.Empty
        return popped_val

    def get_nowait(self, item):
        """Try to get an item from the top of the stack."""
        return self.get(item, False)
