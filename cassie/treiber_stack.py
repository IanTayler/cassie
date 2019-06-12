"""Python code implementing the interface for TreiberStack."""
import _treiber


class TreiberStack(_treiber.TreiberStack):  # pylint: disable=c-extension-no-member
    """Treiber stack backed by a C implementation.

    Works as a drop-in replacement of queue.SimpleQueue, which is the same
    as saying it's a queue.Queue except for the max_size init argument,
    join and task_done."""

    def put_nowait(self, item):
        """In this implementation, equivalent to put(item)."""
        self.push(item)

    def get_nowait(self, item):
        """Try to get an item from the top of the stack."""
        return self.get(item, False)
