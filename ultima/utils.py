"""
Helpers and utilities.
"""
import logging
import itertools
import threading
from typing import Iterable, TypeVar


T = TypeVar('T')


def batches(iterable: Iterable[T], size: int) -> Iterable[Iterable[T]]:
    """
    Iterate over the given iterable in batches of fixed size.
    The last batch may be smaller.
    """
    it = iter(iterable)
    while True:
        batchiter = itertools.islice(it, size)
        try:
            next_item = next(batchiter)
        except StopIteration:
            return
        yield itertools.chain([next_item], batchiter)


class class_logger():  # noqa
    def __get__(self, obj, cls):
        return logging.getLogger(cls.__module__).getChild(cls.__name__)


class SyncCounter:
    """
    Thread-safe counter.

    A class holding a numeric value and capable of performing atomic post-increment on it.

    Parameters
    ----------
    next_value : int, default 0
        The first value to be returned from `get_next_value`.
    """
    def __init__(self, next_value: int = 0):
        self.next_value = next_value
        self._lock = threading.Lock()

    def get_next_value(self) -> int:
        """
        Get the next value of the counter.

        Safely increment the value by 1 and return the value before incrementation.

        Returns
        -------
        int
            The value of the counter.
        """
        with self._lock:
            result = self.next_value
            self.next_value += 1
        return result
