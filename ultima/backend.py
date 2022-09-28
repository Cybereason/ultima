"""
Various execution backends, all using a single interface.
"""
import os
import sys
import math
import signal
import threading
import multiprocessing
import multiprocessing.managers
import concurrent.futures
from functools import partial
from abc import ABC, abstractmethod
from typing import Union, Type, TypeVar, Literal

import dill

from .inline import InlineExecutor


#
# Interface
#

class Backend(ABC):
    """
    Abstract base class for an execution backend, providing a common interface.

    Attributes
    ----------
    executor_shutdown_nowait_allowed
    dict
    Executor
    NAME : str
        The name of the backend. Can be used in `get_backend` to get an instance of the backend.
    """
    def __del__(self):
        self.shutdown()

    @property
    def executor_shutdown_nowait_allowed(self) -> bool:
        """Indicates whether we should call executor.shutdown with wait=False."""
        return True

    @classmethod
    @abstractmethod
    def parse_n_workers(cls, n_workers: Union[int, float, None]) -> int:
        """
        Parse the `n_workers` parameters in the context of the relevant backend
        and return the actual number of workers to use.

        Parameters
        ----------
        n_workers : int, float or None
            Generalized number of workers. Allowed types depend on the backend.

        Returns
        -------
        int
            Actual number of workers to use.
        """
        raise NotImplementedError()

    @staticmethod
    def serialize(item):
        """
        Serialize an item in a way that's appropriate for the backend.

        Parameters
        ----------
        item
            Item to be serialized.

        Returns
        -------
        object
            Serialized `item`.
        """
        return item

    @staticmethod
    def deserialize(item):
        """
        Inverse of the `serialize` method.

        Parameters
        ----------
        item
            An object previously returned by `serialize`.

        Returns
        -------
        object
            Original object.
        """
        return item

    @property
    def dict(self):
        """A mapping class appropriate for use with this backend."""
        return dict

    @property
    @abstractmethod
    def Executor(self):
        """An Executor class appropriate for use with this backend."""
        raise NotImplementedError()

    def shutdown(self):
        """Shut down any resources this backend uses."""
        pass


BackendType = TypeVar('BackendType', bound=Backend)
BackendArgumentType = Union[str, BackendType, Type[BackendType]]


def get_backend(name_or_backend: BackendArgumentType) -> BackendType:
    """
    Get a Backend object by name or, if already providing a backend class or object,
    returns an instance of the backend.

    Parameters
    ----------
    name_or_backend : str, Backend or Backend class
        Identifies the required backend.

    Returns
    -------
    Backend
        If `name_or_backend` is already a Backend instance, returns the same object.
        Otherwise, returns a new instance of the required backend.
    """
    if isinstance(name_or_backend, Backend):
        return name_or_backend
    elif isinstance(name_or_backend, type) and issubclass(name_or_backend, Backend):
        backend_class = name_or_backend
    elif isinstance(name_or_backend, str):
        backend_classes = [i for i in globals().values() if isinstance(i, type) and issubclass(i, Backend)]
        for backend_class in backend_classes:
            if getattr(backend_class, "NAME", None) == name_or_backend:
                break
        else:
            names = [name for cls in backend_classes if (name := getattr(cls, "NAME", None)) is not None]
            raise ValueError(f"invalid backend name '{name_or_backend}', should be one of {names}")
    else:
        raise TypeError(f"invalid backend requested: {name_or_backend}")
    return backend_class()


#
# Backend implementations
#

class InlineBackend(Backend):
    """
    A backend for executing tasks inline, i.e. without any execution pool.
    """
    NAME = "inline"

    @classmethod
    def parse_n_workers(cls, n_workers: Literal[0, None]) -> Literal[0]:
        if n_workers not in (0, None):
            raise ValueError(f"{cls.__name__} does not support {n_workers=}, only zero")
        return 0

    @property
    def Executor(self):
        return InlineExecutor


class ThreadingBackend(Backend):
    """
    A backend for executing tasks using multithreading.
    """
    NAME = "threading"

    @classmethod
    def parse_n_workers(cls, n_workers: int) -> int:
        if n_workers is None:
            raise ValueError(f"{cls.__name__} does not provide a default number of workers")
        if n_workers <= 0 or type(n_workers) is float:
            raise ValueError(f"{cls.__name__} does not support non-positive or floating-point worker numbers")
        return n_workers

    @property
    def Executor(self):
        return concurrent.futures.ThreadPoolExecutor


class MultiprocessingBackend(Backend):
    """
    A backend for executing tasks using multiprocessing.

    Attributes
    ----------
    manager
    CONTEXT : str or None
        Name of the multiprocessing context used by this backend.
    """
    NAME = "multiprocessing"
    CONTEXT = None

    def __init__(self):
        self._manager = None
        self._lock = threading.Lock()

    @property
    def _ctx(self):
        if self.CONTEXT is None:
            return None
        else:
            return multiprocessing.get_context(self.CONTEXT)

    @property
    def manager(self) -> multiprocessing.managers.SyncManager:
        if self._manager is None:
            with self._lock:
                if self._manager is None:
                    manager = multiprocessing.managers.SyncManager(ctx=self._ctx)
                    manager.start(self._mgr_init)
                    self._manager = manager
        return self._manager

    @staticmethod
    def _mgr_init():
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    @property
    def executor_shutdown_nowait_allowed(self):
        # there's a multiprocessing bug that was fixed in py 3.9 when shutting down with wait=False
        # https://github.com/python/cpython/commit/a5cbab552d294d99fde864306632d7e511a75d3c
        return sys.version_info >= (3, 9)

    @classmethod
    def parse_n_workers(cls, n_workers: Union[int, float, None]) -> int:
        """
        Parse the `n_workers` parameters and return the actual number of workers to use.

        Parameters
        ----------
        n_workers : int, float or None
            Generalized number of workers. Different values have different meanings:
            - None:         Use total number of CPUs.
            - Positive int: Actual number of workers.
            - Negative int: Use n_CPUs + `n_workers` + 1, i.e. -1 means n_CPUs, -2 means 1 fewer, etc.
            - float:        Fraction of the number of CPUs. Must be positive.

        Returns
        -------
        int
            Actual number of workers to use.

        Raises
        ------
        ValueError
            If `n_workers` equals zero (either int or float).
        TypeError
            If `n_workers` is of an unexpected type.
        NotImplementedError
            If the number of CPUs is required but cannot be computed.
        """
        if n_workers is None:
            n_workers = -1
        if isinstance(n_workers, int):
            if n_workers < 0:
                cpu_count = os.cpu_count()
                if cpu_count is None:
                    raise NotImplementedError("cannot determine number of CPUs")
                n_workers = max(cpu_count + 1 + n_workers, 1)
            elif n_workers == 0:
                raise ValueError(f"{cls.__name__} does not support {n_workers=}")
        elif isinstance(n_workers, float):
            if n_workers <= 0:
                raise ValueError(f"{cls.__name__} does not support non-positive float {n_workers=}")
            n_workers = math.ceil(multiprocessing.cpu_count() * n_workers)
        else:
            raise TypeError(f"{cls.__name__} does not support {n_workers=}")
        return n_workers

    @staticmethod
    def serialize(item):
        return dill.dumps(item)

    @staticmethod
    def deserialize(item):
        return dill.loads(item)

    @property
    def dict(self):
        return self.manager.dict

    @property
    def Executor(self):
        executor = concurrent.futures.ProcessPoolExecutor
        if (ctx := self._ctx) is not None:
            executor = partial(executor, mp_context=ctx)
        return executor

    def shutdown(self):
        if (manager := self._manager) is not None:
            self._manager = None
            manager.shutdown()


class MultiprocessingSpawnBackend(MultiprocessingBackend):
    NAME = 'multiprocessing-spawn'
    CONTEXT = 'spawn'


class MultiprocessingForkBackend(MultiprocessingBackend):
    NAME = 'multiprocessing-fork'
    CONTEXT = 'fork'


class MultiprocessingForkserverBackend(MultiprocessingBackend):
    NAME = 'multiprocessing-forkserver'
    CONTEXT = 'forkserver'
