import os
import time
import queue
import threading
import multiprocessing as mp
from multiprocessing.managers import SyncManager
from typing import Iterable, Callable, Protocol, overload, ClassVar

from ultima.backend import BackendArgument as UltimaBackendArgument, BackendType as UltimaBackendType, get_backend
from ultima import Args, MultiprocessingBackend, ThreadingBackend, InlineBackend

__all__ = [
    'task_recursion_map', 'task_recursion_ultimap', 'AddTaskProtocol'
]


def task_recursion_ultimap(func, inputs, backend=None):
    """
    Enable recursion for an ultimap() call.
    The callback `func` will receive an extra parameter through which additional tasks can be added.

    Parameters
    ----------
    func: Callable
        The function to map over all `inputs`, with an additional `task_adder: AddTaskProtocol` keyword argument.
    inputs: iterable
        The `inputs` to use in the ultima mapping.
    backend : str, Backend or Backend class, default 'multiprocessing'
        The `backend` to use in the ultima mapping.

    To add additional inputs to the mapping while handling an existing input, call `task_adder.add_task(...)`.
    See `AddTaskProtocol` for more details.

    Examples
    --------
    Run ultimap on a recursive function
        >>> import random
        >>> from ultima import ultimap
        >>>
        >>> def func(number, *, task_adder: AddTaskProtocol):
        ...     if number % 2 == 0:
        ...         task_adder.add_task(random.randint(1, 100))
        ...     return number
        ...
        >>> results = list(ultimap(**task_recursion_ultimap(func, range(10))))
        >>> assert len(results) > 10

    Returns
    -------
    dictionary with adjusted [func, inputs, backend] parameters, to be used as **kwargs in an `ultima.ultimap` call.
    """
    task_recursion = TaskRecursion(func, inputs, backend)
    return {
        'func': task_recursion.func,
        'inputs': task_recursion.inputs,
        'backend': task_recursion.backend,
    }


def task_recursion_map(func, inputs, workforce):
    """
    Enable recursion for an ultima.Workforce.map() call.
    The callback `func` will receive an extra parameter through which additional tasks can be added.

    Parameters
    ----------
    func: Callable
        The function to map over all `inputs`, with an additional `task_adder: AddTaskProtocol` keyword argument.
    inputs: iterable
        The `inputs` to use in the ultima mapping.
    workforce : ultima.Workforce
        The `Workforce` instance used for mapping.

    To add additional inputs to the mapping while handling an existing input, call `task_adder.add_task(...)`.
    See `AddTaskProtocol` for more details.

    Examples
    --------
    Run ultima.Workforce.map on a recursive function
        >>> import random
        >>> from ultima import Workforce
        >>>
        >>> def func(number, *, task_adder: AddTaskProtocol):
        ...     if number % 2 == 0:
        ...         task_adder.add_task(random.randint(1, 100))
        ...     return number
        ...
        >>> with Workforce() as wf:
        ...     results = list(wf.map(**task_recursion_map(func, range(10), wf)))
        >>> assert len(results) > 10

    Returns
    -------
    dictionary with adjusted [func, inputs] parameters, to be used as **kwargs in an `ultima.Workforce.map` call.
    """
    task_recursion = TaskRecursion(func, inputs, workforce.backend)
    return {
        'func': task_recursion.func,
        'inputs': task_recursion.inputs,
    }


class AddTaskProtocol(Protocol):
    @overload
    def add_task(self, args: Args) -> None: ...
    @overload
    def add_task(self, args: tuple) -> None: ...
    @overload
    def add_task(self, *args, **kwargs) -> None: ...

    def add_task(self, *args, **kwargs) -> None:
        ...


class TaskRecursion:
    _MP_MANAGER: ClassVar[SyncManager | None] = None

    def __init__(self, func: Callable, inputs: Iterable, backend: UltimaBackendArgument | None = None,
                 *, interval: float = 0.1):
        if backend is None:
            backend = 'multiprocessing'
        self._orig_func = func
        self._orig_inputs = inputs
        self.backend = get_backend(backend)
        self._interval = interval
        self._queue = self._make_queue(self.backend)
        self._safe_data = _SafeData(self.backend)

    # this class can get pickled when sent to ultima workers.
    # if that happens we can't give them the non-pickle-able backend; and they don't need it anyway.
    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop('backend')
        return d

    @classmethod
    def _make_queue(cls, backend: UltimaBackendType):
        if isinstance(backend, MultiprocessingBackend):
            if cls._MP_MANAGER is None:
                cls._MP_MANAGER = mp.Manager()
            return cls._MP_MANAGER.Queue()
        elif isinstance(backend, (ThreadingBackend, InlineBackend)):
            return queue.Queue()
        else:
            raise ValueError(f"backend {backend} is not supported")

    # interface for func callback to add tasks, and for its wrapper to mark tasks as done
    # these two should be called within the worker

    def add_task(self, *args, **kwargs):
        # we keep the semantics of ultima's input specification
        if kwargs or len(args) != 1:
            task_args = Args(*args, **kwargs)
        else:
            # this supports all canonize-able forms of input: a single argument, a tuple to expand, and an Args instance
            task_args = args[0]
        self._queue.put(task_args)
        self._safe_data.queue_size_inc()

    def _mark_task_done(self):
        self._safe_data.tasks_done_inc()

    # adapters for ultima's interface of inputs, func and backend (stored as member variable)

    @property
    def inputs(self):
        self._safe_data.set_owner()
        source_iterator = iter(self._orig_inputs)
        source_depleted = False
        while True:
            if self._safe_data.outstanding_tasks == 0 and self._safe_data.queue_size == 0 and source_depleted:
                return
            if self._safe_data.queue_size > 0:
                self._safe_data.queue_size_dec()
                self._safe_data.tasks_scheduled_inc()
                yield self._queue.get()
                continue
            if not source_depleted:
                try:
                    next_input = next(source_iterator)
                except StopIteration:
                    source_depleted = True
                else:
                    self._queue.put(next_input)
                    self._safe_data.queue_size_inc()
                continue
            # we have no inputs to yield, but our outstanding tasks might still add new tasks, so we sleep and retry
            time.sleep(self._interval)
            continue

    def func(self, *args, **kwargs):
        try:
            return self._orig_func(*args, **kwargs, task_adder=self)
        finally:
            self._mark_task_done()


class _SafeData:
    # warning: the caller is responsible to keep backend alive for the span of this object's lifecycle.
    # when a backend gets GCed, its manager gets shut down and the data invalidates.
    def __init__(self, backend):
        self._data = backend.dict()
        # we avoid a lock by giving each process/thread combination its own counters
        self._multi_queue_size: dict[(int, int), int] = backend.dict()
        self._multi_tasks_done: dict[(int, int), int] = backend.dict()
        self._tasks_scheduled = 0
        self._owner_key = None

    def queue_size_inc(self):
        key = self._caller_key()
        self._multi_queue_size[key] = self._multi_queue_size.get(key, 0) + 1

    def queue_size_dec(self):
        self._check_owner()
        self._multi_queue_size[self._owner_key] = self._multi_queue_size.get(self._owner_key, 0) - 1

    def tasks_scheduled_inc(self):
        self._check_owner()
        self._tasks_scheduled += 1

    def tasks_done_inc(self):
        key = self._caller_key()
        self._multi_tasks_done[key] = self._multi_tasks_done.get(key, 0) + 1

    @property
    def queue_size(self):
        self._check_owner()
        return sum(self._multi_queue_size.values())

    @property
    def outstanding_tasks(self):
        return self._tasks_scheduled - sum(self._multi_tasks_done.values())

    def set_owner(self):
        if self._owner_key is not None:
            raise Exception("owner has already been set")
        self._owner_key = self._caller_key()

    def _check_owner(self):
        if self._owner_key is None:
            raise Exception("the owner has not been set")
        if not self._caller_key() == self._owner_key:
            raise Exception("only the owner of this object can call this method")

    @staticmethod
    def _caller_key():
        return os.getpid(), threading.get_native_id()
