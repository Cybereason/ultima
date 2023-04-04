import os
import time
import queue
import threading
import multiprocessing as mp
from multiprocessing.managers import SyncManager
from typing import Dict, Tuple, Iterable, Callable, ClassVar, Optional

from .args import Args
from .backend import BackendType, MultiprocessingBackend, ThreadingBackend, InlineBackend


def make_recursive(func: Callable, inputs: Iterable, backend: BackendType):
    recursion = Recursion(func, inputs, backend)
    return recursion.func, recursion.inputs


class Recursion:
    _MP_MANAGER: ClassVar[Optional[SyncManager]] = None

    def __init__(self, func: Callable, inputs: Iterable, backend: BackendType,
                 *, interval: float = 0.1):
        self._orig_func = func
        self._orig_inputs = inputs
        self._interval = interval
        self._queue = self._make_queue(backend)
        self._safe_data = _SafeData(backend)

    @classmethod
    def _make_queue(cls, backend: BackendType):
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

    def add_input(self, *args, **kwargs):
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
            return self._orig_func(*args, **kwargs, add_input=self.add_input)
        finally:
            self._mark_task_done()


class _SafeData:
    # warning: the caller is responsible to keep backend alive for the span of this object's lifecycle.
    # when a backend gets GCed, its manager gets shut down and the data invalidates.
    def __init__(self, backend):
        # we avoid a lock by giving each process/thread combination its own counters
        self._multi_queue_size: Dict[Tuple[int, int], int] = backend.dict()
        self._multi_tasks_done: Dict[Tuple[int, int], int] = backend.dict()
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
