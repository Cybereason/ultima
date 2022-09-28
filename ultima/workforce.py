import sys
import weakref
import threading
import concurrent.futures
from typing import Union, Literal, Callable, Iterable, Optional, TypeVar, get_args

from .args import Args
from .utils import SyncCounter, class_logger
from .backend import get_backend, InlineBackend, BackendArgumentType
from ._workerapi import WorkerAPI
from ._registry import SerializedItemsRegistry
from ._mapping import Mapping, SingularMapping, Error, ReturnKey


T = TypeVar("T")
ShutdownMode = Literal['auto', 'wait', 'nowait']


class Workforce:
    """
    A pool of workers capable of performing tasks.

    A Workforce is a collection of workers, running using a specific backend. They are spun up and initialized when
    the Workforce is initialized, and are then waiting for tasks to perform.
    Wraps a concurrent.futures.Executor object.

    Parameters
    ----------
    backend : str, Backend or Backend class, default 'multiprocessing'
        The backend to use for the workers. Can be either the name of a backend as a string, an instance of
        a Backend subclass or the Backend subclass itself. See backend.get_backend for details.
        Supported backends include 'multiprocessing', 'threading', 'inline', 'multiprocessing-spawn',
        'multiprocessing-fork' and 'multiprocessing-forkserver'.
    n_workers : int or float, optional
        The number of worker in the Workforce. Default and meaning of different argument types depend on `backend`:
        * multiprocessing:
          - Positive int:   Number of processes.
          - Negative int:   Refers to the number of CPUs (-1 means n_CPUs, -2 means 1 fewer, etc.)
          - Positive float: Fraction of the number of CPUs.
          - Default:        Total number of CPUs.
        * threading:
          - Positive int: Number of threads.
          - No default is provided; `n_workers` must be specified.
        * inline:
          - If provided, must be 0.
        If `n_workers` is zero, then `backend` is replaced with 'inline'.
    shutdown_mode : {'auto', 'wait', 'nowait'}, default 'auto'
        Upon shutdown, whether to wait for all running tasks to finish running. 'auto' means 'nowait' if possible
        (determined by the backend and version), otherwise 'wait'.

    Attributes
    ----------
    active : bool
        Whether the Workforce is alive and ready.
    executor : concurrent.futures.Executor
        Underlying Executor managing the workers.
    workforce_id : int
        Unique ID for this Workforce.
    func_registry : SerializedItemsRegistry
        A centralized location for functions to be registered by the Workforce and accessed by the workers.
    """
    logger = class_logger()
    workforce_id_counter = SyncCounter()

    def __init__(self, backend: BackendArgumentType = "multiprocessing", n_workers: Union[int, float, None] = None,
                 shutdown_mode: ShutdownMode = 'auto'):
        assert shutdown_mode in get_args(ShutdownMode)
        self.active = False
        self.executor = None
        self.workforce_id = self.workforce_id_counter.get_next_value()
        self._executor_lock = threading.Lock()
        self.backend = get_backend(backend)
        if n_workers == 0 and not isinstance(self.backend, InlineBackend):
            self.logger.warning(f"replacing backend {self.backend} with InlineBackend()")
            self.backend = InlineBackend()
        self.n_workers = self.backend.parse_n_workers(n_workers)
        self.shutdown_mode = shutdown_mode
        self.func_registry = SerializedItemsRegistry(self.backend)
        self._mappings = []
        self.executor = self.backend.Executor(
            max_workers=self.n_workers,
            initializer=WorkerAPI.initializer,
            initargs=WorkerAPI.init_args(self),
        )
        self.active = True

    def map(self, func: Callable[..., T], inputs: Iterable, *, ordered: bool = False, buffering: Optional[int] = None,
            batch_size: int = 1, errors: Error = 'raise', timeout: Optional[float] = None,
            return_key: ReturnKey = 'none') -> Mapping[T]:
        """
        Map a function over several inputs, performing the tasks by the workers.

        The inputs can be buffered (i.e., not consumed all at once) and gathered into batches. There are several
        options for handling errors and a timeout for the entire calculation can be set.

        The work begins (and the timeout clock starts) only when iteration over the results begins. Results
        become available as they are ready.

        Parameters
        ----------
        func : callable
            The function to map over all `inputs`.
        inputs : iterable
            An iterable yielding arguments to `func`. Each item can be:
                - An args.Args object, having args and kwargs members.
                - A tuple, which will be unpacked when invoking `func` and used as positional arguments.
                - Any other object, which will be used as the single argument to `func`.
            If the iterable is Sized (i.e. has a length), then the returned Mapping is also Sized and its length
            is the same as that of `inputs`.
        ordered : bool, default False
            Whether the order of the outputs should correlate to the order of the `inputs`.
        buffering : int, optional
            Limits the number of `inputs` extracted before waiting for tasks to be completed.
            By default, no limit is set.
        batch_size : int, default 1
            Divide the `inputs` into batches that are sent to the workers instead of sending each input separately.
            This can boost running time significantly, especially when `func` is quick and there are many `inputs`.
        errors : {'raise', 'return', 'ignore', 'log'}, default 'raise'
            How to handle exceptions raised by `func`.
            - 'raise':  Raise the exception.
            - 'return': Return the exception object as if it were one of the results.
            - 'ignore': Ignore the exception completely. If an exception is encountered, the number of results
                        will be smaller than the number of `inputs`.
            - 'log':    Similar to 'ignore', but also log the exception.
        timeout : float, optional
            A global timeout, in seconds, on the entire calculation.
            The clock starts as soon as iteration over the results begins.
        return_key : {'none', 'idx', 'input'}, default 'none'
            Return with each result a key identifying it as a tuple (key, result). May be useful if `ordered` is False.
            - 'none':  Do not return a key. Each result is yielded as is.
            - 'idx':   The key is the zero-based running index of the input.
            - 'input': The key is the input itself in its entirety.

        Returns
        -------
        Mapping
            Iterable object which yields the results of the operation upon iteration.
            If `input` is Sized, it is also Sized.
        """
        self._check_active()
        mapping = Mapping(self, func, inputs, ordered, buffering, batch_size, errors, timeout, return_key)
        self._mappings.append(weakref.ref(mapping))
        return mapping

    def do_one(self, func: Callable[..., T], args, *, timeout: Optional[float] = None) -> T:
        """
        Perform a single task by a worker and return the result.

        Parameters
        ----------
        func : callable
            The function to be invoked by the worker.
        args : args.Args, tuple or any other object
            Arguments for `func`. Can be either of the following:
            - An args.Args object, having args and kwargs members.
            - A tuple, which will be unpacked when invoking `func` and used as positional arguments.
            - Any other object, which will be used as the single argument to `func`.
        timeout : float, optional
            The number of seconds to wait for the result. By default, there is no limit on the wait time.

        Returns
        -------
        The result of invoking `func` on the `args`.

        Raises
        ------
        TimeoutError
            If `timeout` was provided and the calculation timed out.
        RuntimeError
            If the Workforce has been shut down while submitting the task.
        """
        self._check_active()
        func_key = self.func_registry.register(func)
        future = self.submit(WorkerAPI.execute_one, self.workforce_id, func_key, Args.canonize(args))
        if future is None:
            raise RuntimeError("Workforce has been shut down during do_one execution")
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError as exc:
            try:
                future.cancel()
            except:
                self.logger.exception("do_one future timed out but its cancellation raised an exception (suppressing)")
            raise TimeoutError("future timed out") from exc

    def __enter__(self) -> "Workforce":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.debug("Workforce exit handler called")
        self.shutdown()

    def __del__(self):
        self.shutdown()

    def shutdown(self, wait: Optional[bool] = None) -> None:
        """
        Clean-up the resources associated with the Workforce.

        It is safe to call this method several times. However, no other methods can be called after this one.

        Parameters
        ----------
        wait : bool, optional
            If True then shutdown will not return until all running futures have finished executing.
            Default falls back to the `shutdown_mode` parameter of the Workforce.

        Returns
        -------
        None
        """
        self.active = False
        if self.executor is not None:
            with self._executor_lock:
                if self.executor is not None:
                    for mapping_ref in self._mappings:
                        if (mapping := mapping_ref()) is not None:
                            mapping.terminate()
                    wait = (
                        wait if wait is not None else
                        True if self.shutdown_mode == 'wait' else
                        False if self.shutdown_mode == 'nowait' else
                        not self.backend.executor_shutdown_nowait_allowed
                    )
                    try:
                        kwargs = {}
                        if sys.version_info >= (3, 9):
                            kwargs['cancel_futures'] = True
                        self.executor.shutdown(wait=wait, **kwargs)
                    finally:
                        self.executor = None
                    try:
                        self.backend.shutdown()
                    finally:
                        self.backend = None

    def submit(self, func: Callable, *args, **kwargs) -> concurrent.futures.Future:
        """
        Submit a single task to be performed by a worker.

        This is a thin wrap around concurrent.futures.Executor.submit, providing a low-level interface.
        To submit a single task and simply get its result back, use `do_one`.
        To submit several tasks that all run the same function, use `map`.

        Parameters
        ----------
        func : callable
            The function to be invoked by the worker.
        *args
            Positional arguments for `func`.
        **kwargs
            Keyword arguments for `func`.

        Returns
        -------
        concurrent.futures.Future
            Object representing the result of the calculation.
        """
        try:
            with self._executor_lock:
                if self.active:
                    return self.executor.submit(func, *args, **kwargs)
                else:
                    raise RuntimeError("Submit called on an inactive workforce")
        except concurrent.futures.BrokenExecutor:
            self.logger.warning("Executor got broken during submission, shutting down and re-raising")
            self.shutdown()
            raise

    def _check_active(self) -> None:
        if not self.active:
            raise RuntimeError("This workforce instance has already been shut down")


def ultimap(func: Callable[..., T], inputs: Iterable, *, ordered: bool = False, buffering: Optional[int] = None,
            batch_size: int = 1, errors: Error = 'raise', timeout: Optional[float] = None,
            return_key: ReturnKey = 'none', backend: BackendArgumentType = "multiprocessing",
            n_workers: Union[int, float, None] = None, shutdown_mode: ShutdownMode = 'auto') -> SingularMapping[T]:
    """
    A one-liner shortcut for creating a single-use Workforce and using it to map a function over several inputs.

    Thus, the call:
        results = list(ultimap(func, inputs))

    is roughly equivalent to:
        with Workforce() as wf:
            results = list(wf.map(func, inputs))

    with the notable difference that the iterable returned by `ultimap` is also Sized, if the input is Sized.

    Refer to `Workforce` and `Workforce.map` for full documentation.

    See Also
    --------
    Workforce
    Workforce.map
    """
    # TODO: can we also optimize n_workers here according to n_inputs?
    wf = Workforce(backend, n_workers, shutdown_mode)
    return SingularMapping(wf, func, inputs, ordered, buffering, batch_size, errors, timeout, return_key)
