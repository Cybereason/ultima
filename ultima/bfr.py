import time
import threading
import concurrent.futures
from typing import Iterable, Optional, Generic, TypeVar, Tuple, Any

from .utils import class_logger


T = TypeVar("T")


class KeyedFuture(Generic[T]):
    def __init__(self, key: T, future: concurrent.futures.Future):
        self.key = key
        self.future = future


class BufferedFutureResolver(Generic[T]):
    """
    A generator of results from futures, supporting buffering and timeout.

    This class consumes futures in a background thread and provides iteration on the resolved results.
    Consumption of input only starts when starting iteration on the results.
    Errors are handled according to the user's preference. If aborting prematurely, unresolved futures are canceled.

    Parameters
    ----------
    keyed_futures : iterable of KeyedFuture
        The futures to resolve, each paired with a key via the KeyedFuture helper class.
    abort_on_error : bool
        Controls the behavior when an exception is raised when obtaining a result:
        - True:  Abort the iteration and raise the exception.
        - False: Return the exception object instead of the result.
    ordered : bool
        Whether the order of the outputs should correlate to the order of the inputs in `keyed_futures`.
    buffering : int, optional
        Limits the number of futures extracted from `keyed_futures`. When the buffering limit is reached, wait
        until some futures are resolved (finished) before extracting more.
        By default, no limit is set.
    timeout : float, optional
        A global timeout, in seconds, on the entire procedure of resolving all the futures.
        The clock starts as soon as iteration over the results starts.
    name : str, optional
        Name for the feeder thread.
    """
    SYNC_TIMEOUT = 0.2

    logger = class_logger()

    def __init__(self, keyed_futures: Iterable[KeyedFuture[T]], abort_on_error: bool, ordered: bool,
                 buffering: Optional[int] = None, timeout: Optional[float] = None, name: Optional[str] = None):
        self.logger.debug("created")
        self.keyed_futures = iter(keyed_futures)
        self.abort_on_error = abort_on_error
        self.ordered = ordered
        self.capacity = buffering
        self.timeout = timeout
        self.name = name
        self.active = True
        self._start_time = None
        self._feeder_error = None

        self.lock = threading.Lock()
        self.evt_new_future = threading.Event()
        self.evt_new_capacity = threading.Event()
        self.futures = {}

        self.feeder_thread = threading.Thread(
            target=self._feeder_thread_impl,
            daemon=True,
            name=None if self.name is None else f"{self.name}-FeederThread"
        )

    def __iter__(self) -> Iterable[Tuple[T, Any]]:
        self._start_time = time.monotonic()
        self.feeder_thread.start()
        yield from self._futures_iter()

    # dev note: this code runs in a single-use thread
    # WARNING: this is a daemon thread, do not assume resource cleanup or try-finally mechanisms work
    def _feeder_thread_impl(self):
        self.logger.debug("feeder thread running")
        # once a future has been created, we're responsible for it until we've put it into the future set.
        # TODO: Replace "while True" with "while self.active"
        while True:
            try:
                keyed_future = next(self.keyed_futures)
            except StopIteration:
                break
            except concurrent.futures.BrokenExecutor as exc:
                self.logger.warning("feeder thread aborting due to executor failure in future generation")
                self._feeder_error = exc
                return
            except BaseException as exc:
                if isinstance(exc, Exception):
                    err = "error"
                    log_method = self.logger.info
                else:
                    err = "base exception"
                    log_method = self.logger.exception
                log_method(f"{err} thrown from future generator, marking broken and exiting thread")
                self._feeder_error = exc
                return
            try:
                # we'll keep trying to put it into the future set until successful or until we're asked to stop feeding
                while True:
                    if not self.active:
                        # by definition, we always hold an untracked future at this point, so it's on us to cancel it
                        self.logger.debug("feeder thread exiting since BFR is not active (canceling future)")
                        was_canceled = keyed_future.future.cancel()
                        if was_canceled:
                            # work around gh-94440 by sleeping a tiny bit before potentially shutting down the executor
                            # solves issue #3
                            time.sleep(0.05)
                        return
                    if self._has_capacity():
                        with self.lock:
                            self.futures[keyed_future.future] = keyed_future.key
                        self.evt_new_future.set()
                        break
                    if self.evt_new_capacity.wait(timeout=self.SYNC_TIMEOUT):
                        self.evt_new_capacity.clear()
            except BaseException as exc:
                self.logger.exception("internal error in feeder thread, marking broken and raising to die")
                self._feeder_error = exc
                raise
        self.logger.debug("feeder thread done")

    def _has_capacity(self) -> bool:
        return self.capacity is None or len(self.futures) < self.capacity

    # dev note: this core generator happens in the main thread
    def _futures_iter(self) -> Iterable[Tuple[T, Any]]:
        try:
            while (n_futures := len(self.futures)) or self.feeder_thread.is_alive() and self._feeder_error is None:
                self._check_timeout()
                if not n_futures:
                    if not self.evt_new_future.wait(timeout=self.SYNC_TIMEOUT):
                        continue
                    self.evt_new_future.clear()
                    if not self.futures:
                        continue
                self._check_timeout()
                with self.lock:
                    if self.ordered:
                        futures = [next(iter(self.futures))]
                    else:
                        futures = list(self.futures)
                time_remaining = self._check_timeout()
                done, not_done = concurrent.futures.wait(futures, time_remaining, concurrent.futures.FIRST_COMPLETED)
                for done_future in done:
                    self._check_timeout()
                    with self.lock:
                        key = self.futures.pop(done_future)
                    self.evt_new_capacity.set()
                    self._check_timeout()
                    try:
                        yield key, done_future.result()
                    except concurrent.futures.TimeoutError as exc:
                        # replace with a standard TimeoutError
                        raise TimeoutError("future timed out") from exc
                    except Exception as exc:
                        if self.abort_on_error:
                            raise
                        yield key, exc
        except GeneratorExit:
            # we were asked nicely to terminate, no need to report an error
            raise
        except:
            self.logger.exception("error raised in _futures_iter")
            raise
        finally:
            self.terminate()
        # noinspection PyUnreachableCode
        if self._feeder_error is not None:
            raise self._feeder_error

    def _check_timeout(self) -> Optional[float]:
        if self.timeout is None:
            return None
        elapsed = time.monotonic() - self._start_time
        remaining = self.timeout - elapsed
        if remaining <= 0:
            msg = f"Timed out after {elapsed:g} seconds (timeout={self.timeout})"
            self.logger.warning(msg)
            raise TimeoutError(msg)
        return remaining

    def terminate(self) -> None:
        if not self.active:
            return
        self.logger.debug("cleaning up futures")
        self.active = False
        # we could be terminated due to an error in the feeder thread, in which case no need to join
        if self.feeder_thread.is_alive() and self.feeder_thread is not threading.current_thread():
            self.feeder_thread.join()
        if n := len(self.futures):
            self.logger.debug(f"canceling {n} pending futures")
        any_future_canceled = False
        for future in self.futures.keys():
            any_future_canceled |= future.cancel()
        if any_future_canceled:
            # work around gh-94440 by sleeping a tiny bit before proceeding to (probably) shut down the executor
            # solves issue #2
            time.sleep(0.05)
        self.logger.debug("cleaned up futures")
