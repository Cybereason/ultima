from math import ceil
from functools import partial
from collections.abc import Sized
from concurrent.futures import BrokenExecutor
from typing import Optional, Literal, Callable, Iterable, List, Collection, \
    Union, Tuple, Any, Generic, TypeVar, TYPE_CHECKING, get_args

from .bfr import BufferedFutureResolver, KeyedFuture
from .args import Args
from .utils import class_logger, batches
from ._registry import RegistryKey
from ._workerapi import WorkerAPI, ErrorResult
if TYPE_CHECKING:
    from .workforce import Workforce


T = TypeVar("T")
ReturnKey = Literal['none', 'idx', 'input']
Error = Literal['raise', 'return', 'ignore', 'log']


class Mapping(Generic[T]):
    """
    Represents the results of mapping a single function over some inputs.

    This class is given a Workforce and performs the actions needed to map a function over the inputs.
    This includes registering the function for use by the workers, batching the inputs, submitting the tasks
    and using a BufferedFutureResolver to resolve the submitted Futures.

    The results are accessed by iterating over the Mapping.

    Parameters
    ----------
    workforce : workforce.Workforce
        The Workforce in charge of executing the function.
    func : callable
        The function to map.
    inputs : iterable
        An iterable yielding arguments to `func`. Each item can be:
            - An args.Args object, having args and kwargs members.
            - A tuple, which will be unpacked when invoking `func` and used as positional arguments.
            - Any other object, which will be used as the single argument to `func`.
        If the iterable is Sized (i.e. has a length), then its length is also the length of this Mapping.
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
    """
    logger = class_logger()

    def __init__(self, workforce: "Workforce", func: Callable[..., T], inputs: Iterable, ordered: bool = False,
                 buffering: Optional[int] = None, batch_size: int = 1, errors: Error = 'raise',
                 timeout: Optional[float] = None, return_key: ReturnKey = 'none'):
        assert errors in get_args(Error)
        assert return_key in get_args(ReturnKey)
        self.batch_size = batch_size
        self.errors = errors
        self.return_key = return_key
        func_key = workforce.func_registry.register(func)
        self._n_inputs = len(inputs) if isinstance(inputs, Sized) else None
        if self._n_inputs and workforce.n_workers != 0:
            self.batch_size = min(self.batch_size, ceil(self._n_inputs / workforce.n_workers))
        assert self.batch_size >= 1
        if buffering is not None:
            buffering = ceil(buffering / self.batch_size)
        input_batches = self._batch_inputs(map(Args.canonize, inputs))
        self.bfr = BufferedFutureResolver(
            keyed_futures=self._generate_keyed_futures(workforce, func_key, input_batches),
            abort_on_error=True,
            ordered=ordered,
            buffering=buffering,
            timeout=timeout,
            name=f"Workforce-{workforce.workforce_id}-BFR",
        )
        results = self.bfr
        results = self._unbatch_results(results)
        results = self._handle_errors(results)
        results = self._handle_workforce_failures(results, workforce)
        if self.return_key == 'none':
            results = (i[1] for i in results)
        self._results = results

    def __len__(self) -> int:
        if self._n_inputs is None:
            raise TypeError("input iterable has no length")
        return self._n_inputs

    def __iter__(self) -> Union[Iterable[Union[T, Exception]], Iterable[Tuple[Any, Union[T, Exception]]]]:
        return self._results

    def _batch_inputs(self, inputs: Iterable[Args]) -> Iterable[List[Args]]:
        return map(list, batches(inputs, self.batch_size))

    def _generate_keyed_futures(self, workforce: "Workforce", func_key: RegistryKey,
                                input_batches: Iterable[List[Args]]) -> Iterable[KeyedFuture[list]]:
        if not workforce.active:
            return
        make_keys = partial(self._make_keys, return_key=self.return_key, batch_size=self.batch_size)
        # Don't keep a reference so that the Mapping could be deleted before the generator is fully consumed
        del self
        for batch_idx, input_batch in enumerate(input_batches):
            future = workforce.submit(WorkerAPI.execute, workforce.workforce_id, func_key, input_batch)
            if future is not None:
                yield KeyedFuture(make_keys(batch_idx, input_batch), future)
            if not workforce.active:
                return

    # This must be static so that _generate_keyed_futures doesn't keep a reference to self
    @staticmethod
    def _make_keys(batch_idx: int, input_batch: Collection[Args], return_key: ReturnKey, batch_size: int) -> list:
        if return_key == 'idx':
            return list(range(s := batch_idx * batch_size, s + len(input_batch)))
        elif return_key == 'input':
            return [args.raw for args in input_batch]
        else:
            return [None] * len(input_batch)

    @staticmethod
    def _unbatch_results(results):
        for batch_keys, batch_results in results:
            yield from zip(batch_keys, batch_results)

    def _handle_errors(self, results):
        for key, result in results:
            if isinstance(result, ErrorResult):
                exc = result.exc
                if self.errors == 'return':
                    yield key, exc
                elif self.errors == 'ignore':
                    continue
                elif self.errors == 'log':
                    self.logger.info(f'exception raised from key {key}', exc_info=exc)
                    continue
                else:
                    assert self.errors == 'raise'
                    self.terminate()
                    raise exc
            else:
                yield key, result

    def _handle_workforce_failures(self, results, workforce):
        try:
            yield from results
        except BrokenExecutor:
            self.logger.warning("Executor got broken while reading results, shutting down workforce and re-raising")
            workforce.shutdown()
            raise

    def terminate(self) -> None:
        self.bfr.terminate()


class SingularMapping(Mapping[T]):
    """
    A Mapping that is also in charge of shutting its Workforce down when finished.

    This Mapping is used when it is the only Mapping of a Workforce, which cannot be used as
    a context manager to allow proper shutdown, e.g. by the ultimap sugaring function.
    """
    def __init__(self, workforce: "Workforce", *args, **kwargs):
        super().__init__(workforce, *args, **kwargs)
        self.workforce = workforce

    def __iter__(self) -> Union[Iterable[Union[T, Exception]], Iterable[Tuple[Any, Union[T, Exception]]]]:
        try:
            yield from super().__iter__()
        finally:
            self.terminate()

    def terminate(self) -> None:
        self.workforce.shutdown()
        super().terminate()
