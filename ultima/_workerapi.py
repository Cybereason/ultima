"""
Classes for communicating between the Workforce and the workers.
"""
import os
import sys
import signal
import threading
import traceback
from typing import Iterable, Mapping, Callable, Tuple, TYPE_CHECKING

from .args import Args
from ._registry import RegistryKey
if TYPE_CHECKING:
    from .workforce import Workforce


# TODO: convert these into module functions?
class WorkerAPI:
    """
    A collection of methods for interacting with the workers.

    Attributes
    ----------
    tl : threading.local
        Data local to each thread.
        Specifically, `tl.records` is a dictionary storing the functions accessible to the workers.
    """
    tl = threading.local()

    @classmethod
    def initializer(cls, workforce_id: int, functions: Mapping[RegistryKey, Callable], manager_pid: int) -> None:
        """
        Initialize the worker.

        This is called by each worker once, when it is spun up, before any tasks are performed.

        Parameters
        ----------
        workforce_id : int
            The ID of the Workforce of which this worker is a part.
        functions : mapping
            Maps keys to callable objects. These are the functions accessible to the worker in order to perform tasks.
        manager_pid : int
            The ID of the process that spun the worker up.

        Returns
        -------
        None
        """
        if not hasattr(cls.tl, "records"):
            cls.tl.records = {}
        elif workforce_id in cls.tl.records:
            raise RuntimeError(f"WorkerAPI re-initialized for the same Workforce id {workforce_id}")
        cls.tl.records[workforce_id] = functions
        if manager_pid != os.getpid():
            # we're a spawn, protect ourselves from SIGINT
            signal.signal(signal.SIGINT, signal.SIG_IGN)

    @staticmethod
    def init_args(workforce: "Workforce") -> Tuple[int, Mapping[RegistryKey, Callable], int]:
        """
        Get the required arguments for initialization of the worker.
        This is a convenience function.

        Parameters
        ----------
        workforce : Workforce
            The Workforce initializing the worker.

        Returns
        -------
        tuple
            Arguments for the `initializer` method.
        """
        return workforce.workforce_id, workforce.func_registry.get_deserializer(), os.getpid()

    @classmethod
    def _get_func(cls, workforce_id: int, key: RegistryKey) -> Callable:
        return cls.tl.records[workforce_id][key]

    @classmethod
    def execute_one(cls, workforce_id: int, key: RegistryKey, args: Args):
        """
        Execute a single task.

        Parameters
        ----------
        workforce_id : int
            The ID of the Workforce submitting the task.
        key : str or int
            Key identifying the function for executing the task.
        args : Args
            Arguments for the function.

        Returns
        -------
        Result of executing the function with given `args`.
        """
        assert isinstance(args, Args)
        func = cls._get_func(workforce_id, key)
        return func(*args.args, **args.kwargs)

    @classmethod
    def execute(cls, workforce_id: int, key: RegistryKey, args_iter: Iterable[Args]) -> list:
        """
        Execute several tasks as a batch.
        The tasks must share the same function for execution.

        Parameters
        ----------
        workforce_id : int
            The ID of the Workforce submitting the tasks.
        key : str or int
            Key identifying the function for executing all the tasks.
        args_iter : iterable of Args
            Each item is the arguments for a single invocation of the function.

        Returns
        -------
        list
            Results of all function invocations.
            If an exception was raised by the function, the result is replaced by an `ErrorResult` object.
        """
        func = cls._get_func(workforce_id, key)
        results = []
        for args in args_iter:
            assert isinstance(args, Args)
            try:
                result = func(*args.args, **args.kwargs)
            except BaseException:  # noqa
                result = ErrorResult.from_current_exc()
            results.append(result)
        return results


class ErrorResult:
    """
    Holds an exception and its traceback, allowing safe pickling while retaining the traceback text.
    """
    def __init__(self, exc_info):
        self.exc = exc_info[1]
        self.exc.__traceback__ = exc_info[2]

    def __getstate__(self):
        tb_text = f"\n'''\n{''.join(traceback.format_exception(type(self.exc), self.exc, self.exc.__traceback__))}'''"
        return self.exc, tb_text

    def __setstate__(self, state):
        exc, tb_text = state
        self.exc = exc
        self.exc.__cause__ = _WorkerTraceback(tb_text)

    @classmethod
    def from_current_exc(cls) -> "ErrorResult":
        return cls(sys.exc_info())


class _WorkerTraceback(Exception):
    def __init__(self, tb_text: str):
        self.tb_text = tb_text

    def __str__(self):
        return self.tb_text
