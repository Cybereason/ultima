"""
Mock implementations of Executor and Future for inline execution,
i.e. without any execution pool or parallelization.
"""
from concurrent.futures import Executor, Future


class InlineExecutor(Executor):
    """
    A mock executor, executing using `InlineFuture` objects.
    """
    def __init__(self, max_workers=None, initializer=None, initargs=()):
        if initializer is not None:
            initializer(*initargs)

    def submit(self, fn, *args, **kwargs):
        return InlineFuture(fn, args, kwargs)


class InlineFuture(Future):
    """
    A mock Future, executing inline upon request of result.
    """
    MOCK_RESULT = object()

    def __init__(self, fn, args, kwargs):
        self._result = None
        self.func = fn
        self.args = args
        self.kwargs = kwargs
        super().__init__()
        self.set_result(self.MOCK_RESULT)

    def result(self, timeout=None):
        if self._result is self.MOCK_RESULT:
            self._result = self.func(*self.args, **self.kwargs)
        return super().result(timeout=timeout)
