import os
import gc
import sys
import math
import time
import random
import signal
import weakref
import itertools
import threading
from numbers import Number
from functools import partial
from contextlib import contextmanager
from concurrent.futures import BrokenExecutor

import psutil
import pytest

from ultima import Workforce, Args, ultimap, MultiprocessingBackend, ThreadingBackend, InlineBackend
from ultima.utils import class_logger


#
# marks
#

skip = pytest.mark.skip
skip_on_windows = pytest.mark.skip_on_windows
mark_flaky = pytest.mark.wip  # fails, but not always; bad test!


def stages(n):
    return pytest.mark.parametrize("stage", list(range(n)))


on_three_backends = pytest.mark.parametrize(
    "backend,n_workers", [("inline", 0), ("threading", 4), ("multiprocessing", 4)]
)

# only 'spawn' is supported on Windows
MP_BACKENDS = ['multiprocessing', 'multiprocessing-spawn']
if sys.platform != 'win32':
    MP_BACKENDS.extend(['multiprocessing-fork', 'multiprocessing-forkserver'])


#
# useful asserts
#

class FamilyTree:
    @staticmethod
    def child_processes(recursive=True):
        current_process = psutil.Process()
        children = current_process.children(recursive=recursive)
        return set(children)

    @staticmethod
    def other_threads():
        threads = (t for t in threading.enumerate() if t is not threading.current_thread())
        # we do not count threads started by the pytest-timeout plugin
        threads = (t for t in threads if "pytest_timeout" not in str(t))
        return set(threads)

    @classmethod
    def offsprings(cls):
        return cls.child_processes() | cls.other_threads()


def assert_no_offsprings():
    assert not FamilyTree.offsprings()


@contextmanager
def assert_no_leftover_offsprings(max_allowed=0):
    before = FamilyTree.offsprings()
    yield
    after = FamilyTree.offsprings()
    born = after - before
    assert len(born) <= max_allowed


def fail_the_test():
    assert False


#
# Parameter grid
#

def param_grid(**params):
    for values in itertools.product(*params.values()):
        yield dict(zip(params.keys(), values))


#
# Helper static functions
#

class Func:
    @staticmethod
    def pow2(n):
        return n ** 2

    @staticmethod
    def pow3(n):
        return n ** 3

    @staticmethod
    def div_k_by(n, k=5):
        return k / n

    @staticmethod
    def sleep_for(n):
        time.sleep(n)
        return n

    @staticmethod
    def random(*args, **kwargs):
        return random.random()


#
# Tests
#

class TestHappyFlow:
    @on_three_backends
    def test_basic(self, backend, n_workers):
        results = ultimap(Func.pow2, [1, 3, 5], backend=backend, n_workers=n_workers)
        assert sum(results) == 35

    @on_three_backends
    def test_no_runaway_offspring_count(self, backend, n_workers):
        results = ultimap(Func.pow2, [1, 3, 5], backend=backend, n_workers=n_workers)
        assert sum(results) == 35
        with assert_no_leftover_offsprings():
            results = ultimap(
                Func.pow2, [1, 3, 5], backend=backend, n_workers=n_workers, shutdown_mode='wait'
            )
            assert sum(results) == 35

    @on_three_backends
    def test_advanced(self, backend, n_workers):
        with Workforce(backend, n_workers) as wf:
            assert sum(wf.map(Func.pow2, ((i,) for i in range(10)), return_key='none')) == 285
            assert sum(wf.map(Func.pow2, (Args(i) for i in range(10)), return_key='none')) == 285
            assert sum(wf.map(Func.div_k_by, (Args(k=2 * i, n=i) for i in range(1, 11)), return_key='none')) == 20
            assert sum(i[0] for i in wf.map(Func.random, (() for i in range(100, 110)), return_key='idx')) == 45
            assert sum(i[0] for i in wf.map(Func.pow2, range(100, 110), return_key='input')) == 1045
            inputs = [Args(x='a', y=123) for _i in range(100, 110)]
            assert set(inputs) == set(i[0] for i in wf.map(Func.random, inputs, return_key='input'))

    @pytest.mark.parametrize("backend", ['threading'] + MP_BACKENDS)
    @pytest.mark.parametrize("n_workers", [1, 3])
    def test_parametrized(self, backend, n_workers):
        with Workforce(backend, n_workers) as workforce:
            kwargs_grid = param_grid(
                ordered=[True, False],
                buffering=[None, 4],
                batch_size=[1, 2],
                errors=['ignore', 'log', 'raise', 'return'],
                timeout=[None, 10],
                return_key=['none', 'idx', 'input'],
            )
            for kwargs in kwargs_grid:
                inputs = range(10)
                results = list(workforce.map(Func.pow2, inputs, **kwargs))
                results = [i[1] if isinstance(i, tuple) else i for i in results]
                assert sum(results) == sum(map(Func.pow2, inputs))

    def test_length(self):
        with Workforce('threading', 2) as wf:
            assert len(wf.map(Func.pow2, range(10))) == 10
            with pytest.raises(TypeError):
                len(wf.map(Func.pow2, (x for x in range(10))))
        assert len(ultimap(Func.pow2, range(10), backend='threading', n_workers=2)) == 10
        with pytest.raises(TypeError):
            len(ultimap(Func.pow2, (x for x in range(10)), backend='threading', n_workers=2))

    @on_three_backends
    def test_bare_submit(self, backend, n_workers):
        wf = Workforce(backend, n_workers)
        future = wf.submit(Func.pow2, 5)
        assert future.result() == 25
        wf.shutdown()
        with pytest.raises(RuntimeError):
            wf.submit(Func.pow2, 5)


class TestMultiParallel:
    def test_various(self):
        workforces = dict()
        try:
            workforces[len(workforces)] = Workforce('threading', n_workers=4)
            workforces[len(workforces)] = Workforce('threading', n_workers=4)
            for mp_backend in MP_BACKENDS:
                workforces[len(workforces)] = Workforce(mp_backend, n_workers=4)
            workforces[len(workforces)] = Workforce('inline', n_workers=0)
            result_iters = [
                iter(workforce.map(
                    Func.pow2 if i % 2 == 0 else Func.pow3, range(10),
                    ordered=False, buffering=3, batch_size=2
                ))
                for i, workforce in enumerate(workforces.values())
            ]
            iters = list(itertools.chain(*itertools.repeat(result_iters, 10)))
            random.shuffle(iters)
            result = sum(map(next, iters))
            assert result == (
                math.ceil(len(workforces) / 2) * sum(i**2 for i in range(10)) +
                len(workforces) // 2 * sum(i**3 for i in range(10))
            )
        finally:
            for workforce in workforces.values():
                workforce.shutdown()

    @on_three_backends
    def test_repeated(self, backend, n_workers, n=2):
        for i in range(n):
            with Workforce(backend, n_workers=n_workers):
                pass
        with Workforce(backend, n_workers=n_workers):
            pass

    @on_three_backends
    def test_interspersed(self, backend, n_workers, n=2):
        wfs = []
        try:
            for i in range(n):
                wfs.append(Workforce(backend, n_workers=n_workers))
            for wf in wfs:
                result_iter = wf.map(Func.pow2, range(10))
                result = sum(result_iter)
                assert result == 285
        finally:
            for wf in wfs:
                wf.shutdown()


class TestErrors:
    logger = class_logger()

    @classmethod
    def error_thrower(cls, n):
        if n == 3:
            raise Exception("third time I scream")
        if n == 5:
            raise KeyboardInterrupt("Ctrl-C from task, how sad")
        if n == 7:
            raise RuntimeError("This is a runtime error")
        if n == 11:
            raise BaseException("Hardcore exception")  # noqa
        if n == 13:
            sys.exit(0)
        if n == 15:
            sys.exit(1)
        return n

    class CustomException(Exception):
        def __init__(self):
            super().__init__(f"Custom exception in pid {os.getpid()}")

    @on_three_backends
    def test_error_modes(self, backend, n_workers):
        with Workforce(backend, n_workers=n_workers) as workforce:
            map_call = partial(
                workforce.map, self.error_thrower, range(17),
                ordered=False, buffering=3, batch_size=1, timeout=None, return_key='idx'
            )
            results = list(map_call(errors='return'))
            for i, j in results:
                assert i is j or isinstance(j, BaseException)
            results = list(map_call(errors='ignore'))
            assert len(results) == 11
            results = list(map_call(errors='log'))
            assert len(results) == 11
            with pytest.raises(BaseException):
                list(map_call(errors='raise'))

    @staticmethod
    def external_killer(n):
        if sys.platform == 'win32':
            if n in (3, 5):
                psutil.Process().kill()
        else:
            if n == 3:
                os.system(f"kill {os.getpid()}")
            if n == 5:
                os.system(f"kill -9 {os.getpid()}")
        return n

    @pytest.mark.parametrize("backend", MP_BACKENDS)
    @pytest.mark.parametrize("death_mode", [3, 5])
    def test_external_death_specific(self, backend, death_mode):
        with Workforce(backend, n_workers=4) as workforce:
            self._executor_fails_on(workforce, self.external_killer, [death_mode] * 5)
            assert not workforce.active, "after a failed workforce mapping, the workforce should be shut down"

    @pytest.mark.parametrize("backend", MP_BACKENDS)
    def test_external_death_mix(self, backend):
        with Workforce(backend, n_workers=4) as workforce:
            inputs = list(range(7))
            random.shuffle(inputs)
            self._executor_fails_on(workforce, self.external_killer, inputs)

    @staticmethod
    def _executor_fails_on(workforce, func, inputs):
        # we expect this to fail, even when `errors` is set to ignore.
        # this is a failure in the workforce level and not the task level.
        with pytest.raises(BrokenExecutor):
            list(workforce.map(
                func, inputs, ordered=False, buffering=3, batch_size=1, errors='ignore',
                timeout=None, return_key='idx'
            ))
            fail_the_test()

    @pytest.mark.parametrize("backend", MP_BACKENDS)
    def test_external_death_ultimap(self, backend):
        inputs = list(range(7))
        random.shuffle(inputs)
        mapping = ultimap(self.external_killer, inputs, buffering=1, backend=backend, n_workers=2)
        assert mapping.bfr.active
        assert mapping.workforce.active
        try:
            list(mapping)
        except BrokenExecutor:
            assert not mapping.bfr.active
            assert not mapping.workforce.active
        else:
            fail_the_test()

    @mark_flaky
    @stages(4)
    @on_three_backends
    @pytest.mark.timeout(30)
    def test_incomplete_usage(self, backend, n_workers, stage):
        def once():
            with Workforce(backend, n_workers, shutdown_mode='wait') as workforce:
                if stage >= 1:
                    mapping = workforce.map(
                        Func.pow2, range(10), ordered=False, buffering=3, batch_size=1, errors='return',
                        timeout=None, return_key='idx',
                    )
                    if stage >= 2:
                        # partial fetching
                        list(itertools.islice(mapping, 7))
                        if stage >= 3:
                            # exhaust remaining items (for sanity)
                            list(mapping)
            assert not workforce.active
        once()
        with assert_no_leftover_offsprings():
            once()

    @on_three_backends
    def test_delete_mapping(self, backend, n_workers):
        with Workforce(backend, n_workers) as wf:
            mapping = wf.map(Func.pow2, range(10), buffering=1)
            mapping_ref = weakref.ref(mapping)
            bfr_ref = weakref.ref(mapping.bfr)
            list(itertools.islice(mapping, 7))
            del mapping
            gc.collect()
            assert mapping_ref() is None
            assert bfr_ref() is None

    # on Windows, this ^C kills the entire pytest run, so we need to skip it until further notice
    @skip_on_windows
    @pytest.mark.parametrize("backend", ['threading', 'multiprocessing'])
    def test_ctrl_c_in_main_process(self, backend):
        # there's one leftover process that doesn't seem to accumulate that we can't get rid of
        allowed_leftovers = 1 if backend == "multiprocessing" else 0
        with assert_no_leftover_offsprings(max_allowed=allowed_leftovers):
            workforce = Workforce(backend, n_workers=4, shutdown_mode='wait')
            ctrl_c_delay = 0.1
            with pytest.raises(KeyboardInterrupt):
                results_iter = workforce.map(Func.sleep_for, [1] * 20, ordered=False, buffering=3)
                results = list()
                # get first result, to make sure the executor pool has started working
                results.append(next(iter(results_iter)))
                t_ctrl_c = []
                threading.Thread(target=self._ctrl_c_after, args=(ctrl_c_delay, t_ctrl_c)).start()
                results.extend(results_iter)
            # normally 0.5 should be enough, but the additional grace makes the test less fragile on slow systems
            allowed_delay = 1.5
            delay = time.monotonic() - t_ctrl_c[0]
            workforce.shutdown()
            assert delay <= allowed_delay

    @staticmethod
    def _ctrl_c_after(sleep_time, t_out):
        time.sleep(sleep_time)
        t_out.append(time.monotonic())
        os.kill(os.getpid(), signal.SIGINT)

    @on_three_backends
    def test_error_in_input(self, backend, n_workers):
        with Workforce(backend, n_workers) as wf:
            kwargs_grid = param_grid(
                ordered=[True, False],
                buffering=[None, ],
                batch_size=[1, 2],
                errors=['ignore', 'log', 'raise', 'return'],
                timeout=[None, 10],
                return_key=['none', 'idx', 'input'],
                #
                explode_after=[0, 1, 10, 100],
            )
            for kwargs in kwargs_grid:
                explode_after = kwargs.pop("explode_after")
                result = []
                with pytest.raises(self.ExplodingInput):
                    result.extend(wf.map(Func.pow2, self._exploding_input_generator(explode_after), **kwargs))
                assert len(result) == explode_after - explode_after % kwargs['batch_size']

    class ExplodingInput(Exception):
        pass

    @classmethod
    def _exploding_input_generator(cls, explode_after=10):
        for i in range(explode_after * 2 + 1):
            if i == explode_after:
                raise cls.ExplodingInput("boom")
            else:
                yield i


class TestInline:
    @pytest.mark.parametrize("ordered", [True, False])
    @pytest.mark.parametrize("buffering", [None, 4])
    @pytest.mark.parametrize("batch_size", [1, 2])
    @pytest.mark.parametrize("errors", ['ignore', 'log', 'raise', 'return'])
    @pytest.mark.parametrize("timeout", [None, 10])
    def test_inline_multi(self, ordered, buffering, batch_size, errors, timeout: Number):
        with Workforce('inline', n_workers=0) as workforce:
            result = sum(workforce.map(
                Func.pow2, range(10), ordered=ordered, buffering=buffering, batch_size=batch_size, errors=errors,
                timeout=timeout
            ))
        assert result == sum(i**2 for i in range(10))

    def test_inline_basic(self):
        return self.test_inline_multi(False, 3, 2, 'raise', 0.5)

    @pytest.mark.parametrize("ordered", [True, False])
    @pytest.mark.parametrize("batch_size", [1, 2])
    def test_inline_buffering(self, ordered, batch_size):
        with Workforce('inline', n_workers=0) as workforce:
            deltas = []
            progress = []
            results = workforce.map(
                progress.append, range(30), ordered=ordered, buffering=5, batch_size=batch_size
            )
            time.sleep(0.5)
            for i, j in enumerate(results):
                deltas.append(len(progress) - i)
                if i % 10 == 0:
                    time.sleep(0.1)
        assert max(deltas) <= batch_size

    def test_inline_n_workers(self):
        for n_workers in [1, 4, -1, 0.1, b':)', "zero"]:
            with pytest.raises(ValueError):
                Workforce('inline', n_workers)
        wf1 = Workforce('inline', 0)
        wf2 = Workforce('inline', None)
        wf3 = Workforce('inline')
        assert wf1.n_workers == wf2.n_workers == wf3.n_workers == 0

    def test_fallback_to_inline(self):
        wf_inline = Workforce(backend='inline')
        for backend in ['threading'] + MP_BACKENDS:
            wf = Workforce(backend=backend, n_workers=0)
            assert isinstance(wf.backend, type(wf_inline.backend))


class TestDoOne:
    logger = class_logger()

    @on_three_backends
    def test_basic(self, backend, n_workers):
        with Workforce(backend, n_workers) as wf:
            assert wf.do_one(Func.pow2, 3) == 3**2
            assert sum(wf.map(Func.pow2, range(10))) == 285
            assert wf.do_one(Func.pow3, 3) == 3**3
            assert sum(wf.map(Func.pow2, range(10))) == 285
            assert wf.do_one(Func.pow2, 3) == 3**2

    @mark_flaky
    @on_three_backends
    def test_advanced(self, backend, n_workers):
        with Workforce(backend, n_workers) as wf:
            assert wf.do_one(Func.pow2, 3) == 3**2
            with pytest.raises(ZeroDivisionError):
                wf.do_one(Func.div_k_by(0), 1)
            two = wf.do_one(partial(Func.div_k_by, k=6), 3)
            assert two == (6 / 3) and type(two) is type(6 / 3)  # noqa
            if backend != 'inline':
                # debug code - remove when test is no longer flaky
                import time
                t0 = time.monotonic()
                try:
                    # a smaller timeout should work, but the added grace makes the test less flaky on slower systems
                    wf.do_one(Func.sleep_for, 0.1, timeout=5)
                finally:
                    self.logger.warning(f"time passed: {time.monotonic() - t0}")
                with pytest.raises(TimeoutError):
                    wf.do_one(Func.sleep_for, 1, timeout=0.1)


class TestBackend:
    def test_backend_specification(self):
        wf1 = Workforce(backend='inline')
        wf2 = Workforce(backend=wf1.backend)
        assert wf1.backend is wf2.backend
        wf3 = Workforce(backend=type(wf1.backend))
        assert wf1.backend is not wf3.backend
        assert type(wf1.backend) is type(wf3.backend)  # noqa

    def test_bad_params(self):
        with pytest.raises(ValueError):
            Workforce(backend="nosuchbackend")
        with pytest.raises(TypeError):
            Workforce(backend=b'whattypeisthis')

    def test_n_workers(self):
        with pytest.raises(ValueError):
            Workforce(backend='threading')
        for n_workers in [-1, None, 1.0]:
            with pytest.raises(ValueError):
                Workforce(backend='threading', n_workers=n_workers)
        for n_workers in [1, 4, 199]:
            assert Workforce(backend='threading', n_workers=n_workers).n_workers == n_workers

        def wf_mp_n_workers(n_workers):
            return Workforce(backend='multiprocessing', n_workers=n_workers).n_workers

        assert Workforce(backend='multiprocessing').n_workers == os.cpu_count()
        assert wf_mp_n_workers(None) == os.cpu_count()
        assert wf_mp_n_workers(1.0) == os.cpu_count()
        assert wf_mp_n_workers(10.0) == 10 * os.cpu_count()
        assert wf_mp_n_workers(10) == 10
        assert wf_mp_n_workers(-1) == os.cpu_count()
        if os.cpu_count() > 1:
            assert wf_mp_n_workers(-2) == os.cpu_count() - 1
        assert wf_mp_n_workers(-1000) == 1
        assert wf_mp_n_workers(-1 - os.cpu_count() + 0) == 1
        assert wf_mp_n_workers(-1 - os.cpu_count() + 1) == 1
        assert wf_mp_n_workers(-1 - os.cpu_count() + 2) == 2
        with pytest.raises(ValueError):
            wf_mp_n_workers(-1.0)
        with pytest.raises(TypeError):
            wf_mp_n_workers("-1.0")

    def test_backend_impl(self):
        # test backends directly and not through the Workforce interface
        for backend in [MultiprocessingBackend, ThreadingBackend]:
            with pytest.raises(ValueError):
                backend.parse_n_workers(0)
            with pytest.raises(ValueError):
                backend.parse_n_workers(0.0)
        with pytest.raises(ValueError):
            ThreadingBackend.parse_n_workers(None)
        with pytest.raises(ValueError):
            InlineBackend.parse_n_workers(1)


class TestSpecificReproductions:
    logger = class_logger()

    @pytest.mark.specifically("issue_2")
    @pytest.mark.timeout(20)
    @pytest.mark.repeat(400)
    def test_stress_reproduce_issue_2(self, caplog):
        import logging
        caplog.set_level(logging.DEBUG)
        self.logger.debug("test_stress_reproduce_issue_2: creating the workforce")
        workforce = Workforce('multiprocessing', n_workers=4, shutdown_mode='wait')
        self.logger.debug("test_stress_reproduce_issue_2: creating the mapping")
        mapping = workforce.map(
            Func.pow2, range(2), ordered=False, buffering=None, batch_size=1, errors='return',
            timeout=None, return_key='idx',
        )
        # partial fetching
        self.logger.debug("test_stress_reproduce_issue_2: kicking off the mapping")
        next(iter(mapping))
        # now shutdown, and test whether this hangs (using the test timeout)
        self.logger.debug("test_stress_reproduce_issue_2: shutting down the workforce")
        workforce.shutdown()
        self.logger.debug("test_stress_reproduce_issue_2: test done")

    @pytest.mark.specifically("issue_3")
    @pytest.mark.timeout(10)
    @pytest.mark.repeat(400)
    def test_stress_reproduce_issue_3(self, caplog, backend='multiprocessing', n_workers=4):
        import logging
        caplog.set_level(logging.DEBUG)
        wf = Workforce(backend, n_workers)
        mapping = wf.map(Func.pow2, range(10), buffering=1)
        bfr_ref = weakref.ref(mapping.bfr)
        list(zip(range(7), mapping))
        mapping_ref = weakref.ref(mapping)
        del mapping
        gc.collect()
        assert mapping_ref() is None
        assert bfr_ref() is None
        wf.shutdown()
