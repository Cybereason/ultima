import time
import random

import pytest

from ultima import ultimap, Workforce, Args
from ultima._recursive import AddTaskProtocol


on_three_backends = pytest.mark.parametrize(
    ['backend', 'n_workers', 'n'], [
        ['inline', 0, 10],
        ['threading', 16, 100],
        ['multiprocessing', 16, 400]
    ]
)


class TestTaskRecursion:
    @pytest.mark.timeout(30)
    @on_three_backends
    def test_ultimap(self, backend, n_workers, n):
        results = list(ultimap(
            self.func, range(n), backend=backend, n_workers=n_workers, recursive=True
        ))
        assert len(results) > n
        assert sum(1 for i in results if i % 2 == 1) == n

    @pytest.mark.timeout(30)
    @on_three_backends
    def test_map(self, backend, n_workers, n):
        n1 = n // 2
        with Workforce(backend=backend, n_workers=n_workers) as wf:
            results1 = list(wf.map(
                self.func, range(n1), recursive=True, ordered=False
            ))
            results2 = list(wf.map(
                self.func, range(n1, n), recursive=True, ordered=True
            ))
        results = results1 + results2
        assert len(results) > n
        assert sum(1 for i in results if i % 2 == 1) == n
        assert [r for r in results2 if r in range(n1, n)] == list(range(n1, n))

    @pytest.mark.timeout(10)
    def test_with_args_class(self):
        n = 10
        results = list(ultimap(
            self.func2, range(n), backend='threading', n_workers=4, recursive=True
        ))
        assert len(results) > n
        assert sum(1 for i in results if i % 2 == 1) == n

    @pytest.mark.timeout(10)
    def test_with_multiple_args(self):
        n = 10
        results = list(ultimap(
            self.func3, range(n), backend='threading', n_workers=4, recursive=True
        ))
        assert len(results) > n
        assert sum(1 for i in results if i % 2 == 1) == n

    @staticmethod
    def func(number, *, task_adder: AddTaskProtocol):
        time.sleep(0.02)
        if number % 2 == 0:
            task_adder.add_task(random.randint(1001, 1100))
        return number

    @staticmethod
    def func2(number, mul=1, *, task_adder: AddTaskProtocol):
        time.sleep(0.02)
        number *= mul
        if number % 2 == 0:
            task_adder.add_task(Args(random.randint(1001, 1100), mul=3))
        return number

    @staticmethod
    def func3(number, mul=1, *, task_adder: AddTaskProtocol):
        time.sleep(0.02)
        number *= mul
        if number % 2 == 0:
            task_adder.add_task(random.randint(1001, 1100), 3)
        return number

    def test_errors(self):
        # note: this often hangs with the inline backend, due to inputs generator waiting for more tasks to finish
        # with pytest.raises(UserWarning):
        #     list(ultimap(
        #         self.func_no7, range(10), backend='inline', recursive=True
        #     ))
        with pytest.raises(UserWarning):
            list(ultimap(
                self.func_no7, range(10), backend='threading',
                n_workers=2, recursive=True
            ))
        with pytest.raises(UserWarning):
            list(ultimap(
                self.func_no7, range(10), backend='multiprocessing',
                n_workers=2, recursive=True
            ))

    @staticmethod
    def func_no7(number, *, task_adder: AddTaskProtocol):
        if number % 7 == 0:
            raise UserWarning(f"no {number} for me")
        return number
