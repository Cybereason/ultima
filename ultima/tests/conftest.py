import sys
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-slow", action="store_true", default=False, help="run slow tests"
    )
    parser.addoption(
        "--specifically", action="append", help="when used, only tests marked with this specific-run marker will run"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")
    config.addinivalue_line("markers", "wip: mark test as a work in progress")
    config.addinivalue_line("markers", "skip_on_windows: mark test to be skipped on Windows")
    config.addinivalue_line("markers", "specifically: run some tests only when specifically requested")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--run-slow"):
        skip_slow = pytest.mark.skip(reason="need --run-slow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)
    if sys.platform == 'win32':
        skip_windows = pytest.mark.skip(reason="skipped on Windows")
        for item in items:
            if "skip_on_windows" in item.keywords:
                item.add_marker(skip_windows)
    default_timeout = pytest.mark.timeout(60)
    for item in items:
        if "timeout" not in item.keywords:
            item.add_marker(default_timeout)
    specifically = set(config.getoption("--specifically") or [])
    skip_not_specifically = pytest.mark.skip(reason="test not specifically requested")
    for item in items:
        specific_names = set(name for marker in item.iter_markers("specifically") for name in marker.args)
        if (specifically | specific_names) and not (specifically & specific_names):
            item.add_marker(skip_not_specifically)
