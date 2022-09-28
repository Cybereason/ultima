# Ultima - intuitive parallel `map()` for Python
[![Ultima CI](https://github.com/Cybereason/ultima/actions/workflows/ci.yml/badge.svg)](https://github.com/Cybereason/ultima/actions/workflows/ci.yml)

## What is it?

**ultima** is a Python package that provides a simple yet powerful interface to do `map()` in parallel.
It uses `concurrent.futures` as its execution backend and can run tasks in either threads or sub-processes.
It is designed to squeeze maximum performance (let those CPUs burn 🔥) with the same simple interface.

## Usage examples:

Run a heavy function in sub-processes:

```python
from ultima import Workforce

inputs = open("input_data.txt")

with Workforce() as wf:
    for result in wf.map(cpu_intensive_function, inputs):
        ...
```

An equivalent one-liner:

```python
from ultima import ultimap

for result in ultimap(cpu_intensive_function, inputs):
    ...
```

The default backend is multiprocessing, but you can easily use threads instead:

```python
from ultima import ultimap

for result in ultimap(io_bound_function, inputs, backend='threading', n_workers=64):
    ...
```

To chain an IO-intensive task with a CPU-intensive task:

```python
from ultima import ultimap

def io_intensive(url):
    import requests
    return requests.get(url).text

def cpu_intensive(page):
    import hashlib
    return hashlib.sha1(page.encode()).hexdigest()

urls = open("urls.txt")
webpages = ultimap(io_intensive, urls, backend='threading', n_workers=64)
hashes = ultimap(cpu_intensive, webpages, backend='multiprocessing')
print(len(set(hashes)))
```
