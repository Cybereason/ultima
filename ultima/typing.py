from typing import Literal

from .backend import BackendArgument


ReturnKey = Literal['none', 'idx', 'input']
Error = Literal['raise', 'return', 'ignore', 'log']
