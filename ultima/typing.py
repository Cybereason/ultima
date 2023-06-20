"""
This module contains types that are outward-facing, to be imported and used by users of the package.
"""
from typing import Literal, Union

from .backend import BackendArgument


ReturnKey = Literal['none', 'idx', 'input']
Error = Literal['raise', 'ignore', 'log', 'return']
