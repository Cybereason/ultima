import logging

from .workforce import Workforce, ultimap
from .args import Args
from .backend import (
    InlineBackend,
    ThreadingBackend,
    MultiprocessingBackend,
    MultiprocessingForkBackend,
    MultiprocessingSpawnBackend,
)

logging.getLogger(__name__).addHandler(logging.NullHandler())
