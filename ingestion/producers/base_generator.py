"""Common utilities for RabbitMQ event generators.

This module defines :class:`BaseGenerator` which provides a reproducible
random number generator.  Producers can inherit from this class or
instantiate it directly to ensure that mock data generation is
deterministic given the same seed value.
"""

from __future__ import annotations

import logging
from random import Random
from typing import Sequence, TypeVar

T = TypeVar("T")


class BaseGenerator:
    """Base class providing seeded randomness utilities.

    Parameters
    ----------
    seed:
        Seed string used to initialize the underlying random number
        generator.  Using a consistent seed across runs ensures
        reproducibility of mock data streams, which is important for
        debugging and deterministic tests.
    """

    def __init__(self, seed: str) -> None:
        self._rng = Random(seed)

    def choice(self, seq: Sequence[T]) -> T:
        """Return a random element from *seq* using the seeded RNG."""
        return self._rng.choice(list(seq))

    def randint(self, a: int, b: int) -> int:
        """Return random integer ``N`` such that ``a <= N <= b``."""
        return self._rng.randint(a, b)


def get_logger(name: str) -> logging.Logger:
    """Return a logger configured to emit to stdout.

    This helper centralizes logger configuration so that individual
    generator scripts can obtain a ready-to-use logger without repeating
    boilerplate setup.  If the logger for *name* has not been configured,
    a :class:`~logging.StreamHandler` with a simple format is attached and
    the level is set to ``INFO``.
    """

    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
