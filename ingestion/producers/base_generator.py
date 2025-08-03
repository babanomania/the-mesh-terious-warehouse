"""Common utilities for RabbitMQ event generators.

This module defines :class:`BaseGenerator` which provides a reproducible
random number generator.  Producers can inherit from this class or
instantiate it directly to ensure that mock data generation is
deterministic given the same seed value.
"""

from __future__ import annotations

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
