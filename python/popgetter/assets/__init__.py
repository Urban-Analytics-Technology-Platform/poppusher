from __future__ import annotations

from . import bel, gb_nir, uk, us

countries = [(mod, mod.__name__.split(".")[-1]) for mod in [bel, gb_nir, uk, us]]

__all__ = ["countries"]
