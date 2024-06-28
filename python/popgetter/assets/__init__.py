from __future__ import annotations

from . import bel, gb_nir, uk, usa

countries = [(mod, mod.__name__.split(".")[-1]) for mod in [bel, gb_nir, uk, usa]]

__all__ = ["countries"]
