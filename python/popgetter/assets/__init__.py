from __future__ import annotations

from . import bel, gb_eaw, gb_nir, gb_sct, uk, usa

countries = [
    (mod, mod.__name__.split(".")[-1]) for mod in [bel, gb_nir, uk, usa, gb_eaw, gb_sct]
]

__all__ = ["countries"]
