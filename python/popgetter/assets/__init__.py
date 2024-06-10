from __future__ import annotations

from . import be, ni, uk, us

countries = [(mod, mod.__name__.split(".")[-1]) for mod in [be, ni, uk, us]]

__all__ = ["countries"]
