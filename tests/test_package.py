from __future__ import annotations

import importlib.metadata

import popgetter as m


def test_version():
    assert importlib.metadata.version("popgetter") == m.__version__
