from __future__ import annotations

import importlib.metadata

import popgetter as m


def test_version():
    assert importlib.metadata.version("popgetter") == m.__version__


def test_always_passing():
    assert True

def test_always_failing():
    assert False