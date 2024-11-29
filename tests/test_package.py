from __future__ import annotations

import importlib.metadata

import pytest

import poppusher as m


def test_version():
    assert importlib.metadata.version("poppusher") == m.__version__


def test_always_passing():
    assert True


@pytest.mark.xfail()
def test_always_failing():
    pytest.fail(
        "A test which always fails to check the CI infrastructure is working as expected"
    )
