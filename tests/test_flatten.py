import tinypl as pl
import pytest

@pytest.mark.parametrize("flatten", [pl.flatten_eager, pl.flatten_lazy])
def test_flatten_pairs(flatten):
    pipe = iter([(i, i + 1) for i in range(0, 20, 2)])
    pipe = flatten(pipe)
    assert list(pipe) == list(range(0, 20))

@pytest.mark.parametrize("flatten", [pl.flatten_eager, pl.flatten_lazy])
def test_flatten_empty(flatten):
    pipe = [[] for i in range(0, 20)]
    pipe = flatten(pipe)
    assert len(list(pipe)) == 0
