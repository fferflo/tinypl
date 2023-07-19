import tinypl as pl

def test_map_multiply():
    pipe = iter(range(0, 10))
    pipe = pl.map(pipe, lambda x: x * 2)
    assert list(pipe) == list(range(0, 20, 2))
