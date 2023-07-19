import tinypl as pl

def test_filter_even():
    pipe = iter(range(0, 20))
    pipe = pl.filter(pipe, lambda x: x % 2 == 0)
    assert list(pipe) == list(range(0, 20, 2))

def test_filter_empty():
    pipe = iter(range(0, 20))
    pipe = pl.filter(pipe, lambda x: x < 0)
    assert len(list(pipe)) == 0
