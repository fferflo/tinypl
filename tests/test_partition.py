import tinypl as pl

def test_partition():
    input = range(50)
    pipe = iter(input)
    pipe = pl.partition(pipe, 2)
    for i, x in enumerate(list(pipe)):
        assert x == [2 * i, 2 * i + 1]

def test_partition_split_by_marker():
    input = range(10)
    pipe = iter(input)
    pipe, marker = pl.marker.insert_periodic(pipe, n=1, after=True)
    pipe = pl.partition(pipe, 4, markers="after", split_on_marker=True)
    assert len(list(pipe)) == 10

def test_partition_flatten():
    input = range(50)
    pipe = iter(input)
    pipe = pl.partition(pipe, 3)
    pipe = pl.flatten(pipe)
    assert list(pipe) == list(input)
