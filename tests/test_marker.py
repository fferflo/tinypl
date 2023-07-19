import tinypl as pl

def test_until_marker():
    input = range(0, 10)
    pipe = iter(input)
    pipe, marker = pl.marker.insert_periodic(pipe, n=1, after=True)
    for i in input:
        assert list(pl.marker.until(pipe, marker)) == [i]

def test_on_marker():
    input = range(0, 10)
    pipe = iter(input)
    counter = 10
    def func(x):
        nonlocal counter 
        counter += 1
    pipe = pl.marker.on(pipe, func)
    for _ in pipe:
        pass
    assert counter == 10
