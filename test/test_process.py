import tinypl as pl
import pytest

def test_simple():
    pipe = iter(range(0, 10))
    pipe = pl.process.map(pipe, lambda x: x * 2, workers=4)
    assert set(pipe) == set(range(0, 20, 2))

def test_multiple():
    pipe = iter(range(0, 10))
    pipe = pl.process.map(pipe, lambda x: x * 2, workers=4)
    pipe = pl.process.map(pipe, lambda x: x / 2, workers=4)
    pipe = pl.process.map(pipe, lambda x: x * 2, workers=4)
    assert set(pipe) == set(range(0, 20, 2))

def test_worker_exception_forward():
    pipe = iter(range(0, 10))
    def raise_error(x):
        raise ValueError("")
    pipe = pl.process.map(pipe, raise_error, workers=1, on_worker_error="forward")
    pipe = pl.process.map(pipe, raise_error, workers=4, on_worker_error="forward")
    pipe = pl.process.map(pipe, raise_error, workers=4, on_worker_error="forward")
    with pytest.raises(pl.process.WorkerException):
        for _ in pipe:
            pass
