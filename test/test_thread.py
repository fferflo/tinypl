import pytest
import tinypl as pl

def test_simple():
    pipe = iter(range(0, 10))
    pipe = pl.thread.map(pipe, lambda x: x * 2, workers=4)
    assert set(pipe) == set(range(0, 20, 2))

def test_multiple():
    pipe = iter(range(0, 10))
    pipe = pl.thread.map(pipe, lambda x: x * 2, workers=4)
    pipe = pl.thread.map(pipe, lambda x: x / 2, workers=4)
    pipe = pl.thread.map(pipe, lambda x: x * 2, workers=4)
    assert set(pipe) == set(range(0, 20, 2))

def test_worker_exception_forward():
    pipe = iter(range(0, 10))
    def raise_error(x):
        raise ValueError
    pipe = pl.each(pipe, raise_error)
    pipe = pl.thread.queue(pipe, workers=4, maxsize=8, on_worker_error="forward")
    pipe = pl.thread.queue(pipe, workers=4, maxsize=8, on_worker_error="forward")
    pipe = pl.thread.queue(pipe, workers=4, maxsize=8, on_worker_error="forward")
    with pytest.raises(pl.thread.queue.WorkerException):
        for _ in pipe:
            pass
