import tinypl as pl
import time

def test():
    n = 100
    dt = 0.01

    pipe = iter(range(0, 100))
    pipe = pl.ratelimit(pipe, num=10, period=dt)

    start = time.time()
    for i in range(n):
        next(pipe)
    end = time.time()
    assert end - start >= 9 * dt