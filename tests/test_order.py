import tinypl as pl
import random, time

def test_order_queue():
    input = range(50)
    pipe = iter(input)
    pipe = order = pl.order.save(pipe, maxsize=8)
    pipe = pl.thread.each(pipe, lambda x: time.sleep(random.uniform(0.01, 0.001)), workers=8, maxsize=50)
    pipe = pl.order.load(pipe, order)

    assert list(pipe) == list(input)

def test_order_multiprocess():
    input = range(50)
    pipe = iter(input)
    pipe = order = pl.order.save(pipe, maxsize=8)
    pipe = pl.process.each(pipe, lambda x: time.sleep(random.uniform(0.01, 0.001)), workers=8, maxsize=50)
    pipe = pl.order.load(pipe, order)

    assert list(pipe) == list(input)
