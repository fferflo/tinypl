from . import pipe
from .item import Marker
import tinypl as pl

class map(pipe.Pipe):
    def __init__(self, input, func):
        super().__init__()
        self.func = func
        self.input = pipe.wrap(input)

    def stop(self):
        self.input.stop()

    def _next(self):
        item = pipe._next(self.input)

        if not isinstance(item.get(), Marker):
            item.set(self.func(item.get()))
        return item

def each(pipe, func, **kwargs):
    def outer(x):
        func(x)
        return x
    return map(pipe, outer, **kwargs)