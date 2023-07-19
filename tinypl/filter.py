from . import pipe
from .item import Marker

class filter(pipe.Pipe):
    def __init__(self, input, pred):
        super().__init__()
        self.pred = pred
        self.input = pipe.wrap(input)

    def stop(self):
        self.input.stop()

    def _next(self):
        while True:
            item = pipe._next(self.input)
            if isinstance(item.get(), Marker) or self.pred(item.get()):
                return item
