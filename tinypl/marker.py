from . import pipe, util
from .item import Marker, Item
import threading

class insert_periodic_(pipe.EagerFlattenedPipe):
    def __init__(self, input, n, after):
        super().__init__()
        if n <= 0:
            raise ValueError("n must be larger than zero")
        self.input = pipe.wrap(input)
        self.n = n
        self.lock = threading.Lock()
        self.i = 0
        self.after = after

    def stop(self):
        self.input.stop()
        super().stop()

    def _next_unflattened(self):
        item = self.input._next()
        if not isinstance(item.get(), Marker):
            with self.lock:
                self.i += 1
                if self.i == self.n:
                    self.i = 0
                    marker = Item(Marker(util.id(self)))
                    return [item, marker] if self.after else [marker, itemi]
                else:
                    return [item]
        else:
            return [item]

def insert_periodic(pipe, n=1, after=True):
    pipe = insert_periodic_(pipe, n=n, after=after)
    return pipe, util.id(pipe) # Interpret as (pipe, marker)

# Must consume marker
class until(pipe.Pipe):
    def __init__(self, input, marker=None):
        super().__init__()
        self.input = pipe.wrap(input)
        self.source = marker
        self.end = False
        self.lock = threading.Lock()

    def stop(self):
        self.input.stop()

    def _next(self):
        with self.lock:
            if self.end:
                raise StopIteration
            item = self.input._next()
            if isinstance(item.get(), Marker) and (self.source is None or item.get().source == self.source):
                self.end = True
                raise StopIteration
            else:
                return item

class on(pipe.EagerFlattenedPipe):
    def __init__(self, input, func, marker=None, consume=False):
        super().__init__()
        self.func = func
        self.input = pipe.wrap(input)
        self.source = marker
        self.consume = consume

    def stop(self):
        self.input.stop()
        super().stop()

    def _next_unflattened(self):
        item = self.input._next()
        if isinstance(item.get(), Marker) and (self.source is None or item.get().source == self.source):
            self.func()
            if self.consume:
                return []
            else:
                return [item]
        else:
            return [item]
