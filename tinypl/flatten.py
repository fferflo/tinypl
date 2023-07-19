from . import pipe
import queue, threading
from .item import Marker, Item

def define_class(base_class):
    class flatten(base_class):
        def __init__(self, input):
            super().__init__()
            self.input = pipe.wrap(input)

        def stop(self):
            self.input.stop()
            super().stop()

        def _next_unflattened(self):
            item = pipe._next(self.input)
            if isinstance(item.get(), Marker):
                return [item]
            else:
                def generator(item=item):
                    for x in item.get():
                        yield Item(x)
                return generator()
    return flatten

flatten_eager = define_class(pipe.EagerFlattenedPipe)
flatten = flatten_lazy = define_class(pipe.LazyFlattenedPipe)