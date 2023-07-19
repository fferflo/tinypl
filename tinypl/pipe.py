from .item import Item, Marker
import threading, queue

class Pipe:
    def __iter__(self):
        return self

    def __next__(self):
        while True:
            value = self._next().get()
            if not isinstance(value, Marker):
                return value

class LazyFlattenedPipe(Pipe):
    def __init__(self):
        self.iterator = None
        self._flatten_lock = threading.Lock()

    def stop(self):
        self.iterator = None

    def _next(self):
        with self._flatten_lock:
            while True:
                if self.iterator is None:
                    iterator = self._next_unflattened() # Non-generators raise StopIteration here
                    if hasattr(iterator, "__next__"):
                        self.iterator = iterator
                    elif hasattr(iterator, "__iter__"):
                        self.iterator = iter(iterator)
                    else:
                        raise ValueError("Item produced in LazyFlattenedPipe is neither an iterator nor an iterable")

                try:
                    item = next(self.iterator)
                except StopIteration:
                    self.iterator = None
                else:
                    if item is None: # Generators do not allow raising StopIteration, so they indicate end of pipeline by yielding None instead of an item
                        raise StopIteration
                    if not isinstance(item, Item):
                        raise ValueError("Returned non-Item object to LazyFlattenedPipe")
                    return item

class EagerFlattenedPipe(Pipe):
    def __init__(self):
        self.queue = queue.Queue()

    def stop(self):
        while not self.queue.empty():
            try:
                self.queue.get(block=False)
            except queue.Empty:
                pass

    def _next(self):
        while True:
            try:
                item = self.queue.get(block=False)
                if item is None: # Generators do not allow raising StopIteration, so they indicate end of pipeline by yielding None instead of an item
                    raise StopIteration
                if not isinstance(item, Item):
                    raise ValueError("Returned non-Item object to EagerFlattenedPipe")
                return item
            except queue.Empty:
                for item in self._next_unflattened(): # Non-generators raise StopIteration here
                    self.queue.put(item)



class IteratorPipe(Pipe):
    def __init__(self, iterator):
        super().__init__()
        if hasattr(iterator, "__next__"):
            self.iterator = iterator
        elif hasattr(iterator, "__iter__"):
            self.iterator = iter(iterator)
        else:
            raise ValueError("Pipe object is neither an iterator nor an iterable")

    def stop(self):
        self.iterator = None

    def _next(self):
        return Item(self.iterator.__next__())

def wrap(pipe_or_iterator):
    if isinstance(pipe_or_iterator, Pipe):
        return pipe_or_iterator
    else:
        return IteratorPipe(pipe_or_iterator)

def _next(pipe):
    item = pipe._next()
    if not isinstance(item, Item):
        raise ValueError("Pipe returned non-Item object")
    return item