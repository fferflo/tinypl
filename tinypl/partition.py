from . import pipe
from .item import Item, Marker
import traceback, os

class MarkerNotAllowedException(Exception):
    pass

class partition(pipe.LazyFlattenedPipe):
    def __init__(self, input, num, markers=None, split_on_marker=False):
        super().__init__()
        self.num = num
        self.input = pipe.wrap(input)
        self.tb = "".join(traceback.format_stack()[:-1])
        if not (markers is None or markers == "after" or markers == "before"):
            raise ValueError("markers parameter must be one of None, 'before' and 'after'")
        self.markers = markers
        self.split_on_marker = split_on_marker

    def stop(self):
        self.input.stop()
        super().stop()

    def _next_unflattened(self):
        after_markers = []
        items = []
        def make_item():
            return Item([item.get() for item in items])
        while True:
            try:
                item = pipe._next(self.input)
            except StopIteration:
                if len(items) > 0:
                    yield make_item()
                for marker in after_markers:
                    yield marker
                yield None # raises StopIteration in pipe.LazyFlattenedPipe
                break
            if isinstance(item.get(), Marker):
                if self.markers is None:
                    raise MarkerNotAllowedException("Markers not allowed at:\n" + str(self.tb))
                elif self.markers == "after":
                    if len(items) == 0:
                        # No batch currently in construction, so forward marker immediately
                        yield item
                    else:
                        # Wait with forwarding marker until after current batch
                        after_markers.append(item)
                else:
                    assert self.markers == "before"
                    # Forward marker immediately
                    yield item
                if self.split_on_marker:
                    if len(items) > 0:
                        yield make_item()
                        for marker in after_markers:
                            yield marker
                        break
            else:
                items.append(item)
                if len(items) == self.num:
                    yield make_item()
                    for marker in after_markers:
                        yield marker
                    break
