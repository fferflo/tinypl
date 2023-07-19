from . import pipe, util
import threading, traceback
import tinypl as pl

class save(pipe.Pipe):
    class Annotation:
        def __init__(self, order, index):
            self.order = order
            self.index = index

        def remove(self):
            with self.order.output_lock:
                assert self.order.next_annotations[0].index == self.index
                self.order.next_annotations.pop(0)
                self.order.next_output_index += 1
                self.order.can_maybe_push.set()

    def __init__(self, input, maxsize):
        super().__init__()
        self.input = pipe.wrap(input)
        self.next_annotations = []
        self.next_input_index = 0
        self.next_output_index = 0
        self.input_lock = threading.Lock()
        self.output_lock = threading.Lock()

        self._maxsize = maxsize
        self.can_maybe_push = threading.Event()
        self.can_maybe_push.set()
        self.forward_lock = threading.Lock()

        self.force_stop = False

    def stop(self):
        self.force_stop = True
        self.input.stop()

    @property
    def maxsize(self):
        return self._maxsize

    @maxsize.setter
    def maxsize(self, value):
        self._maxsize = value
        self.can_maybe_push.set()

    def _next(self):
        with self.input_lock:
            item = pipe._next(self.input)
            new_annotation = save.Annotation(self, self.next_input_index)
            with self.output_lock:
                self.next_annotations.append(new_annotation)
            item[util.id(self)] = new_annotation
            # print(f"ORDER: Annotating item with sequence element {self.next_input_index}")
            self.next_input_index += 1

            while True:
                with self.forward_lock:
                    if new_annotation.index < self.next_output_index + self._maxsize or self.force_stop:
                        # item is within next {maxsize} sequence elements, so forward it
                        self.can_maybe_push.clear()
                        return item
                self.can_maybe_push.wait(2.0)

class load(pipe.Pipe):
    class FullException(Exception):
        pass

    def __init__(self, input, order):
        super().__init__()
        self.input = pipe.wrap(input)
        self.order = order
        self.lock = threading.Lock()
        self.index_to_item = {}
        self.tb = "".join(traceback.format_stack()[:-1])

    def stop(self):
        self.input.stop()

    def _next(self):
        while True:
            # Check if item from next sequence element is already queued
            with self.lock:
                if len(self.index_to_item) > self.order.maxsize:
                    raise load.FullException()
                next_output_index = self.order.next_output_index
                if next_output_index in self.index_to_item:
                    # Retrieve stored item
                    old_item = self.index_to_item[next_output_index]
                    if not util.id(self.order) in old_item:
                        raise ValueError(f"pl.order.load got order argument that was not saved before. At:\n {str(self.tb)}")
                    # Remove stored item from queue
                    del self.index_to_item[next_output_index]
                    # Remove annotation from queue
                    old_item[util.id(self.order)].remove()
                    # Return item
                    # print(f"ORDER: Returning stored item for sequence element {next_output_index}")
                    return old_item
            # No queued item is next in sequence, retrieve new item from input pipe
            new_item = pipe._next(self.input)
            with self.lock:
                next_output_index = self.order.next_output_index
                if not util.id(self.order) in new_item:
                    raise ValueError(f"pl.order.load got order argument that was not saved before. At:\n {str(self.tb)}")
                annotation = new_item[util.id(self.order)]
                if next_output_index == annotation.index:
                    # New item is next in sequence
                    # Remove annotation from queue
                    annotation.remove()
                    # Return item
                    # print(f"ORDER: Directly returning retrieved item for sequence element {next_output_index}")
                    return new_item
                else:
                    # New item is not next in sequence, store for later
                    # print(f"ORDER: Storing item for sequence element {annotation.index} since sequence element {next_output_index} is next")
                    self.index_to_item[annotation.index] = new_item
