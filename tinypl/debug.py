from . import pipe

class print_on_item(pipe.Pipe):
    def __init__(self, input, msg_fn):
        super().__init__()
        self.input = pipe.wrap(input)

        if isinstance(msg_fn, str):
            msg_fn = lambda item=None, result=msg_fn: result
        self.msg_fn = msg_fn

    def stop(self):
        self.input.stop()

    def _next(self):
        print(f"PIPE: Pulling   {self.msg_fn()}")
        item = pipe._next(self.input)
        print(f"PIPE: Returning {self.msg_fn(item)}")
        return item
