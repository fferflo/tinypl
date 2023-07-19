import tinypl as pl

def print_on_item(pipe, msg_fn=lambda x: str(x)):
    if isinstance(msg_fn, str):
        msg_fn = lambda item, result=msg_fn: result
    def mapper(item):
        print(msg_fn(item))
    return pl.each(mapper, pipe)
