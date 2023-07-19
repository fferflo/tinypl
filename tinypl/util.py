import inspect, builtins

def unpack(func):
    if inspect.isgeneratorfunction(func):
        def wrapper(args):
            for el in func(*args):
                yield el
    else:
        def wrapper(args):
            return func(*args)
    return wrapper

def id(x):
    return builtins.id(x).to_bytes(8, "big")
