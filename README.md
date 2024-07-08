

# tinypl

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![PyPI version shields.io](https://img.shields.io/pypi/v/tinypl.svg)](https://pypi.python.org/pypi/tinypl/)

> A lightweight library for building data-processing pipelines in Python.

#### Content

1. [Installation](#installation)
2. [Usage](#usage)
    1. [Basics](#basics)
    2. [Iterators & iterables](#iterators-iterables)
    3. [Multi-threading & multi-processing](#multi-threading-multi-processing)
    4. [Markers](#markers)
    5. [Ordering](#ordering)
3. [Applications](#applications)
    1. [Dataloader for Deep Learning](#dataloader-for-deep-learning)

## Install

```
pip install git+https://github.com/fferflo/tinypl.git
```

## Usage

### Basics

The following is an example of a single-threaded multi-stage pipeline for processing a sequence of numbers:

```python
import tinypl as pl

# Input to the pipeline: Can be any iterable object (list, sequence, set, etc), or an iterator.
pipe = range(100) 

# Apply a function to every element: [0, 1, 2, 3, ...] -> [1, 4, 7, 10, ...]
pipe = pl.map(pipe, lambda x: 3 * x + 1)

# Keep only even numbers: [1, 4, 7, 10, ...] -> [4, 10, 16, 22, ...]
pipe = pl.filter(pipe, lambda x: x % 2 == 0)

# Partition the pipe into lists with size 2: [4, 10, 16, 22, ...] -> [[4, 10], [16, 22], ...]
pipe = pl.partition(pipe, 2)

# Alternative 1: Iterate the output using a for-loop
for x in pipe:
    print(x)
# Alternative 2: Pull all items into a list
print(list(pipe))
# Alternative 3: Pull only a single item from the pipeline
print(next(pipe))
```

The pipeline follows a *lazy-evaluation* paradigm: Elements are only processed when they are retrieved from the pipeline using one of the three shown alternatives. Once an item is retrieved, it is consumed and cannot be retrieved again.

For simplicity, the above code overrides the `pipe` variable at every step to point to the current stage, since the entire pipeline consists only of a single track. Nevertheless, any intermediate stage remains valid and can be used to pull elements:

```python
input = range(100)

# Apply a function to every element
pipe_mapped = pl.map(input, lambda x: x + 1)

# Construct separate stages for even and odd numbers
pipe_even_only = pl.filter(pipe_mapped, lambda x: x % 2 == 0)
pipe_odd_only  = pl.filter(pipe_mapped, lambda x: x % 2 == 1)

print(next(pipe_mapped)) # Prints 1
print(next(pipe_odd_only)) # Discards 2, prints 3
print(next(pipe_even_only)) # Prints 4
print(next(pipe_even_only)) # Discards 5, prints 6
print(next(pipe_mapped)) # Prints 7
```

### Iterators & iterables

The input to the first stage can either be 
1. an **iterable sequence** (e.g. list, set, dictionary, etc), or
2. an **iterator over a sequence** (an object that captures the current state of the iteration and is updated whenever an item is pulled).

When an iterable sequence is passed to the first stage, a stateful iterator is implicitly constructed for it:

```python
numbers = [0, 1, 2, 3, 4]
pipe1 = pl.map(numbers, lambda x: x + 1) # Constructs an iterator over numbers
pipe2 = pl.map(numbers, lambda x: x + 1) # Constructs another iterator over numbers
print(next(pipe1)) # Prints 1
print(next(pipe2)) # Prints 1, since it uses a different iterator
```

An iterable sequence can also first be converted into an iterator using the builtin [`iter`](https://docs.python.org/3/library/functions.html#iter) function:

```python
numbers = iter([0, 1, 2, 3, 4])
pipe1 = pl.map(numbers, lambda x: x + 1)
pipe2 = pl.map(numbers, lambda x: x + 1)
print(next(pipe1)) # Prints 1
print(next(pipe2)) # Prints 2, since both pipelines were constructed from the same iterator
```

Any pipeline stage also implements the iterator interface and can be used as a drop-in replacement, e.g. to pull the next item using `next` or to pull all items into a list using `list`.


### Multi-threading & multi-processing

*tinypl* employs concurrent processing to speed up computation and prefetch elements. It offers the following paradigms for this case:

* **Multi-threading** launches multiple threads in a single process that concurrently run computations. Since the [Global Interpreter Lock](https://en.wikipedia.org/wiki/Global_interpreter_lock) prevents Python code itself from being executed in parallel, multi-threading is only useful when running code that releases the GIL. This includes many libraries implemented with a C/C++ backend (e.g. [Numpy](https://stackoverflow.com/questions/36479159/why-are-numpy-calculations-not-affected-by-the-global-interpreter-lock/36480941#36480941), [OpenCV](https://github.com/opencv/opencv/blob/8839bd572ea8078a9b33d1cdcae19a12443277a8/modules/python/src2/cv2_util.hpp#L19)), as well as blocking operations like reading and writing to disk.
* **Multi-processing** launches multiple processes each with their own Python instance and GIL, such that Python code itself can be executed in parallel. This comes with an overhead of starting up multiple processes, (de)serializing Python objects via [pickle](https://docs.python.org/3/library/pickle.html) and copying memory between processes.

**Multi-threading** in *tinypl* is supported using the `pl.thread.queue` stage that prefetches items from the previous stage using concurrent threads and stores the results in a queue for later retrieval.

```python
pipe = range(100)
pipe = pl.map(pipe, lambda x: 2 * x)

# Prefetches up to 8 items from the pipe using 4 concurrent threads
pipe = pl.thread.queue(pipe, workers=4, maxsize=8)

print(next(pipe)) # Pulls a single prefetched item from the queue
```

As soon as items are removed from the queue, the worker threads will fill the queue up to `maxsize` again. Due to the concurrent evaluation, the order of items is not guaranteed to be the same after the stage.

Stacking multiple `pl.thread.queue` stages works out-of-the-box, since each worker thread will execute the code of the previous stages up to pulling items from the previous queue (and potentially waiting if it is empty).

```python
pipe = range(100)                                   #       ||
                                                    #       ||
pipe = pl.map(pipe, lambda x: x + 1)                #       ||
pipe = pl.thread.queue(pipe, workers=4, maxsize=8)  # ||    \/ Workers from this queue will executed these stages
                                                    # ||
pipe = pl.map(pipe, lambda x: x * 2)                # ||
pipe = pl.thread.queue(pipe, workers=4, maxsize=8)  # \/ Workers from this queue will executed these stages
```

The utility function `pl.thread.map` can be used as a concurrent alternative to `pl.map`, and is equivalent to `pl.map` followed by `pl.thread.queue`.

```python
pipe = pl.thread.map(pipe, lambda x: 2 * x, workers=4, maxsize=8)
```

Lastly, stages of the pipeline that are not thread-safe can be prevented from being accessed concurrently using `pl.thread.mutex`:

```python
def numbers(): # Raises "ValueError: generator already executing" when called concurrently
    i = 0
    while True:
        i += 2
        yield i

pipe = numbers()
pipe = pl.thread.mutex(pipe) # Enforce mutually exclusive access to previous stages
pipe = pl.thread.map(pipe, ..., workers=16)
```

**Multi-processing** in *tinypl* is supported using the `pl.process.map` stage that applies a function to all elements using concurrent processes and stores the result in a queue for later retrieval.

```python
pipe = pl.process.map(pipe, lambda x: 2 * x, workers=4, maxsize=8)
```

Any item passed to or returned from pl.process.map must be [picklable](https://docs.python.org/3/library/pickle.html) to enable inter-process communication. The mapping function cannot have side-effects like writing global/ stage variables. Unlike the multi-threading stage `pl.thread.queue`, multi-processing therefore only supports a *map* function rather than evaluating arbitrary previous stages.

In addition to starting up processes, this also creates a new thread that reads elements from the input stage and writes them in a memory structure that allows for inter-process communication with the worker processes.

Multi-thread and multi-process pipelines can be intermixed arbitrarily:

```python
pipe = range(100)
pipe = pl.thread.map(pipe, lambda x: 1 + x, workers=4, maxsize=8)
pipe = pl.process.map(pipe, lambda x: 2 * x, workers=4, maxsize=8)
```

To reduce the overhead of copying memory between different processes, *tinypl* implements the `pl.process.SharedMemoryRingBuffer` class that allows
passing objects with a large memory footprint between processes using shared memory instead:

```python
ringbuffer = pl.process.SharedMemoryRingBuffer(4 * 1024 * 1024 * 1024) # Allocate 4 GB of shared memory

pipe = range(100)

def stage1(i):
    # Create a large numpy array in this process
    x = np.random.rand(1024, 1024, 200) # 200 MB

    # Serialize and move the array to shared memory. The return value is a reference to the shared memory
    # that can be passed between processes cheaply and can be deserialized later
    x = ringbuffer.write(x)

    return x
pipe = pl.process.map(pipe, stage1, workers=16)

def stage2(x):
    # Read from shared memory and deserialize in main process
    x = ringbuffer.read(x)

    return x
pipe = pl.map(pipe, stage2) # (Reading is currently only supported from the main process)
```

#### Should I use multi-threading or multi-processing?

Some factors have to be considered here: Does some library release the GIL or call another library that releases the GIL? How much time is spent in GIL-locked code compared to GIL-free code? What is the overhead of running additional processes and copying items between processes in `pl.process`?

Both `pl.process.map` and `pl.thread.map` are provided with the same interface and can be exchanged easily to test which one achieves better throughput and/ or memory consumption.

### Markers

Markers are virtual items that can be injected into the pipeline and raise events when they arrive at later stages, e.g. to indicate which part of the input sequence has been processed so far. Markers are forwarded through stages without applying any of the processing steps.

```python
pipe = [range(i + 1) for i in range(50)]
# -> [[0], [0, 1], [0, 1, 2], ...]

# Inject a marker after every item. marker variable acts as identifier for markers inserted here
pipe, marker = pl.marker.insert_periodic(pipe, n=1, after=True)
# -> [[0], marker, [0, 1], marker, ...]

# Flatten the lists (ignores markers)
pipe = pl.flatten(pipe)
# -> [0, marker, 0, 1, marker, 0, 1, 2, marker, ...]

# Apply a function to all items (ignores markers)
pipe = pl.map(pipe, lambda x: x + 1)
# -> [1, marker, 1, 2, marker, 1, 2, 3, marker, ...]

# Print "Hello world!" whenever we see a marker
def hello_world():
    print(f"Hello world!")
pipe = pl.marker.on(pipe, hello_world, marker=marker)

# Create new stages that stop when marker is found, and pull into list
print(list(pl.marker.until(pipe, marker=marker))) # Prints [1]
print(list(pl.marker.until(pipe, marker=marker))) # Prints [1, 2]
print(list(pl.marker.until(pipe, marker=marker))) # Prints [1, 2, 3]
```


### Ordering

The ordering of items at a pipeline stage can be saved via `pl.order.save` and restored at a later stage using `pl.order.load`, for example to retain the order of elements through multi-thread or multi-process stages. To do this, `pl.order.save` annotates each item with a sequence number, and `pl.order.load` pulls items and stores them until they are next in line.

```python
pipe = range(50)

# Save the order of items
pipe = order = pl.order.save(pipe, maxsize=8) # pl.order.save stage is passed to pl.order.load constructor

# Change the order of items
import time, random
pipe = pl.thread.each(pipe, lambda x: time.sleep(random.uniform(0.1, 0.01)), workers=8)

# Restore the order
pipe = pl.order.load(pipe, order)

assert list(pipe) == list(range(50))
```

To prevent the temporary buffer in `pl.order.load` from running out-of-memory (i.e. if the next item is taking very long to process, but items after it are processed faster), `pl.order.save` allows at most `maxsize` items to pass through and otherwise blocks until items are pulled and removed from the `pl.order.load` stage. The `pl.order.save` stage further enforces mutually exclusive access to previous stages similar to `pl.thread.mutex`.



## Related libraries

- PyTorch's [`torch.utils.data`](https://pytorch.org/tutorials/beginner/basics/data_tutorial.html)
- Tensorflow's [`tf.data`](https://www.tensorflow.org/guide/data)
- [Pypeln](https://cgarciae.github.io/pypeln/)
- [mpipe](http://vmlaker.github.io/mpipe/)