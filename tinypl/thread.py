from . import pipe
import threading, sys, traceback, os, time
import queue as py_queue
import tinypl as pl

class WorkerEndTimeoutException(Exception):
        pass
class WorkerException(Exception):
    pass

class StopSignal:
    pass

class queue(pipe.Pipe):
    WorkerEndTimeoutException = WorkerEndTimeoutException
    WorkerException = WorkerException

    def __init__(self, input, workers, maxsize=None, end_timeout=30.0, warn_timeout=0, on_worker_error="exit"):
        super().__init__()
        if maxsize is None:
            maxsize = 2 * workers
        self.input = pipe.wrap(input)
        assert maxsize > 0
        self.maxsize = maxsize
        self.end_timeout = end_timeout
        self.tb = "".join(traceback.format_stack()[:-1])
        self.warn_timeout = warn_timeout
        if on_worker_error not in ["exit", "forward", "forward-and-print"]:
            raise ValueError("on_worker_error parameter must be {exit|forward|forward-and-print}")
        self.on_worker_error = on_worker_error

        self.q = py_queue.Queue(maxsize)

        self.force_stop = False
        self.exception = None
        self.threads = []

        self.running_workers_lock = threading.Lock()
        self.running_workers = workers
        with self.running_workers_lock:
            for _ in range(workers):
                thread = threading.Thread(target=self._work)
                thread.daemon = True
                thread.start()
                self.threads.append(thread)

        self.stop_lock = threading.Lock()

    @property
    def fill(self):
        return float(min(max(self.q.qsize(), 0), self.maxsize)) / self.maxsize

    @property
    def workers(self):
        return self.running_workers

    def _work(self):
        try:
            while not self.force_stop:
                try:
                    item = pipe._next(self.input)
                except StopIteration:
                    break
                self.q.put(item)
        except:
            if self.on_worker_error in ["exit", "forward-and-print"]:
                print("Worker got exception:\n" + traceback.format_exc())
            if self.on_worker_error == "exit":
                os._exit(-1)
            else:
                if self.exception is None:
                    self.exception = WorkerException("Worker got exception:\n" + traceback.format_exc())
        with self.running_workers_lock:
            if self.running_workers == 1:
                self.q.put(StopSignal())
            self.running_workers -= 1

    def _next(self):
        # If exception was already raised, raise it again
        if not self.exception is None:
            self.stop()
            raise self.exception
        # If force stop was requested, stop the pipeline and raise StopIteration or exception
        if self.force_stop:
            self.stop()
            if not self.exception is None:
                raise self.exception
            else:
                raise StopIteration

        # Retrieve item from queue
        try:
            item = self.q.get(timeout=self.warn_timeout if self.warn_timeout > 0 else None)
        except py_queue.Empty:
            print("Waited more than " + str(self.warn_timeout) + " seconds for queue output at:\n" + str(self.tb))
            item = self.q.get()
        self.q.task_done()

        # If exception was raised after pulling item, stop the pipeline and raise it
        if not self.exception is None:
            self.stop()
            raise self.exception

        # Otherwise forward the item
        if isinstance(item, StopSignal):
            self.stop()
            raise StopIteration
        else:
            return item

    def stop(self):
        with self.stop_lock:
            self.force_stop = True
            self.input.stop()
            start_time = time.time()
            while not self.q.empty() or self.running_workers > 0:
                if time.time() - start_time > self.end_timeout:
                    ex = f"Timeout when waiting for workers to finish. {self.running_workers} workers still running."
                    if not self.exception is None:
                        ex = ex + f" Got previous worker exception: {self.exception}"
                    raise WorkerEndTimeoutException(ex)

                try:
                    item = self.q.get(timeout=0.1)
                except py_queue.Empty:
                    pass
            self.q.put(StopSignal())
            for thread in self.threads:
                thread.join(self.end_timeout)
            self.threads = []

def map(pipe, func, **kwargs):
    pipe = pl.map(pipe, func)
    pipe = pl.thread.queue(pipe, **kwargs)
    return pipe

def each(pipe, func, **kwargs):
    def outer(x):
        func(x)
        return x
    return map(pipe, outer, **kwargs)

class mutex(pipe.Pipe):
    def __init__(self, input):
        super().__init__()
        self.input = pipe.wrap(input)
        self.lock = threading.Lock()

    def stop(self):
        self.input.stop()

    def _next(self):
        with self.lock:
            return pipe._next(self.input)