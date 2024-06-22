from . import pipe
from .item import Marker
import threading, traceback, queue, os, time, sys
import tinypl as pl
import multiprocessing
import threadpoolctl

class StopSignal:
    pass
class ExceptionSignal:
    def __init__(self, exception):
        self.exception = exception

class WorkerStartTimeoutException(Exception):
    pass
class WorkerEndTimeoutException(Exception):
    pass
class WorkerException(Exception):
    pass

class map(pipe.Pipe):
    WorkerEndTimeoutException = WorkerEndTimeoutException
    WorkerException = WorkerException

    def __init__(self, input, func, workers=1, maxsize=None, input_workers=1, input_maxsize=None, start_timeout=10.0, end_timeout=10.0, warn_timeout=0, start_method=None, on_worker_error="exit", multiprocessing_context=multiprocessing, force_single_thread=True):
        if maxsize is None:
            maxsize = 2 * workers
        if input_maxsize is None:
            input_maxsize = maxsize
        input_maxsize = max(2, input_maxsize)

        self.ctx = multiprocessing_context
        self.input = pipe.wrap(input)
        self.end_timeout = end_timeout
        self.tb = "".join(traceback.format_stack()[:-1])
        self.warn_timeout = warn_timeout
        if on_worker_error not in ["exit", "forward", "forward-and-print"]:
            raise ValueError("on_worker_error parameter must be {exit|forward|forward-and-print}")
        self.on_worker_error = on_worker_error
        self.force_single_thread = force_single_thread

        self.id_lock = threading.Lock()
        self.next_id = 0
        self.id_to_item = {}

        self.input_queue = self.ctx.Queue(input_maxsize)
        self.output_queue = self.ctx.Queue(maxsize)
        self.input_maxsize = input_maxsize
        self.output_maxsize = maxsize

        self.force_stop = self.ctx.Value("b", 0)
        self.exception = None

        # Start input threads
        self.input_lock = threading.Lock()
        self.input_workers_num = input_workers
        self.input_workers = []
        self.started_input_workers = 0
        self.running_input_workers = 0
        self.stopped_input_workers = 0
        self.all_input_workers_started = threading.Event()
        with self.input_lock:
            for _ in range(input_workers):
                thread = threading.Thread(target=self._work_input)
                thread.daemon = True
                thread.start()
                self.input_workers.append(thread)

        start = time.time()
        while not self.all_input_workers_started.wait(0.5):
            if time.time() - start > start_timeout:
                raise WorkerStartTimeoutException(f"Timeout when waiting for input workers to start. {self.running_input_workers} input workers and {self.running_output_workers.value} output workers are running.")

        # Start output processes
        self.started_output_workers = self.ctx.Value("i", 0)
        self.running_output_workers = self.ctx.Value("i", 0)
        self.stopped_output_workers = self.ctx.Value("i", 0)
        self.output_workers = []
        self.all_output_workers_started = self.ctx.Event()
        self.all_output_workers_stopped = self.ctx.Event()
        for _ in range(workers):
            worker = self.ctx.Process(target=map._work_output, args=(self.input_queue, self.output_queue, self.force_stop, self.started_output_workers, self.running_output_workers, self.stopped_output_workers, func, on_worker_error, self.all_output_workers_started, self.all_output_workers_stopped, workers, force_single_thread), daemon=True)
            worker.start()
            self.output_workers.append(worker)

        start = time.time()
        while not self.all_output_workers_started.wait(0.5):
            if time.time() - start > start_timeout:
                raise WorkerStartTimeoutException(f"Timeout when waiting for output workers to start. {self.running_input_workers} input workers and {self.running_output_workers.value} output workers are running.")

        self.stop_lock = threading.Lock()

    @property
    def input_fill(self):
        return float(min(max(self.input_queue.qsize(), 0), self.input_maxsize)) / self.input_maxsize

    @property
    def fill(self):
        return float(min(max(self.output_queue.qsize(), 0), self.output_maxsize)) / self.output_maxsize

    @property
    def workers(self):
        return self.running_output_workers.value

    @property
    def maxsize(self):
        return self.output_maxsize

    def _work_input(self):
        with self.input_lock:
            self.running_input_workers += 1
            self.started_input_workers += 1
            if self.started_input_workers == self.input_workers_num:
                self.all_input_workers_started.set()
        try:
            while self.force_stop.value != 1:
                try:
                    item = pipe._next(self.input)
                except StopIteration:
                    break

                with self.id_lock:
                    next_id = self.next_id
                    self.id_to_item[next_id] = item
                    self.next_id += 1

                if isinstance(item.get(), Marker):
                    self.output_queue.put((item.get(), next_id))
                else:
                    self.input_queue.put((item.get(), next_id))
        except:
            if self.on_worker_error in ["exit", "forward-and-print"]:
                print("Input worker got exception:\n" + traceback.format_exc())
            if self.on_worker_error == "exit":
                sys.exit(-1)
                # os._exit(-1)
            else:
                self.exception = WorkerException("Input worker got exception:\n" + traceback.format_exc())
                self.force_stop.value = 1
        with self.input_lock:
            if self.stopped_input_workers == self.input_workers_num - 1:
                self.input_queue.put(StopSignal())
            self.running_input_workers -= 1
            self.stopped_input_workers += 1

    @staticmethod
    def _work_output(input_queue, output_queue, force_stop, started_workers, running_workers, stopped_workers, func, on_worker_error, all_output_workers_started, all_output_workers_stopped, workers_num, force_single_thread):
        with running_workers.get_lock():
            running_workers.value += 1
        with started_workers.get_lock():
            started_workers.value += 1
            if started_workers.value == workers_num:
                all_output_workers_started.set()
        if force_single_thread:
            if "torch" in sys.modules:
                import torch
                torch.set_num_threads(1)
            if "tensorflow" in sys.modules:
                import tensorflow as tf
                tf.config.threading.set_intra_op_parallelism_threads(1)
                tf.config.threading.set_inter_op_parallelism_threads(1)
            if "cv2" in sys.modules:
                import cv2
                cv2.setNumThreads(0)
                cv2.ocl.setUseOpenCL(False)

        def work():
            try:
                while force_stop.value != 1:
                    packet = input_queue.get()
                    if isinstance(packet, StopSignal):
                        input_queue.put(StopSignal())
                        break

                    value, item_id = packet

                    # Process
                    assert not isinstance(value, Marker)
                    value = func(value)

                    output_queue.put((value, item_id))
            except:
                if on_worker_error in ["exit", "forward-and-print"]:
                    print("Output worker got exception:\n" + traceback.format_exc())
                if on_worker_error == "exit":
                    os._exit(-1)
                else:
                    output_queue.put(ExceptionSignal(WorkerException("Worker got exception:\n" + traceback.format_exc())))
                    force_stop.value = 1

        if force_single_thread:
            with threadpoolctl.threadpool_limits(limits=1):
                work()
        else:
            work()

        with running_workers.get_lock():
            running_workers.value -= 1
        with stopped_workers.get_lock():
            if stopped_workers.value == workers_num - 1:
                output_queue.put(StopSignal())
            stopped_workers.value += 1
            if stopped_workers.value == 0:
                all_output_workers_stopped.set()

    def stop(self):
        with self.stop_lock:
            self.force_stop.value = 1
            self.input.stop()

            # Stop output workers
            start_time = time.time()
            while (not self.output_queue is None and not self.output_queue.empty()) or self.running_output_workers.value > 0:
                if time.time() - start_time > self.end_timeout:
                    raise WorkerEndTimeoutException(f"Timeout when waiting for output workers to finish. {self.running_input_workers} input workers and {self.running_output_workers.value} output workers still running, got {self.input_queue.qsize() if not self.input_queue is None else 0} items in input queue and {self.output_queue.qsize() if not self.output_queue is None else 0} items in output queue.")

                if not self.output_queue is None:
                    while not self.output_queue.empty():
                        try:
                            item = self.output_queue.get(block=False)
                            if isinstance(item, ExceptionSignal) and self.exception is None:
                                self.exception = item.exception
                        except:
                            pass

                time.sleep(0.1)

            # Stop input workers
            while (not self.input_queue is None and not self.input_queue.empty()) or self.running_input_workers > 0:
                if time.time() - start_time > self.end_timeout:
                    raise WorkerEndTimeoutException(f"Timeout when waiting for input workers to finish. {self.running_input_workers} input workers and {self.running_output_workers.value} output workers still running, got {self.input_queue.qsize() if not self.input_queue is None else 0} items in input queue and {self.output_queue.qsize() if not self.output_queue is None else 0} items in output queue.")

                if not self.input_queue is None:
                    while not self.input_queue.empty():
                        try:
                            item = self.input_queue.get(block=False)
                        except:
                            pass

                time.sleep(0.1)
    
            # Close handles
            for process in self.output_workers:
                process.join(5.0)
                if process.is_alive():
                    # print("WARNING: Output worker did not stop in time, terminating it.")
                    process.terminate()
                    process.join(5.0)
                    if process.is_alive():
                        raise WorkerEndTimeoutException(f"Timeout when waiting for output worker to terminate.")
                process.close()
            self.output_workers = []
            if not self.output_queue is None:
                self.output_queue.close()
                self.output_queue = None

            for thread in self.input_workers:
                thread.join(5.0)
            self.input_workers = []
            if not self.input_queue is None:
                self.input_queue.close()
                self.input_queue = None

    def _next(self):
        # If exception was already raised, raise it again
        if not self.exception is None:
            self.stop()
            raise self.exception
        # If force stop was requested, stop the pipeline and raise StopIteration or exception
        if self.force_stop.value == 1:
            self.stop()
            if not self.exception is None:
                raise self.exception
            else:
                raise StopIteration
        if self.output_queue is None:
            raise StopIteration

        # Retrieve item from queue
        try:
            packet = self.output_queue.get(timeout=self.warn_timeout if self.warn_timeout > 0 else None)
        except queue.Empty:
            print("Waited more than " + str(self.warn_timeout) + " seconds for multiprocessed output queue at:\n" + str(self.tb))
            packet = self.output_queue.get()

        # If exception was raised after pulling item, stop the pipeline and raise it
        if not self.exception is None:
            self.stop()
            raise self.exception

        # Otherwise forward the pulled item
        if isinstance(packet, StopSignal):
            self.stop()
            raise StopIteration
        elif isinstance(packet, ExceptionSignal):
            if self.exception is None:
                self.exception = packet.exception
            self.stop()
            raise packet.exception
        else:
            new_value, item_id = packet
            with self.id_lock:
                item = self.id_to_item[item_id]
                del self.id_to_item[item_id]
            item.set(new_value)
            return item

def each(pipe, func, **kwargs):
    def outer(x):
        func(x)
        return x
    return map(pipe, outer, **kwargs)