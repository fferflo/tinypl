from . import pipe
from .item import Marker
import threading, traceback, queue, os, time, sys, contextlib
import tinypl as pl
import multiprocessing
import threadpoolctl
import multiprocessing.shared_memory

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

class RWLock(object):
    def __init__(self):
        self.w_lock = threading.Lock()
        self.num_r_lock = threading.Lock()
        self.num_r = 0

    def r_acquire(self):
        self.num_r_lock.acquire()
        self.num_r += 1
        if self.num_r == 1:
            self.w_lock.acquire()
        self.num_r_lock.release()

    def r_release(self):
        assert self.num_r > 0
        self.num_r_lock.acquire()
        self.num_r -= 1
        if self.num_r == 0:
            self.w_lock.release()
        self.num_r_lock.release()

    @contextlib.contextmanager
    def r_locked(self):
        try:
            self.r_acquire()
            yield
        finally:
            self.r_release()

    def w_acquire(self, blocking=True):
        return self.w_lock.acquire(blocking=blocking)

    def w_release(self):
        self.w_lock.release()

    @contextlib.contextmanager
    def w_locked(self):
        try:
            self.w_acquire()
            yield
        finally:
            self.w_release()

import types
import pickle
import numpy as np

class readwrite:
    @staticmethod
    def make(src, allow_pickle=True):
        if isinstance(src, (list, tuple)):
            return readwrite.sequence(src, allow_pickle=allow_pickle)
        elif isinstance(src, (dict, types.SimpleNamespace)):
            return readwrite.mapping(src, allow_pickle=allow_pickle)
        elif isinstance(src, np.ndarray):
            return readwrite.numpy(src)
        else:
            if not allow_pickle:
                raise ValueError(f"Value of type {type(src)} must be pickled")
            return readwrite.pickle(src)

    @staticmethod
    def read(src, aux):
        assert isinstance(aux, tuple) and len(aux) == 2
        return aux[0].read(src, aux[1])

    class sequence:
        def __init__(self, x, allow_pickle):
            self.readwrites = [readwrite.make(s, allow_pickle) for s in x]
            self.aux = (type(self), (type(x), [(rw.aux, len(rw)) for rw in self.readwrites]))

        def __len__(self):
            return sum(len(rw) for rw in self.readwrites)

        def write(self, dest):
            idx = 0
            for rw in self.readwrites:
                rw.write(dest[idx:idx + len(rw)])
                idx += len(rw)

        @staticmethod
        def read(src, aux):
            seq_type, rw_auxs = aux
            result = []
            idx = 0
            for rw_aux, rw_len in rw_auxs:
                result.append(rw.read(src[idx:idx + rw_len], aux))
                idx += rw_len
            assert idx == len(src)
            return seq_type(result)

    class mapping:
        def __init__(self, x, allow_pickle):
            dict_type = type(x)
            if isinstance(x, types.SimpleNamespace):
                x = vars(x)
            self.readwrites = {k: readwrite.make(v, allow_pickle) for k, v in x.items()}
            self.aux = (type(self), (dict_type, {k: (rw.aux, len(rw)) for k, rw in self.readwrites.items()}))

        def __len__(self):
            return sum(len(rw) for rw in self.readwrites.values())

        def write(self, dest):
            idx = 0
            for rw in self.readwrites.values():
                rw.write(dest[idx:idx + len(rw)])
                idx += len(rw)
        
        @staticmethod
        def read(src, aux):
            dict_type, rw_auxs = aux
            result = {}
            idx = 0
            for k, (rw_aux, rw_len) in rw_auxs.items():
                result[k] = readwrite.read(src[idx:idx + rw_len], rw_aux)
                idx += rw_len
            assert idx == len(src)
            return dict_type(**result)

    class numpy:
        def __init__(self, src):
            self.src = src
            self.aux = (type(self), (src.shape, src.dtype))

        def __len__(self):
            return self.src.nbytes
        
        def write(self, dest):
            assert len(dest) == self.src.nbytes
            dest = np.ndarray(self.src.shape, dtype=self.src.dtype, buffer=dest)
            dest[:] = self.src

        @staticmethod
        def read(src, aux):
            shape, dtype = aux
            result = np.copy(np.ndarray(shape, dtype=dtype, buffer=src))
            assert result.nbytes == len(src)
            return result

    class pickle:
        def __init__(self, src):
            self.src = pickle.dumps(src)
            self.aux = (type(self), None)

        def __len__(self):
            return len(self.src)

        def write(self, dest):
            dest[:] = self.src

        @staticmethod
        def read(src, aux):
            return pickle.loads(src)

class SharedMemoryRingBuffer:
    class Item:
        def __init__(self, idx, begin, end, aux):
            assert end > begin
            self.idx = idx
            self.begin = begin
            self.end = end
            self.aux = aux

    def __init__(self, size, allow_pickle=True, verbose=False):
        if os.name != "posix":
            raise NotImplementedError("SharedMemoryRingBuffer is only supported on POSIX systems.")
        self.shm = multiprocessing.shared_memory.SharedMemory(create=True, size=size)
        self.shm.unlink() # Unlink so that shared memory is freed when this process dies

        self.allow_pickle = allow_pickle
        self.size = size
        self.verbose = verbose

        self.lock = multiprocessing.RLock()
        self.begin = multiprocessing.Value("q", self.size)
        self.end = multiprocessing.Value("q", 0)
        self.next_write_idx = multiprocessing.Value("q", 0)
        self.read_event = multiprocessing.Event()
        self.read_event.set()
        self.item_queue = multiprocessing.Queue()
        self.next_read_idx = 0
        self.next_items = []
        self.returned_items = []

    def close(self):
        self.item_queue.close()
        self.shm.close()

    @property
    def num_stored_items(self):
        with self.lock:
            return self.next_write_idx.value - self.next_read_idx

    @property
    def free_ratio(self):
        with self.lock:
            end = self.end.value
            begin = self.begin.value
            if begin < end:
                return (begin + self.size - end) / self.size
            else:
                return (begin - end) / self.size

    def write(self, data):
        data = readwrite.make(data, self.allow_pickle)
        if len(data) > self.size:
            raise ValueError("Data too large to fit in buffer")
        while True:
            with self.lock:
                begin = self.begin.value
                end = self.end.value
                if begin < end and self.size - end > len(data):
                    # Data fits after begin and end
                    item_begin = end
                elif begin < end and begin > len(data):
                    # Data fits before begin and end, possible leaving a gap at the end
                    item_begin = 0
                elif begin > end and begin - end > len(data):
                    # Data fits between end and begin
                    item_begin = end
                else:
                    # Data does not fit
                    item_begin = None

                if item_begin is not None:
                    # Write item
                    item = self.Item(self.next_write_idx.value, item_begin, item_begin + len(data), aux=data.aux)
                    self.end.value = item.end % self.size
                    self.next_write_idx.value = item.idx + 1
                    break

            # Failed to write, wait for read
            self.read_event.wait(5.0)
            self.read_event.clear()

        data.write(self.shm.buf[item.begin:item.end])

        self.item_queue.put(item)
        return item

    def read(self, item):
        if not isinstance(item, self.Item):
            raise ValueError("Invalid item", self.num_stored_items)

        # Update items
        with self.lock:
            while not any(x.idx == item.idx for x in self.next_items):
                self.next_items.append(self.item_queue.get()) # TODO: detect deadlock if this times out, but shm is full
            self.next_items.sort(key=lambda x: x.idx)

        # Read and deserialize data
        data = readwrite.read(self.shm.buf[item.begin:item.end], item.aux)

        # Remove item from queue
        with self.lock:
            self.next_items = [x for x in self.next_items if x.idx != item.idx]
            item.aux = None
            self.returned_items.append(item)
            self.returned_items.sort(key=lambda x: x.idx)

            # Update begin if possible
            updated = False
            while len(self.returned_items) > 0 and self.returned_items[0].idx == self.next_read_idx:
                updated = True
                item = self.returned_items.pop(0)
                self.begin.value = item.end
                self.next_read_idx += 1
            if self.begin.value == self.end.value:
                # Buffer is empty
                self.begin.value = self.size
                self.end.value = 0
                assert len(self.returned_items) == 0
                assert self.next_read_idx == self.next_write_idx.value, f"{self.next_read_idx} == {self.next_write_idx.value}"
            if updated:
                self.read_event.set() # Signal that a new item can be written

        return data


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

        self.output_queue_lock = RWLock()

        # Start output processes
        self.started_output_workers = self.ctx.Value("i", 0)
        self.running_output_workers = self.ctx.Value("i", 0)
        self.num_output_workers_cleanup = self.ctx.Value("i", 0)
        self.num_output_workers_flushed = self.ctx.Value("i", 0)
        self.output_workers = []
        self.num_output_workers = workers
        self.all_output_workers_started = self.ctx.Event()
        self.all_output_workers_flushed = self.ctx.Event()
        for _ in range(workers):
            worker = self.ctx.Process(target=map._work_output, args=(self.input_queue, self.output_queue, self.force_stop, self.started_output_workers, self.running_output_workers, self.num_output_workers_cleanup, self.num_output_workers_flushed, func, on_worker_error, self.all_output_workers_started, self.all_output_workers_flushed, workers, force_single_thread), daemon=True)
            worker.start()
            self.output_workers.append(worker)

        start = time.time()
        while not self.all_output_workers_started.wait(0.5):
            if time.time() - start > start_timeout:
                raise WorkerStartTimeoutException(f"Timeout when waiting for output workers to start. {self.running_input_workers} input workers and {self.running_output_workers.value} output workers are running.")

        self.stop_lock = threading.Lock()

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

    @property
    def input_fill(self):
        if self.input_queue is None:
            return 0.0
        else:
            return float(min(max(self.input_queue.qsize(), 0), self.input_maxsize)) / self.input_maxsize

    @property
    def fill(self):
        if self.output_queue is None:
            return 0.0
        else:
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
            self.running_input_workers -= 1
            self.stopped_input_workers += 1
            if self.stopped_input_workers == self.input_workers_num:
                for _ in range(self.num_output_workers):
                    self.input_queue.put(StopSignal())

    @staticmethod
    def _work_output(input_queue, output_queue, force_stop, started_workers, running_workers, num_workers_cleanup, num_workers_flushed, func, on_worker_error, all_output_workers_started, all_output_workers_flushed, workers_num, force_single_thread):
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

        # Last process to reach this point will put a StopSignal on the output queue
        with num_workers_cleanup.get_lock():
            num_workers_cleanup.value += 1
            put_stop_signal = num_workers_cleanup.value == workers_num

        # All other processes will flush the output queue to ensure that items are inserted before the StopSignal
        if not put_stop_signal:
            output_queue.close()
            output_queue.join_thread()
        with num_workers_flushed.get_lock():
            num_workers_flushed.value += 1
            if num_workers_flushed.value == workers_num:
                all_output_workers_flushed.set()

        # The chosen process will wait for all other processes to flush the queue and then put the StopSignal
        if put_stop_signal:
            all_output_workers_flushed.wait()
            output_queue.put(StopSignal())
            output_queue.close()
            output_queue.join_thread()

        with running_workers.get_lock():
            running_workers.value -= 1

    def stop(self):
        with self.stop_lock:
            self.force_stop.value = 1
            self.input.stop()
            try:
                self.input_queue.put(StopSignal(), block=False)
            except:
                pass

            # Stop output workers
            with self.output_queue_lock.r_locked():
                start_time = time.time()
                while (not self.output_queue is None and not self.output_queue.empty()) or self.running_output_workers.value > 0:
                    try:
                        self.input_queue.put(StopSignal(), block=False)
                    except:
                        pass
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
                    print("WARNING: Output worker did not stop in time, terminating it.")
                    process.terminate()
                    process.join(5.0)
                    if process.is_alive():
                        raise WorkerEndTimeoutException(f"Timeout when waiting for output worker to terminate.")
                process.close()
            self.output_workers = []

            if not self.output_queue is None:
                # Acquire write access to output_queue
                while not self.output_queue_lock.w_acquire(blocking=False):
                    with self.output_queue_lock.r_locked():
                        try:
                            self.output_queue.put(StopSignal(), block=False)
                        except:
                            pass

                while not self.output_queue.full():
                    self.output_queue.put(StopSignal())
                self.output_queue.close()
                self.output_queue.join_thread()
                self.output_queue = None

                self.output_queue_lock.w_release()

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

        # Retrieve item from queue
        with self.output_queue_lock.r_locked():
            if self.output_queue is None:
                raise StopIteration
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
