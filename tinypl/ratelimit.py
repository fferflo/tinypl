from . import pipe
import threading, time

class ratelimit(pipe.Pipe):
    def __init__(self, input, num, period):
        super().__init__()
        self.input = pipe.wrap(input)
        self.lock = threading.RLock()
        self.last_times = []
        self.num = num
        self.period = period

    def stop(self):
        self.input.stop()

    @property
    def time_to_next_drop(self):
        with self.lock:
            if len(self.last_times) == 0:
                return 0.0
            return max(self.last_times[0] + self.period - time.time(), 0.0)

    def _next(self):
        while True:
            with self.lock:
                t = time.time() - self.period
                while len(self.last_times) > 0 and self.last_times[0] < t:
                    self.last_times.pop(0)
                if len(self.last_times) >= self.num:
                    wait = self.time_to_next_drop
                else:
                    self.last_times.append(time.time())
                    break
            time.sleep(wait)
        return pipe._next(self.input)