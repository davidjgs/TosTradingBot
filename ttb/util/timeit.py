import threading
import time
import logging
import gc
from functools import wraps

from utils.logging import get_logger


def hiresTimer():
    return time.perf_counter


class ThreadLocal(threading.local):
    initialized = False
    def __init__(self, **defaults):
        self.__dict__.update(defaults)
        self.initialized = True

class Timer(object):
    tl = ThreadLocal(timer=None)
    def __init__(self, fmt = 'timer', *args, **kwargs):
        self.fmt = fmt
        self.verbose = kwargs.pop('verbose', True)
        self.logname = kwargs.pop('logname', 'timeit')
        self.threshold = kwargs.pop('threshold', 0)
        self.time_fmt = kwargs.pop('time_fmt', '%.4f')
        self.log_level = kwargs.pop('log_level', logging.INFO)
        self.gc_off = kwargs.pop('gc_off', False)
        self.process_time = kwargs.pop('process_time', False)
        self.args = args
        self._elapsed = 0
        self._count = 0
        self._childTime = 0
        self._start = None
        self.parent = None
        self.getTime = hiresTimer()
        if self.process_time:
            self.getTime = time.process_time
        if kwargs:
            logging.error(f'Timer: unkwown kwarg: {kwargs}')

    def __enter__(self):
        if self._start is not None:
            raise RuntimeError('Time was invloked re-entrantly')
        self.restoreGc = self.gc_off and gc.isenabled()
        if self.restoreGc:
            gc.collect()
            gc.disable()
        self._start = self.getTime()
        self.parent = Timer.tl.timer
        Timer.tl.timer = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end = self.getTime()
        t = end - self._start
        self._elapsed += t
        self._count += 1
        if self.parent:
            self.parent._childTime += t
            Timer.tl.timer=self.parent
        self._start = None
        if self.restoreGc:
            gc.enable()
        if self.verbose and (self._elapsed >= self.threshold):
            self._log(':%s seconds' % self.time_fmt, (self._elapsed, ))

    def __call__(self, fn):
        @wraps(fn)
        def __wrapper(*args, **kwargs):
            with self:
                return fn(*args, **kwargs)
        return __wrapper

    def _log(self, msg, args):
        logger = get_logger(self.logname)
        logger.log(self.log_level, self.fmt + msg, *(self.args+args))

    def name(self):
        return self.fmt % tuple(self.args)

    def elapsed(self):
        return self._elapsed

    def count(self):
        return self._count

    def childTime(self):
        return self._childTime

def timed(func):
    return Timer(func.__name__)(func)
