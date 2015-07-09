#!/usr/bin/python
'''
Defines a non-blocking lock context for threading.Lock objects
'''
import time


class NBLockContext(object):
    '''
    Creates a context for threading.Lock objects, similar to the default but which
    returns the result of a non-blocking acquisition, rather than blocking till acquiring

    Example Usage:
    # No timeout, try once and report
    with NBLockContext(mylock) as acquired:
        if acquired:
            # do stuff
            pass
        else:
            # don't do stuff (or realize the lock is NOT acquired)
            pass
    # With timeout, interval
    with NBLockContext(mylock, timeout=5, sleep_interval=0.1) as acquired:
        if acquired:
            print 'Acquired within 5s'
        else:
            print 'Didn't lock within 5s'
    '''
    def __init__(self, lock, timeout=0, sleep_interval=0.01):
        self.lock = lock
        self.timeout = timeout
        self.sleep_interval = sleep_interval
        assert hasattr(self.lock, 'acquire')
        assert hasattr(self.lock, 'release')
        self.acquired = False

    def __enter__(self):
        '''
        Enter method, returns the value given "as" in the with
        '''
        if self.timeout != 0:
            start = time.time()
        self.acquired = self.lock.acquire(False)
        while (not self.acquired) and (self.timeout != 0) and (time.time() - start < self.timeout):
            time.sleep(self.sleep_interval)
            self.acquired = self.lock.acquire(False)
        return self.acquired

    def __exit__(self, type, value, traceback):
        '''
        Exit method, releases the lock if we acquired it
        '''
        if self.acquired:
            self.lock.release()
            self.acquired = False
