#!/usr/bin/python
'''
This subclasses redis.client.PubSub from Andy McGurdy's redis-py
'''

import threading
from redis._compat import nativestr
from redis.client import PubSub
from redis.exceptions import PubSubError
from redis._compat import iteritems
from weakref import WeakKeyDictionary
from multiprocessing.pool import ThreadPool
import time as mod_time # imported as it is in redis-py

class AsyncPubSub(PubSub):
    '''
    Replaces redis.client.PubSub
    This runs message handlers in an asyncronous threadpool
    '''
    def __init__(self, connection_pool, threadpool_size=5, **kwargs):
        super(AsyncPubSub, self).__init__(connection_pool, **kwargs)
        if not hasattr(threading.current_thread(), "_children"):
            threading.current_thread()._children = WeakKeyDictionary()
        self.threadpool = ThreadPool(threadpool_size)
        self.running = []

    def handle_message(self, response, ignore_subscribe_messages=False):
        '''
        Replacement for the default PubSub message handler, use the threadpool for handling
        Mostly copied from http://github.com/andymccurdy/redis-py/master/redis/client.py
        '''
        message_type = nativestr(response[0])
        if message_type == 'pmessage':
            message = {
                'type': message_type,
                'pattern': response[1],
                'channel': response[2],
                'data': response[3],
            }
        else:
            message = {
                'type': message_type,
                'pattern': None,
                'channel': response[1],
                'data': response[2],
            }

        # if this is an unsubscribe message, remove it from memory
        if message_type in self.UNSUBSCRIBE_MESSAGE_TYPES:
            subscribed_dict = None
            if message_type == 'punsubscribe':
                subscribed_dict = self.patterns
            else:
                subscribed_dict = self.channels
            try:
                del subscribed_dict[message['channel']]
            except KeyError:
                pass

        if message_type in self.PUBLISH_MESSAGE_TYPES:
            # if there's a message handler, invoke it
            handler = None
            if message_type == 'pmessage':
                handler = self.patterns.get(message['pattern'], None)
            else:
                handler = self.channels.get(message['channel'], None)
            if handler:
                res = self.threadpool.apply_async(handler, [message])
                return None

        else:
            # this is a subscribe/unsubscribe message. ignore if we don't
            # want them
            if ignore_subscribe_messages or self.ignore_subscribe_messages:
                return None

        return message

    def run_in_thread(self, sleep_time=0):
        '''
        Replacement for the default PubSub run_in_thread method from
        http://github.com/andymccurdy/redis-py/master/redis/client.py
        '''
        for channel, handler in iteritems(self.channels):
            if handler is None:
                raise PubSubError("Channel: '%s' has no handler registered" % channel)
        for pattern, handler in iteritems(self.channels):
            if handler is None:
                raise PubSubError("Pattern: '%s' has no handler registered" % pattern)
        pubsub = self

        class WorkerThread(threading.Thread):
            ''' Listens for messages on subscriptions '''
            def __init__(self, *args, **kwargs):
                super(WorkerThread, self).__init__(*args, **kwargs)
                self.daemon = True
                self._running = False

            def run(self):
                ''' loop while running on subscriptions '''
                if self._running:
                    return
                self._running = True
                while self._running and pubsub.subscribed:
                    pubsub.get_message(ignore_subscribe_messages=True)
                    mod_time.sleep(sleep_time)

            def stop(self):
                ''' stops the main loop '''
                self._running = False
                self.join()

        thread = WorkerThread()
        thread.start()
        return thread
