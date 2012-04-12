# -*- coding: utf-8 -*-

"""HotQueue is a Python library that allows you to use Redis as a message queue
within your Python programs.
"""

from contextlib import contextmanager
from functools import wraps
try:
    import cPickle as pickle
except ImportError:
    import pickle

from redis import Redis


__all__ = ['HotQueue']

__version__ = '0.2.6'


DEFAULT_BULK_SIZE = 500


def key_for_name(name):
    """Return the key name used to store the given queue name in Redis."""
    return 'hotqueue:%s' % name


class HotQueue(object):
    
    """Simple FIFO message queue stored in a Redis list. Example:

    >>> from hotqueue import HotQueue
    >>> queue = HotQueue("myqueue", host="localhost", port=6379, db=0)
    
    :param name: name of the queue
    :param serializer: the class or module to serialize msgs with, must have
        methods or functions named ``dumps`` and ``loads``,
        `pickle <http://docs.python.org/library/pickle.html>`_ is the default,
        if ``None`` is given then messages will be passed to Redis without serialization
    :param kwargs: additional kwargs to pass to :class:`Redis`, most commonly
        :attr:`host`, :attr:`port`, :attr:`db`
    """
    
    def __init__(self, name, serializer=pickle, **kwargs):
        self.name = name
        self.serializer = serializer
        self.__redis = Redis(**kwargs)
        self._bulk_mode = False
        self._bulk_size = DEFAULT_BULK_SIZE
        self._bulk_items = []
    
    def __len__(self):
        return self.__redis.llen(self.key)
    
    @property
    def key(self):
        """Return the key name used to store this queue in Redis."""
        return key_for_name(self.name)
    
    def clear(self):
        """Clear the queue of all messages, deleting the Redis key."""
        self.__redis.delete(self.key)
    
    def consume(self, **kwargs):
        """Return a generator that yields whenever a message is waiting in the
        queue. Will block otherwise. Example:

        >>> for msg in queue.consume(timeout=1):
        ...     print msg
        my message
        another message
        
        :param kwargs: any arguments that :meth:`~hotqueue.HotQueue.get` can
            accept (:attr:`block` will default to ``True`` if not given)
        """
        kwargs.setdefault('block', True)
        try:
            while True:
                msg = self.get(**kwargs)
                if msg is None:
                    break
                yield msg
        except KeyboardInterrupt:
            print; return
    
    def get(self, block=False, timeout=None):
        """Return a message from the queue. Example:
    
        >>> queue.get()
        'my message'
        >>> queue.get()
        'another message'
        
        :param block: whether or not to wait until a msg is available in
            the queue before returning; ``False`` by default
        :param timeout: when using :attr:`block`, if no msg is available
            for :attr:`timeout` in seconds, give up and return ``None``
        """
        if block:
            if timeout is None:
                timeout = 0
            msg = self.__redis.blpop(self.key, timeout=timeout)
            if msg is not None:
                msg = msg[1]
        else:
            msg = self.__redis.lpop(self.key)
        if msg is not None and self.serializer:
            msg = self.serializer.loads(msg)
        return msg
    
    def put(self, *msgs):
        """Put one or more messages onto the queue. Example:
    
        >>> queue.put("my message")
        >>> queue.put("another message")
        """
        for msg in msgs:
            if self.serializer:
                msg = self.serializer.dumps(msg)
            if self._bulk_mode:
                self._bulk_items.append(msg)
                if len(self._bulk_items) >= self._bulk_size:
                    self._flush_bulk()
            else:
                self.__redis.rpush(self.key, msg)

    @contextmanager
    def bulk(self, bulk_size=None):
        """Context manager for adding messages to the queue in bulk. Example:

        >>> with queue.bulk(1000):
                for counter in range(10000):
                    queue.put('message %s' % counter)

        :param bulk_size: the number of items to accumulate in memory before
            flushing to Redis. 500 by default.
        """
        self._bulk_mode = True
        self._bulk_size = bulk_size or DEFAULT_BULK_SIZE
        yield
        self._bulk_mode = False
        self._flush_bulk()

    def _flush_bulk(self):
        """Flush the bulk queue, sending all the current messages
        to Redis.
        """
        if self._bulk_items:
            self.__redis.rpush(self.key, *self._bulk_items)
            self._bulk_items = []

    def worker(self, *args, **kwargs):
        """Decorator for using a function as a queue worker. Example:
    
        >>> @queue.worker(timeout=1)
        ... def printer(msg):
        ...     print msg
        >>> printer()
        my message
        another message
        
        You can also use it without passing any keyword arguments:
        
        >>> @queue.worker
        ... def printer(msg):
        ...     print msg
        >>> printer()
        my message
        another message
        
        :param kwargs: any arguments that :meth:`~hotqueue.HotQueue.get` can
            accept (:attr:`block` will default to ``True`` if not given)
        """
        def decorator(worker):
            @wraps(worker)
            def wrapper(*args):
                for msg in self.consume(**kwargs):
                    worker(*args + (msg,))
            return wrapper
        if args:
            return decorator(*args)
        return decorator

