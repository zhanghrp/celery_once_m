# -*- coding: utf-8 -*-

"""Definition of the redis locking backend."""

from __future__ import absolute_import

import json
import logging
from time import time
import sys
sys.path.append("..")

logger = logging.getLogger('django.request')


try:
    from urlparse import urlparse, parse_qsl
except:
    # Python 3!
    from urllib.parse import urlparse, parse_qsl


def parse_url(url):
    """
    Parse the argument url and return a redis connection.
    Two patterns of url are supported:
        * redis://host:port[/db][?options]
        * redis+socket:///path/to/redis.sock[?options]
    A ValueError is raised if the URL is not recognized.
    """
    parsed = urlparse(url)
    kwargs = parse_qsl(parsed.query)

    # TCP redis connection
    if parsed.scheme == 'redis':
        details = {'host': parsed.hostname}
        if parsed.port:
            details['port'] = parsed.port
        if parsed.password:
            details['password'] = parsed.password
        db = parsed.path.lstrip('/')
        if db and db.isdigit():
            details['db'] = db

    # Unix socket redis connection
    elif parsed.scheme == 'redis+socket':
        details = {'unix_socket_path': parsed.path}
    else:
        raise ValueError('Unsupported protocol %s' % (parsed.scheme))

    # Add kwargs to the details and convert them to the appropriate type, if needed
    details.update(kwargs)
    if 'socket_timeout' in details:
        details['socket_timeout'] = float(details['socket_timeout'])
    if 'db' in details:
        details['db'] = int(details['db'])

    return details


redis = None


def get_redis(settings):
    global redis
    if not redis:
        try:
            from redis import StrictRedis
        except ImportError:
            raise ImportError(
                "You need to install the redis library in order to use Redis"
                " backend (pip install redis)")
        redis = StrictRedis(**parse_url(settings['url']))
    return redis


class Redis(object):
    """Redis backend."""

    def __init__(self, settings):
        self._redis = get_redis(settings)

    @property
    def redis(self):
        # Used to allow easy mocking when testing.
        return self._redis

    def raise_or_lock(self, key, timeout):
        """
        Checks if the task is locked and raises an exception, else locks
        the task.
        """
        from ..helpers import now_unix
        now = now_unix()
        # Check if the tasks is already queued if key is in cache.
        result = self.redis.get(key)
        logger.info("key %s" % key)
        if result:
            # Work out how many seconds remaining till the task timeout.
            remaining = int(result) - now
            logger.info("int(result) %d; now %d" % (int(result), now))
            if remaining > 0:
                from ..tasks import AlreadyQueued
                raise AlreadyQueued(remaining)

        # By default, the tasks and the key expire after 60 minutes.
        # (meaning it will not be executed and the lock will clear).
        self.redis.setex(key, timeout, now + timeout)

    def clear_lock(self, key):
        """Remove the lock from redis."""
        return self.redis.delete(key)
