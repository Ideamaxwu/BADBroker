import heapq
import logging as log
import time
import brokerutils

log = brokerutils.setup_logging(__name__)


class BADLRUCache:
    def __init__(self, size=100):
        self.size = size
        self.cache = {}

    def hasKey(self, key):
        return key in self.cache

    def get(self, key):
        if key in self.cache:
            timestamp = time.time()
            self.cache[key]['timestamp'] = timestamp
            return self.cache[key]['object']
        else:
            log.error('Object not found with key %s' % key)
            return None

    def put(self, key, object):
        if len(self.cache) >= self.size:
            # delete the oldest one
            maxKey = None
            maxDuration = None
            timestamp = time.time()
            for key in self.cache:
                duration = timestamp - self.cache[key]['timestamp']
                if maxDuration is None or maxDuration < duration:
                    maxKey = key
                    maxDuration = duration

            if maxKey:
                log.info('Evicting %s from cache, times since accessed %s sec' % (maxKey, maxDuration))
                del self.cache[maxKey]
            else:
                log.error('Something is wrong key %s duration %s' % (maxKey, maxDuration))
                return False

        # Add the object
        timestamp = time.time()
        self.cache[key] = {'timestamp': timestamp, 'object': object}
        return True

