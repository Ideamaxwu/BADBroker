import heapq
import logging as log
import time
import brokerutils

log = brokerutils.setup_logging(__name__)


class BADEmptyCache:
    def get(self, key):
        return None

    def put(self, key, object):
        return False
