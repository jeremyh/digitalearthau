import multiprocessing
from logging import LogRecord
from logging.handlers import QueueHandler, QueueListener
import time
import structlog

import digitalearthau.uiutil

import logging

# If we can't log after five minutes, the collector must have died. It will throw an exception to the log caller.
MAX_LOG_WAIT = 60 * 5

_LOG = logging.getLogger(__name__)


class BlockingQueueHandler(QueueHandler):
    def enqueue(self, record: LogRecord):
        """
        The default enqueue raises an exception immediately if the queue is full.

        This overrides the behaviour so that it's willing to wait a little.
        """
        self.queue: multiprocessing.Queue

        # The default is to raise an exception when the _LOG_QUEUE is full, but the logging itself doesn't handle
        # this case. We instead use a blocking-put operation with a timeout.
        self.queue.put(record, timeout=MAX_LOG_WAIT)


class SafeQueueListener(QueueListener):
    def enqueue_sentinel(self):
        """
        Override the default enqueue_sentinel() so that it's willing to wait a little if the Queue is currently full.

        This minimises spurious Full exceptions when trying to shutdown the listener. They'll only be thrown
        if it takes too long.
        """
        self.queue.put(self._sentinel, timeout=MAX_LOG_WAIT)


def do_stuff(x):
    for i in range(100):
        _LOG.info("Printing from %s: %s", x, str(x) * 30)

    _LOG.info("Printing from %s: %s", x, str(x) * 30)

    # TODO: this is not being tunneled presumably?
    sl = structlog.getLogger(__name__)
    sl.info("direct.print", number=x)
    return x


def _redirect_logging(queue):
    root_log = logging.getLogger()
    for handler in root_log.handlers:
        root_log.removeHandler(handler)
    root_log.addHandler(BlockingQueueHandler(queue))


def main():
    # multiprocessing.set_start_method('spawn')

    mlog: logging.Logger = multiprocessing.get_logger()
    mlog.setLevel(logging.INFO)
    mlog.propagate = True

    digitalearthau.uiutil.init_log_storage()

    _LOG.info("Test legacy log")

    sl = structlog.getLogger(__name__)

    sl.info("did.something", ernest="facile")

    q = multiprocessing.Queue(maxsize=1)

    ql = SafeQueueListener(q, digitalearthau.uiutil.StructLogHandler())
    ql.start()

    print("Multiprocessing is using %s" % multiprocessing.get_start_method())
    print("Handlers: %s" % len(logging.getLogger().handlers))

    # Spawn off multiprocessing instances to log.

    with multiprocessing.Pool(50, initializer=_redirect_logging, initargs=(q,)) as pool:
        print(q.qsize())
        count = 0
        for f in pool.imap_unordered(do_stuff, range(200)):
            count += f
        print(q.qsize())

        # We must join them all to ensure log queue is processed fully.
        pool.close()
        # pool.join()
        ql.stop()

    print("All done: " + str(count))


if __name__ == '__main__':
    main()
