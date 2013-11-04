#!/usr/bin/env python
import Queue
import threading
import time
import random


class Worker(threading.Thread):

    INPUT_TIMEOUT = 1  # In seconds.

    def __init__(self, inputq, outputq):
        threading.Thread.__init__(self)
        self._inputq = inputq
        self._outputq = outputq
        self._shutdown = False

    def run(self):
        while not self._shutdown:
            self._is_working = False
            try:
                key, url = self._inputq.get(timeout=self.INPUT_TIMEOUT)
                result = self._process(url)
                self._outputq.put_nowait((key, result))
                # Mark the task as done.
                self._inputq.task_done()
            except Queue.Empty:
                pass

    def _process(self, url):
        # simulate some processing...
        time.sleep(random.uniform(2,5))
        # Think about reusing HTTP connection if possible.
        return "%s --- %.3f" % (self.name, random.random())

    def shutdown(self):
        self._shutdown = True


class Crawler(object):

    DEFAULT_TIMEOUT = 1  # In seconds.

    def __init__(self, nb_workers=2, rate=None,
            inputq=Queue.Queue(), outputq=Queue.Queue()):
        self._nb_workers = nb_workers
        self._rate = rate
        self._inputq = inputq
        self._outputq = outputq
        self._workers = list()

    def add_job(self, key, url):
        self._inputq.put_nowait((key, url))

    def get_result(self, block=True, timeout=self.DEFAULT_TIMEOUT):
        return self._outputq.get(block, timeout)

    def start(self):
        for i in xrange(self._nb_workers):
            worker = Worker(self._inputq, self._outputq)
            self._workers.append(worker)
            worker.start()
    
    def stop(self):
        # Send kill signal to every worker.
        for worker in self._workers:
            worker.shutdown()
        # Wait for all workers to finish.
        for i, worker in enumerate(self._workers):
            print "Attempting to finish worker %d..." % i
            worker.join()
            print "done."

    def is_working(self):
        # I'm not sure that this is part of the official Queue.Queue API.
        return self._inputq.unfinished_tasks > 0


if __name__ == '__main__':
    crawler = Crawler(nb_workers=10)
    for i in xrange(20):
        crawler.add_job(i, i)
    crawler.start()
    try:
        while crawler.is_working():
            try:
                res = crawler.get_result(timeout=1)
            except Queue.Empty:
                continue
            print res
        crawler.stop()
    except KeyboardInterrupt as interrupt:
        crawler.stop()
    finally:
        print "Program is over!! Yay!"
