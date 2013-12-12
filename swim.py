"""swim - a simple, no-frills web crawler.

In a nutshell: you seed it with a bunch of URLs, you give it a function that
extracts more URLs from the HTTP responses, and swim does the rest. Noticeable
features are:

- multithreaded, possibility to enable rate limiting
- kill the crawler and resume it later without losing data
- all crawling-related information is persisted in a database

swim is minimalistic by design. There are plenty of powerful crawlers out
there; the goal of this one is to provide a simple, no-frills basis that is
easy to adapt to your needs.

Here's a small snipper that illustrates the API.

    import re

    def process(body):
        for match in re.finditer(r'<a.*?href="(?P<url>.*?)">', body):
            yield match.group('url')

    config = {
        'folder': "./crawl",
        'processor': process,
        'nb_workers': 4,
        'rate': 2.0,
    }
    manager = swim.CrawlManager(**config)
    manager.run(seeds=["http://lucas.maystre.ch/"])
"""
# Assumptions & design decisions:
#
# - decoupling fetching HTML from processing the content
# - new pages to crawl can be determined solely by processing text output
# - page = URL + GET params.
# - GET requests only - no POST, etc.
# - Output is response body only
# - no cookies handling, form submissions, etc.
#
# Recovery: works only if it is able to gracefully shut down. The shutdown
# procedure is as follows:
#
# 1. set the shutdown flag for all worker threads.
# 2. Wait until all threads are done. This means that they finished the pending
#    request or that they timed out on getting a new job from the input queue.
# 3. Process everything in the output queue, possibly adding new URls to the
#    input queue.
# 4. Pickle the crawler state, commit and close the DB connection.
#
# A wish list of things TODO:
#
# - recover from crashes as well as graceful shutdowns.
# - finish crawling the URLs in the input queue without creating new jobs. To
#   make sure we could still expand the crawl, we could add a flag in the table
#   which indicates whether the resource has been processed (i.e. new jobs
#   extracted) or not.
# - clean up the queues: the output queue should not be pickled anymore (it is
#   empty at the end), so we could just use a regular Queue.Queue for that. As
#   for the input queue, it might be simpler and more efficient to subclass /
#   wrap `collections.deque` which also has atomic ops.
# TODO: https://github.com/patrys/httmock


import contextlib
import cPickle
import datetime
import logging
import os.path
import Queue
import requests
import sqlite3
import threading
import time
import urlparse

from storm.locals import (Bool, DateTime, Float, Int, ReferenceSet,
                          Store, Storm, Unicode, create_database)


SQL_SCHEMA = """
CREATE TABLE IF NOT EXISTS resource (
  id INTEGER PRIMARY KEY,
  url TEXT,
  final_url TEXT,
  method TEXT,
  created TEXT DEFAULT CURRENT_TIMESTAMP,  -- UTC time.
  updated TEXT DEFAULT CURRENT_TIMESTAMP,  -- UTC time.
  duration REAL,
  status_code INTEGER,
  successful BOOLEAN
);
CREATE INDEX IF NOT EXISTS resource_url_idx ON resource(url);

CREATE TABLE IF NOT EXISTS edge (
  id INTEGER PRIMARY KEY,
  src INTEGER REFERENCES resource,
  dst INTEGER REFERENCES resource,
  created TEXT DEFAULT CURRENT_TIMESTAMP,  -- UTC time.
  new_dst BOOLEAN,
  UNIQUE (src, dst) ON CONFLICT IGNORE
);
"""


def _setup_logger():
    """Set up and return the logger for the module."""
    template = "%(asctime)s %(name)s:%(levelname)s - %(message)s"
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()  # Logs to stderr.
    handler.setFormatter(logging.Formatter(fmt=template))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


LOGGER = _setup_logger()


def _ensure_folder(folder):
    """Make sure a given folder exists.

    The given path can actually consist of a hierarchy of multiple unexisting
    folders which will all be created.
    """
    if os.path.exists(folder):
        if not os.path.isdir(folder):
            raise ValueError("path exists but is not a folder")
    else:
        os.makedirs(folder)


def set_loglevel(loglevel):
    """Set the level for the module-wide logger.

    loglevel is a a string such as "debug", "info" or "warning".
    """
    if isinstance(loglevel, basestring):
        # Getting the relevant constant, e.g. `logging.DEBUG`.
        loglevel = getattr(logging, loglevel.upper())
    LOGGER.setLevel(loglevel)


class Resource(Storm):

    """ORM class for the resource table."""

    __storm_table__ = 'resource'
    id = Int(primary=True)
    url = Unicode()
    final_url = Unicode()
    method = Unicode()
    created = DateTime()
    updated = DateTime()
    duration = Float()
    status_code = Int()
    is_successful = Bool(name='successful')
    # References
    parents = ReferenceSet('Resource.id', 'Edge.dst',
                           'Edge.src', 'Resource.id')
    children = ReferenceSet('Resource.id', 'Edge.src',
                            'Edge.dst', 'Resource.id')

    def __init__(self, url):
        self.url = url


class Edge(Storm):

    """ORM class for the edge table."""

    __storm_table__ = 'edge'
    id = Int(primary=True)
    src = Int()
    dst = Int()
    created = DateTime()
    is_new_dst = Bool(name='new_dst')

    def __init__(self, src, dst):
        self.src = src
        self.dst = dst


class SwimQueue(Queue.Queue):

    """A Queue.Queue that's picklable and rate-limited.

    Queue.Queue is not picklable (for good reasons). This class fixes that for
    swim's specific use. Rate limiting is also implemented directly here; the
    rate limit is respected over a one minute window.
    """
    # TODO So far this class is pretty simple. In the future, it might become
    # better to wrap Queue.Queue (decorator pattern) instead of subclassing it.

    def __init__(self, rate_limit=None):
        """Initialize the queue.

        If rate_limit is a positive number, it represents the rate (items per
        second) at which items can be retrieved.
        """
        Queue.Queue.__init__(self, maxsize=0)
        self.rate_limit = rate_limit
        self._tstamps = list()
        self._lock = threading.Lock()

    def __getstate__(self):
        """Return a representation of the instance as a list."""
        items = [self.rate_limit]
        while not self.empty():
            items.append(self._get())
        return items

    def __setstate__(self, state):
        """Restore the instance from a list representation."""
        # First element is the rate limit, the rest are the items.
        self.__init__(rate_limit=state.pop(0))
        for item in state:
            self._put(item)
            self.unfinished_tasks += 1

    def get(self, timeout):
        """Get an element from the queue.

        An item is returned if and only if 1) the caller can acquire the lock,
        2) the rate limit is not exceeded and 3) there is an element in the
        queue. In any other case, the call blocks for timeout seconds and
        raises Queue.Empty.
        """
        assert timeout >= 0, "timeout must be non-negative"
        if self.rate_limit is None:
            return Queue.Queue.get(self, timeout=timeout)
        if self._lock.acquire(False):
            try:
                if not self._rate_limit_exceeded():
                    item = Queue.Queue.get(self, timeout=timeout)
                    self._tstamps.append(time.time())
                    return item
            finally:
                self._lock.release()
        time.sleep(timeout)
        raise Queue.Empty()

    def _rate_limit_exceeded(self):
        """Compute the time remaining until the next element can be fetched."""
        assert self.rate_limit > 0, "rate limit must be positive"
        minute_ago = time.time() - 60
        # Remove outdated timestamps.
        while self._tstamps and self._tstamps[0] < minute_ago:
            self._tstamps.pop(0)
        return len(self._tstamps) / 60.00 >= self.rate_limit


class Worker(threading.Thread):

    """Worker thread.

    A worker fetches jobs from the input queue, performs the HTTP request, and
    puts the result in the output queue.
    """

    INPUT_TIMEOUT = 1  # In seconds.

    def __init__(self, inputq, outputq):
        """Initialize the thread with two queues."""
        threading.Thread.__init__(self)
        self._inputq = inputq
        self._outputq = outputq
        self._shutdown = False
        self._session = requests.Session()
        LOGGER.debug('initializing worker thread')

    def run(self):
        """Fetch jobs from the input queue ad infinitum."""
        while not self._shutdown:
            self._is_working = False
            try:
                key, url = self._inputq.get(timeout=self.INPUT_TIMEOUT)
                LOGGER.debug("{}: got job {} ({})".format(self.name, key, url))
                result = self._process(url)
                self._outputq.put_nowait((key, result))
                # Mark the task as done.
                self._inputq.task_done()
                LOGGER.debug("{}: done with job {}".format(self.name, key))
            except Queue.Empty:
                pass

    def _process(self, url):
        """Process a single url: fetch it and parse the results."""
        data = {'method': u"GET"}
        try:
            LOGGER.info("fetching '{}'...".format(url))
            response = self._session.get(url)
            data['success'] = True
            data['duration'] = response.elapsed.total_seconds()
            data['status_code'] = response.status_code
            data['body'] = response.text
            data['final_url'] = response.url
        except Exception as e:
            LOGGER.warning("exception while processing URL {}: {}"
                           .format(url, repr(e)))
            data['success'] = False
        finally:
            data['updated'] = datetime.datetime.utcnow()
            return data

    def shutdown(self):
        """Set the shutdown flag.

        The thread will exit only once it has finished processing any pending
        job.
        """
        self._shutdown = True


class Crawler(object):

    """Multi-threaded crawler.

    The crawler handles and coordinates several worker threads, and contains
    functions to pickle / unpickle the state. It also abstracts away the input
    and output queues.
    """

    def __init__(self, nb_workers=2, rate=None, inputq=None):
        """Initialize a crawler."""
        self._nb_workers = nb_workers
        self._rate = rate
        self._inputq = inputq if inputq is not None else SwimQueue(rate)
        self._outputq = Queue.Queue()
        self._workers = list()
        self._started = False
        LOGGER.debug('initializing crawler')

    @classmethod
    def from_pickle(cls, path):
        """Recreate a crawler from a pickle."""
        with open(path, 'rb') as f:
            data = cPickle.load(f)
            return cls(nb_workers=data['nb_workers'], rate=data['rate'],
                       inputq=data['inputq'])

    def add_job(self, key, url):
        """Add a URL to crawl.

        The key paramater allows to maps URLs to responses when fetching
        elements from the output queue.
        """
        self._inputq.put_nowait((key, url))

    def get_result(self, block=True, timeout=None):
        """Fetch a result from the output queue.

        Returns a tuple (key, response) to facilitate mapping with the
        corresponding URL / request."""
        return self._outputq.get(block, timeout)

    def start(self):
        """Launch the worker threads and start crawling."""
        assert not self._started, "crawler already started"
        LOGGER.debug("starting the crawler with {} workers"
                     .format(self._nb_workers))
        self._started = True
        for i in xrange(self._nb_workers):
            worker = Worker(self._inputq, self._outputq)
            self._workers.append(worker)
            worker.start()

    def stop(self):
        """Shut the crawler down.

        This might take some time, as we wait until every worker thread has
        finished what it was currently doing (typically performing an HTTP
        request.) Think of this as a graceful shutdown.
        """
        assert self._started, "crawler not started"
        # Send kill signal to every worker.
        for worker in self._workers:
            worker.shutdown()
        # Wait for all workers to finish.
        for worker in self._workers:
            LOGGER.debug("stopping worker {}...".format(worker.name))
            worker.join()
        LOGGER.info("all workers stopped")
        self._started = False

    def save_pickle(self, path):
        """Save the remaining jobs of the crawler as a pickle."""
        assert not self._started, "cannot save a running crawler"
        data = {
            'nb_workers': self._nb_workers,
            'rate': self._rate,
            'inputq': self._inputq,
        }
        with open(path, 'wb') as f:
            cPickle.dump(data, f)
        LOGGER.debug("crawler setup saved as a pickle")

    def is_working(self):
        """Check whether the crawler is still actively crawling."""
        nb_alive = len(filter(lambda w: w.is_alive(), self._workers))
        # I'm not sure that this is part of the official Queue.Queue API.
        return nb_alive > 0 and self._inputq.unfinished_tasks > 0


class CrawlManager(object):

    """Manage the crawler.

    The crawler simply takes URLs and returns HTTP responses. This class does
    the rest: keep track of requests in the database, write responses to disk,
    send new URLs to the crawler based on the responses, and listen to keyboard
    interrupts.

    In principle, this should be the only class of the module that gets called
    in user code. The rest is just here to support this one.
    """

    def __init__(self, folder="./swim", processor=None, crawler_pickle=None,
                 nb_workers=2, rate=1.0):
        """Initialize a crawl manager.

        Keyword arguments:
        folder -- the folder where the output will be stored (default './swim')
        processor -- a function that takes a string containing the response
                body and returns an iterable over URLs (default None)
        crawler_pickle -- a pickle used to resume a previous crawl, overrides
                the next two arguments (default None)
        nb_workers -- the number of worker threads to spawn (default 2)
        rate -- the rate (averaged over one minute) at which to crawl, in URLs
                per second (default 1.0)
        """
        _ensure_folder(folder)
        self._store = self._get_store(os.path.join(folder, "crawl.db"))
        self._data_dir = os.path.join(folder, "data")
        self._pickle_path = os.path.join(folder, "crawler.pickle")
        _ensure_folder(self._data_dir)
        self._process = processor
        if crawler_pickle is None:
            self._crawler = Crawler(nb_workers=nb_workers, rate=rate)
        else:
            self._crawler = Crawler.from_pickle(crawler_pickle)
        LOGGER.info("crawl manager initialized")

    def _get_store(self, db_path):
        """Return a storm Store from a path."""
        # Initialize store. Context automatically commits and closes.
        with contextlib.closing(sqlite3.connect(db_path)) as conn:
            conn.executescript(SQL_SCHEMA)
        return Store(create_database("sqlite:{}".format(db_path)))

    @contextlib.contextmanager
    def _manage(self, crawler, save=None):
        """Manage interruptions and exceptions.

        This is a context manager that wraps around the crawl manager's
        operations and takes care of handling exceptions and trying to
        gracefully shut down (and save) the crawler.
        """
        crawler.start()
        should_save = False
        try:
            yield crawler
        except KeyboardInterrupt:
            LOGGER.info("caught an interruption")
            should_save = True
        except Exception as e:
            # Another exception happened, we should save in case.
            LOGGER.error("crawl manager caught an exception")
            should_save = True
            raise e
        finally:
            crawler.stop()
            try:
                # Process everything in the output queue.
                while True:
                    key, res = crawler.get_result(block=False)
                    self._handle_result(key, res)
            except Queue.Empty:
                pass
            if should_save and save is not None:
                crawler.save_pickle(save)

    def run(self, seeds=()):
        """Start crawling.

        If we are not resuming a previous crawl, we will typically need to
        provide a few seeds to bootstrap the crawler.
        """
        for url in seeds:
            self._add_resource(url, None)
        with self._manage(self._crawler, self._pickle_path) as crawler:
            while crawler.is_working():
                # This is the main loop: wait for results and process them.
                try:
                    key, res = crawler.get_result(timeout=1)
                except Queue.Empty:
                    pass
                else:
                    self._handle_result(key, res)
        self._store.commit()
        self._store.close()
        LOGGER.info("crawl manager has finished")

    def _add_resource(self, url, parent_id):
        """Handle a URL output by the processor.

        Finds out whether the URL is already in the database and inserts it if
        necessary, creates an edge with its parent resource, and adds it to the
        crawler job queue if needed.
        """
        resource = self._store.find(Resource, Resource.url == url).one()
        if resource is None:
            # We've never seen this URL---we need to crawl it.
            resource = Resource(url)
            self._store.add(resource)
            self._store.flush()
            LOGGER.debug("resource {} not fetched yet, creating a new job"
                         .format(resource.id))
            self._crawler.add_job(resource.id, url)
        if parent_id is not None:
            # Add an edge.
            LOGGER.debug("adding edge between {} and {}"
                         .format(parent_id, resource.id))
            self._store.add(Edge(parent_id, resource.id))
        self._store.commit()

    def _handle_result(self, key, result):
        """Handle a result (HTTP response) from the crawler.

        Includes updating the corresponding entry in the database, writing the
        response to disk, and extracting further URLs to crawl.
        """
        LOGGER.debug("handling result for key {}".format(key))
        resource = self._store.get(Resource, key)
        # Update the resource with the new information.
        resource.final_url = result.get('final_url')
        resource.method = result.get('method')
        resource.updated = result.get('updated')
        resource.duration = result.get('duration')
        resource.status_code = result.get('status_code')
        resource.is_successful = result.get('success')
        self._store.commit()
        # Process the body of the response, if there is one.
        if 'body' in result and result['body'] is not None:
            LOGGER.debug("writing response body to disk for key {}"
                         .format(key))
            # Save the file to disk.
            path = os.path.join(self._data_dir, "{}.html".format(resource.id))
            with open(path, 'w') as f:
                f.write(result['body'])
            # Extract more jobs.
            if self._process is not None:
                for url in self._process(result['body']):
                    # urljoin handles all quirky cases of URL resolution.
                    canonical = urlparse.urljoin(resource.final_url, url)
                    self._add_resource(canonical, resource.id)
