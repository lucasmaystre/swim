# -*- coding: utf-8 -*-
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

Here's a small snippet that illustrates the API.

    import re
    import swim

    def process(body):
        for match in re.finditer(r'<a.*?href="(?P<url>.*?)">', body):
            yield match.group('url')

    config = {
        'folder': "./crawl",
        'processor': process,
        'max_workers': 4,
        'rate': 2.0,
    }
    manager = swim.Manager(**config)
    manager.run(seeds=["http://lucas.maystre.ch/"])
"""


import codecs
import contextlib
import cPickle
import datetime
import futures
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
UA_STRING = 'swim/1.0'


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


class Fetcher(threading.Thread):

    """Concurrent URL fetcher.

    The fetcher takes care of performing the HTTP URLs. It enforces a rate
    limit, but if requests takes time it can also fetch several URLs in
    parallel. It can be stopped / resumed thanks to functions that pickle /
    unpickle its state.
    """

    TIMEOUT = 1  # In seconds.

    def __init__(self, max_workers=2, rate=100, ua_string=UA_STRING):
        """Initialize a fetcher."""
        super(Fetcher, self).__init__()
        self._max_workers = max_workers
        self._rate = rate
        self._ua_string = ua_string
        self._in = Queue.Queue()
        self._out = Queue.Queue()
        self._shutdown = False
        self._session = requests.Session()
        self._session.headers.update({'User-Agent': self._ua_string})
        if max_workers > requests.adapters.DEFAULT_POOLSIZE:
            # This is need as self._session might be called concurrently. See:
            # https://github.com/ross/requests-futures.
            kwargs = {'pool_connections': max_workers,
                      'pool_maxsize': max_workers}
            self._session._mount('https://',
                                 requests.adapters.HTTPAdapter(**kwargs))
            self._session._mount('http://',
                                 requests.adapters.HTTPAdapter(**kwargs))
        LOGGER.debug('fetcher initialized')

    @classmethod
    def from_pickle(cls, path):
        """Recreate a fetcher from a pickle."""
        with open(path, 'rb') as f:
            data = cPickle.load(f)
            instance = cls(max_workers=data['max_workers'], rate=data['rate'],
                           ua_string=data['ua_string'])
            for key, url in data['pending']:
                instance.add_url(key, url)
            return instance

    def add_url(self, key, url):
        """Add a URL to fetch.

        The key paramater allows to maps URLs to responses when fetching
        elements from the output queue.
        """
        self._in.put_nowait((key, url))

    def get_result(self, block=True, timeout=None):
        """Fetch a result from the output queue.

        Returns a tuple (key, response) to facilitate mapping with the
        corresponding URL / request.
        """
        return self._out.get(block, timeout)

    def stop(self):
        """Gracefully shut down the fetcher.

        Blocks until all pending requests are done. This method can be called
        only once the fetcher has started.
        """
        assert self.is_alive(), "fetcher is not running."
        self._shutdown = True
        LOGGER.debug("Stopping the fetcher...")
        self.join()

    def save_pickle(self, path):
        """Save remaining URLs and fetcher parameters as a pickle."""
        assert not self.is_alive(), "cannot save a running fetcher"
        self._in.put('♥')  # Small hack to be able to iterate over a queue.
        data = {
            'max_workers': self._max_workers,
            'rate': self._rate,
            'pending': list(iter(self._in.get_nowait, '♥')),
            'ua_string': self._ua_string,
        }
        with open(path, 'wb') as f:
            cPickle.dump(data, f)
        LOGGER.debug("fetcher setup saved as a pickle")

    def is_working(self):
        """Check whether the fetcher is still actively fetching."""
        # I'm not sure that this is part of the official Queue.Queue API.
        return self._in.unfinished_tasks > 0

    def run(self):
        LOGGER.debug("starting the fetcher with max {} workers"
                     .format(self._max_workers))
        kwargs = {'max_workers': self._max_workers}
        with futures.ThreadPoolExecutor(**kwargs) as executor:
            pending = list()
            while not self._shutdown:
                # Update pending jobs.
                pending = filter(lambda f: not f.done(), pending)
                if len(pending) < self._max_workers:
                    try:
                        key, url = self._in.get(timeout=self.TIMEOUT)
                        LOGGER.debug("processing job {} ({})".format(key, url))
                        future = executor.submit(self._fetch, key, url)
                        pending.append(future)
                    except Queue.Empty:
                        pass
                    if self._rate is not None:
                        time.sleep(1.0 / self._rate)
                else:
                    futures.wait(pending, return_when=futures.FIRST_COMPLETED)

    def _fetch(self, key, url):
        """Fetch a URL and parse the response.

        Also, notify the input queue that the task is done, and put the parsed
        reponse in the output queue.
        """
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
            self._out.put_nowait((key, data))
            self._in.task_done()
            LOGGER.debug("done with job {}".format(key))


class Manager(object):

    """Manage a fetcher.

    The fetcher simply takes URLs and returns HTTP responses. This class does
    the rest: keep track of requests in the database, write responses to disk,
    send new URLs to the fetcher based on the responses, and listen to keyboard
    interrupts.

    In principle, this should be the only class of the module that gets called
    in user code. The rest is just here to support this one.
    """

    def __init__(self, folder="./swim", processor=None, fetcher_pickle=None,
                 max_workers=2, rate=1.0, ua_string=UA_STRING):
        """Initialize a crawl manager.

        Keyword arguments:
        folder -- the folder where the output will be stored (default './swim')
        processor -- a function that takes a string containing the response
                body and returns an iterable over URLs (default None)
        fetcher_pickle -- a pickle used to resume a previous crawl, overrides
                the next two arguments (default None)
        max_workers -- the maximum number of worker threads to spawn (default 2)
        rate -- the rate at which to crawl, in URLs per second (default 1.0)
        """
        _ensure_folder(folder)
        self._store = self._get_store(os.path.join(folder, "crawl.db"))
        self._data_dir = os.path.join(folder, "data")
        self._pickle_path = os.path.join(folder, "fetcher.pickle")
        _ensure_folder(self._data_dir)
        self._process = processor
        if fetcher_pickle is None:
            self._fetcher = Fetcher(max_workers=max_workers, rate=rate,
                                    ua_string=ua_string)
        else:
            self._fetcher = Fetcher.from_pickle(fetcher_pickle)
        LOGGER.info("manager initialized")

    def _get_store(self, db_path):
        """Return a storm Store from a path."""
        # Initialize store. Context automatically commits and closes.
        with contextlib.closing(sqlite3.connect(db_path)) as conn:
            conn.executescript(SQL_SCHEMA)
        return Store(create_database("sqlite:{}".format(db_path)))

    @contextlib.contextmanager
    def _manage(self, fetcher, save=None):
        """Manage interruptions and exceptions.

        This is a context manager that wraps around the crawl manager's
        operations and takes care of handling exceptions and trying to
        gracefully shut down (and save) the fetcher.
        """
        fetcher.start()
        should_save = False
        try:
            yield fetcher
        except KeyboardInterrupt:
            LOGGER.info("caught an interruption")
            should_save = True
        except Exception as e:
            # Another exception happened, we should save in case.
            LOGGER.error("crawl manager caught an exception")
            should_save = True
            raise e
        finally:
            fetcher.stop()
            try:
                # Process everything in the output queue.
                while True:
                    key, res = fetcher.get_result(block=False)
                    self._handle_result(key, res)
            except Queue.Empty:
                pass
            if should_save and save is not None:
                fetcher.save_pickle(save)

    def run(self, seeds=()):
        """Start crawling.

        If we are not resuming a previous crawl, we will typically need to
        provide a few seeds to bootstrap the fetcher.
        """
        for url in seeds:
            self._add_resource(url, None)
        with self._manage(self._fetcher, self._pickle_path) as fetcher:
            while fetcher.is_working():
                # This is the main loop: wait for results and process them.
                try:
                    key, res = fetcher.get_result(timeout=1)
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
        fetcher's URL queue if needed.
        """
        resource = self._store.find(Resource, Resource.url == url).one()
        if resource is None:
            # We've never seen this URL---we need to crawl it.
            resource = Resource(url)
            self._store.add(resource)
            self._store.flush()
            LOGGER.debug("resource {} not fetched yet, creating a new job"
                         .format(resource.id))
            self._fetcher.add_url(resource.id, url)
        if parent_id is not None:
            # Add an edge.
            LOGGER.debug("adding edge between {} and {}"
                         .format(parent_id, resource.id))
            self._store.add(Edge(parent_id, resource.id))
        self._store.commit()

    def _handle_result(self, key, result):
        """Handle a result (HTTP response) from the fetcher.

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
            with codecs.open(path, 'w', encoding='utf8') as f:
                f.write(result['body'])
            # Extract more jobs.
            if self._process is not None:
                for url in self._process(result['body']):
                    # urljoin handles all quirky cases of URL resolution.
                    canonical = urlparse.urljoin(resource.final_url, url)
                    # Get rid of any fragment (we have to do it ourselves...)
                    canonical = canonical.split('#', 1)[0]
                    self._add_resource(canonical, resource.id)
