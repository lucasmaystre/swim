# In the folder, there's:
# - the sqlite DB called `crawl.db`
# - a folder called `data` containing called `[id].res`

# Assumptions & design decisions:
# - decoupling fetching HTML from processing the content
# - new pages to crawl can be determined solely by processing text output
# - page = URL + GET params.
# - GET requests only - no POST, etc.
# - Output is response body only
# - no cookies handling, form submissions, etc.

import os.path
import Queue
import threading
import urlparse
import contextlib
import requests
import logging

from storm.locals import *


SQL_SCHEMA = """
CREATE TABLE IF NOT EXISTS resource (
  id INTEGER PRIMARY KEY,
  url TEXT,
  final_url TEXT,
  method TEXT,
  created TEXT DEFAULT CURRENT_TIMESTAMP,
  updated TEXT DEFAULT CURRENT_TIMESTAMP,
  duration REAL,
  status_code INTEGER,
  successful BOOLEAN
);
CREATE INDEX IF NOT EXISTS resource_url_idx ON resource(url);

CREATE TABLE IF NOT EXISTS edge (
  id INTEGER PRIMARY KEY,
  src INTEGER REFERENCES resource,
  dst INTEGER REFERENCES resource,
  created TEXT DEFAULT CURRENT_TIMESTAMP,
  new_dst BOOLEAN,
  UNIQUE (src, dst) ON CONFLICT IGNORE
);
"""

LOGGER = _setup_logger()
LOG_FORMAT = "%(asctime)s %(name)s:%(levelname)s - %(message)s"

def _setup_logger():
    logger = logging.getLogger(__name__)
    handler = logging.handlers.StreamHandler()  # Logs to stderr.
    handler.setFormatter(logging.Formatter(fmt=LOG_FORMAT))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def _ensure_folder(folder):
    if not os.path.exists(folder):
        os.makedirs(folder)


def set_loglevel(logevel):
    if isinstance(loglevel, basestring):
        # Getting the relevant constant, e.g. `logging.DEBUG`.
        loglevel = getattr(logging, loglevel.upper())
    LOOGGER.setLevel(loglevel)


class Resource(Storm):
    __storm_table__ = 'resource'
    id = Int(primary=True)
    url = Unicode()
    final_url = Unicode()
    method = Unicode()
    created = DateTime()
    updated = DateTime()
    duration = Int()
    status_code = Int()
    is_successful = Boolean(name='successful')
    # References
    parents = ReferenceSet('Resource.id', 'Edge.dst',
            'Edge.src', 'Resource.id')
    children = ReferenceSet('Resource.id', 'Edge.src',
            'Edge.dst', 'Resource.id')

    def __init__(self, url):
        self.url = url


class Edge(Storm):
    __storm_table__ = 'edge'
    id = Int(primary=True)
    src = Int()
    dst = Int()
    created = DateTime()
    is_new_dst = Boolean(name='new_dst')

    def __init(self, src, dst):
        self.src = src
        self.dst = dst


class Worker(threading.Thread):

    INPUT_TIMEOUT = 1  # In seconds.

    def __init__(self, inputq, outputq):
        threading.Thread.__init__(self)
        self._inputq = inputq
        self._outputq = outputq
        self._shutdown = False
        self._session = requests.Session()
        LOGGER.debug('initializing worker thread')

    def run(self):
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
        data = {'method': "GET"}
        try
            response = self._session.get(url)
            data['success'] = True
            data['duration'] = response.elapsed.total_seconds()
            data['status_code'] = response.status_code
            data['body'] = reponse.text
            data['final_url'] = response.url
        except Exception as e:
            LOGGER.warning("exception while processing URL {}: {}"
                    .format(url, repr(e)))
            data['success'] = False
        finally:
            data['updated'] = datetime.datetime.now()
            return data

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
        self._started = False
        LOGGER.debug('initializing crawler')

    @classmethod
    def from_pickle(cls, path):
        with open(path, 'rb') as f:
            data = cPickle.load(f)
            return cls(nb_workers=data['nb_workers'], rate=data['rate'],
                    inputq=data['inputq'], outputq=data['outputq'])

    def add_job(self, key, url):
        self._inputq.put_nowait((key, url))

    def get_result(self, block=True, timeout=self.DEFAULT_TIMEOUT):
        return self._outputq.get(block, timeout)

    def start(self):
        LOGGER.debug("starting the crawler with {} workers"
                .format(self._nb_workers))
        assert not self._started, "crawler already started"
        self._started = True
        for i in xrange(self._nb_workers):
            worker = Worker(self._inputq, self._outputq)
            self._workers.append(worker)
            worker.start()
    
    def stop(self):
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
        assert not self._started, "cannot save a running crawler"
        data = {
          'nb_workers': self._nb_workers,
          'rate': self._rate,
          'inputq': self._inputq,
          'outputq': self._outputq,
        }
        with open(path, 'wb') as f:
            cPickle.dump(data, f)
        LOGGER.debug("crawler setup saved as a pickle")

    def is_working(self):
        # I'm not sure that this is part of the official Queue.Queue API.
        return self._inputq.unfinished_tasks > 0


class CrawlManager(object):

    def __init__(self, folder, processor=None, crawler_pickle=None):
        _ensure_folder(folder)
        self._conn = self._connect(os.path.join(folder, "crawl.db"))
        self._data_dir = os.path.join(folder, "data")
        _ensure_folder(self._data_dir)
        self._process = processor
        if crawler_pickle is None:
            self._crawler = Crawler(nb_workers=4, rate=1.0)
        else:
            self._crawler = Crawler.from_pickle(self._crawler_pickle)
        LOGGER.debug("crawl manager initialized")

    def _get_store(self, db_path):
        # Initialize store. Context automatically
        with contextlib.closing(sqlite3.connect(db_path)) as conn:
            conn.executescript(SQL_SCHEMA)
        return Store(create_database("sqlite:{}".format(db_path)))

    @contextlib.contextmanager
    def _manage(self, crawler, save=None):
        crawler.start()
        should_save = False
        try:
            yield crawler
        except KeyboardInterrupt as interrupt:
            LOGGER.info("caught an interruption")
            should_save = True
        except Exception as e:
            # Another exception happened, we should save in case.
            LOGGER.error("crawl manager caught an exception")
            should_save = True
            raise e
        finally
            crawler.stop()
            if should_save and save is not None:
                crawler.save_pickle(save)

    def run(self, seeds=())
        for url in seeds:
            self._add_resource(url)
        self._crawler.start()
        with self._manage(self._crawler, save='crawler.pickle') as crawler:
            while crawler.is_working():
                try:
                    key, res = crawler.get_result(timeout=1)
                except Queue.Empty:
                    continue
                self._handle_result(key, res)
        LOGGER.info("crawl manager has finished")

    def _add_resource(self, url, parent_id):
        resource = self._store.find(Resource, Resource.url == url).one()
        if resource is None:
            # We've never seent this URL---we need to crawl it.
            resource = Resource(url)
            self._store.add(resource)
            self._store.flush()
            LOGGER.debug("resource {} not fetched yet, creating a new job"
                    .format(resource.id))
            self._crawler.add_job(resource.id, url)
        # Add an edge.
        LOGGER.debug("adding edge between {} and {}".format(
                parent_id, resource.id))
        self._store.add(Edge(parent_id, resource.id))
        self._store.commit()

    def _handle_result(self, key, result):
        LOGGER.debug("handling result for key {}".format(key))
        resource = self._store.find(Resource, key)
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
            LOGGER.debug("writing response body to disk for key {}".format(key))
            # Save the file to disk.
            path = os.path.join(self._data_dir, "{}.html".format(resource.id))
            with open(path, 'w') as f:
                f.write(result['body'])
            # Extract more jobs.
            if self._process is not None:
                base = urlparse.urlparse(resource.final_url)
                for url in self._process(res):
                    parsed = urlparse.urlparse(url)
                    if not parsed.netloc:
                        canonical = urlparse.urlunparse(base[:2] + parsed[2:])
                    self._add_resource(canonical, resource.id)
