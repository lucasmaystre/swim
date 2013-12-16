# swim

*A simple, no-frills web crawler.*

In a nutshell: you seed it with a bunch of URLs, you give it a function that
extracts more URLs from the HTTP responses, and `swim` does the rest.
Noticeable features are:

- multithreaded, possibility to enable rate limiting
- kill the crawler and resume it later without losing data
- all crawling-related information is persisted in a database

`swim` is minimalistic by design. There are plenty of powerful crawlers out
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

If you only want an engine that fetches URLs concurrently and provides rate
limiting, you might be interested in the class `swim.Fetcher`. It's your job to
decide which URLs to fetch and what to do with the responses.


## Assumptions & design decisions:

This is a (slightly technical and obtuse) list of assumptions the module makes,
and is subject to change.

- decoupling fetching URLs from processing the content
- new URLs to fetch can be determined solely by processing HTML output
- page = URL + GET params.
- GET requests only - no POST, etc.
- Output is response body only
- no cookies handling, form submissions, etc.

It requires the following additional python libs: `storm`, `requests` and
`futures`. As far as I know, it only works with Python 2.x (probably mostly
because of `storm`.)

Resuming a crawl works only if it is able to gracefully shut down. The shutdown
procedure is as follows:

1. Set the shutdown flag and stop submitting new jobs to the executor.
2. Wait until all jobs submitted to the executor are done (handled by a context
   manager.) This means that all pending requests are finished.
3. Process everything in the output queue, possibly adding new URls to the
   input queue.
4. Pickle the state of the fetcher (input queue + initialization params),
   commit and close the DB connection.

A wish list of things TODO:

- recover from crashes as well as graceful shutdowns.
- finish crawling the URLs in the input queue without creating new jobs. To
  make sure we could still expand the crawl, we could add a flag in the table
  which indicates whether the resource has been processed (i.e. new jobs
  extracted) or not.
- clean up the queues: the output queue should not be pickled anymore (it is
  empty at the end), so we could just use a regular Queue.Queue for that. As
  for the input queue, it might be simpler and more efficient to subclass /
  wrap `collections.deque` which also has atomic ops.


## License

Released under the [MIT license](http://opensource.org/licenses/MIT).
