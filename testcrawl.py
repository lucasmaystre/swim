#!/usr/bin/env python
import argparse
import mucrawl
import re


DEFAULT_FOLDER = "./mucrawl"
DEFAULT_SEED = "http://127.0.0.1:5000/"

URL_RE = re.compile(r'<a.*?href="(?P<url>.*?)">')


def process(body):
    for match in URL_RE.finditer(body):
        yield match.group('url')


def main(args):
    if args.pickle is None:
        mgr = mucrawl.CrawlManager(args.folder, processor=process)
        mgr.run(seeds=[DEFAULT_SEED])
    else:
        mgr = mucrawl.CrawlManager(args.folder, processor=process,
                crawler_pickle=pickle)
        mgr.run()


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--loglevel', choices=('DEBUG', 'INFO', 'WARNING'),
        default='DEBUG')
    parser.add_argument('--folder', default=DEFAULT_FOLDER)
    parser.add_argument('--pickle', default=None)
    parser.add_argument('--seed', default=DEFAULT_SEED)
    return parser.parse_args()


if __name__ == '__main__':
    args = _parse_args()
    main(args)
