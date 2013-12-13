#!/usr/bin/env python
import argparse
import swim
import re


DEFAULT_FOLDER = "./test"
DEFAULT_SEED = u"http://127.0.0.1:5000/"

URL_RE = re.compile(r'<a.*?href="(?P<url>.*?)">')


def process(body):
    for match in URL_RE.finditer(body):
        yield match.group('url')


def main(args):
    swim.set_loglevel(args.loglevel)
    if args.pickle is None:
        mgr = swim.Manager(args.folder, processor=process, rate=0.2)
        mgr.run(seeds=[DEFAULT_SEED])
    else:
        mgr = swim.Manager(args.folder, processor=process,
                           fetcher_pickle=args.pickle)
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
