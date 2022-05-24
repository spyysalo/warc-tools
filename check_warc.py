#!/usr/bin/env python3

import sys
import gzip

from argparse import ArgumentParser
from warcio.archiveiterator import ArchiveIterator


def argparser():
    ap = ArgumentParser()
    ap.add_argument('-q', '--quiet', default=False, action='store_true')
    ap.add_argument('-v', '--verbose', default=False, action='store_true')
    ap.add_argument('warc', nargs='+')
    return ap


def check_warc(warc_stream, options):
    for total, record in enumerate(ArchiveIterator(warc_stream), start=1):
        pass
    return total


def main(argv):
    args = argparser().parse_args(argv[1:])

    for fn in args.warc:
        if args.verbose:
            print(f'Start checking {fn} ...', file=sys.stderr)
        try:
            with gzip.open(fn) as f:
                total = check_warc(f, args)
            if not args.quiet:
                print(f'{fn}: OK: {total} records')
        except Exception as e:
            error_str = str(e).replace('\n', ' ')
            print(f'{fn}: ERROR: {error_str}')
        if args.verbose:
            print(f'Done {fn}.', file=sys.stderr)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
