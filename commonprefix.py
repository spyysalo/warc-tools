#!/usr/bin/env python3

import sys
import fileinput

from os.path import commonprefix, dirname
from argparse import ArgumentParser


def argparser():
    ap = ArgumentParser(description='Get most common prefix')
    ap.add_argument('-d', '--dir', default=False, action='store_true',
                    help='Get dirname of most common prefix')
    ap.add_argument('file', nargs='*')
    return ap


def main(argv):
    args = argparser().parse_args(argv[1:])

    strings = []
    for l in fileinput.input(args.file):
        strings.append(l.rstrip('\n'))

    prefix = commonprefix(strings)
    if not args.dir:
        print(prefix)
    else:
        print(dirname(prefix))


if __name__ == '__main__':
    sys.exit(main(sys.argv))
