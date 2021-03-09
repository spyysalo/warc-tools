#!/usr/bin/env python3

import sys
import random
import gzip
import logging

from argparse import ArgumentParser

from warcio import WARCWriter
from warcio.archiveiterator import ArchiveIterator



def argparser():
    ap = ArgumentParser()
    ap.add_argument('ratio', type=float)
    ap.add_argument('warc_in')
    ap.add_argument('warc_out')
    ap.add_argument('-s', '--seed', default=None, type=int)
    ap.add_argument('-v', '--verbose', default=False, action='store_true')
    return ap


def sample_warc_stream(ratio, warc_in, warc_out):
    writer = WARCWriter(warc_out, gzip=True)

    responses, total, errors = 0, 0, 0
    for record in ArchiveIterator(warc_in):
        if record.rec_type == 'response':
            responses += 1
            if random.random() < ratio:
                try:
                    writer.write_record(record)
                except Exception as e:
                    logging.error(f'failed to write record: {e}')
                    errors += 1
        total += 1
        if total % 10000 == 0:
            logging.info(f'processed {total} records, {responses} responses, '
                         f'{errors} errors')


def main(argv):
    args = argparser().parse_args(argv[1:])
    random.seed(args.seed)

    logging.basicConfig()
    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)

    with gzip.open(args.warc_in) as warc_in:
        with open(args.warc_out, 'wb') as warc_out:
            sample_warc_stream(args.ratio, warc_in, warc_out)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
