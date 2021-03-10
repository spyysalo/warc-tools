#!/usr/bin/env python3

# Filter warc file to records with given WARC-Record-ID values.

import sys
import gzip
import logging

from argparse import ArgumentParser

from warcio import WARCWriter
from warcio.archiveiterator import ArchiveIterator



def argparser():
    ap = ArgumentParser()
    ap.add_argument('ids')
    ap.add_argument('warc_in')
    ap.add_argument('warc_out')
    ap.add_argument('-v', '--verbose', default=False, action='store_true')
    return ap


def get_id(record):
    return record.rec_headers.get_header('WARC-Record-ID')


def filter_warc_stream(ids, warc_in, warc_out):
    writer = WARCWriter(warc_out, gzip=True)

    output, total, errors = 0, 0, 0
    for record in ArchiveIterator(warc_in):
        id_ = get_id(record)
        if id_ in ids:
            output += 1
            try:
                writer.write_record(record)
            except Exception as e:
                logging.error(f'failed to write record: {e}')
                errors += 1
        total += 1
        if total % 10000 == 0:
            logging.info(f'processed {total} records, output {output}, '
                         f'{errors} errors')
    print(f'Done, processed {total} records, output {output}, '
          f'{errors} errors')


def main(argv):
    args = argparser().parse_args(argv[1:])

    logging.basicConfig()
    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)

    ids = set()
    with open(args.ids) as id_in:
        for l in id_in:
            ids.add(l.rstrip('\n'))

    with gzip.open(args.warc_in) as warc_in:
        with open(args.warc_out, 'wb') as warc_out:
            filter_warc_stream(ids, warc_in, warc_out)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
