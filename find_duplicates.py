#!/usr/bin/env python3

# Remove WARC records with text content seen in previous records.

import sys
import dbm
import gzip
import logging

import mmh3

from argparse import ArgumentParser

from warcio.archiveiterator import ArchiveIterator

from common import (
    get_record_id,
    get_record_text_content,
    set_trafilatura_loglevel,
)


def argparser():
    ap = ArgumentParser()
    ap.add_argument('warc', nargs='+')
    ap.add_argument('--db', default='response-hashes.db')
    ap.add_argument('-v', '--verbose', default=False, action='store_true')
    return ap


def find_duplicates(db, warc, options):
    for record in ArchiveIterator(warc):
        id_ = get_record_id(record)
        try:
            text = get_record_text_content(record)
        except ValueError as e:
            logging.error(e)
            continue
        text_hash = mmh3.hash_bytes(text)
        seen = db.get(text_hash, None)
        byte_id = id_.encode('utf-8')
        if seen is None:
            db[text_hash] = byte_id
        elif seen == byte_id:
            pass    # same record
        else:
            seen = seen.decode('utf-8')
            print(f'{id_}\t{seen}')


def main(argv):
    args = argparser().parse_args(argv[1:])

    logging.basicConfig()
    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)
        set_trafilatura_loglevel(logging.WARNING)
    else:
        set_trafilatura_loglevel(logging.CRITICAL)

    db = dbm.open(args.db, 'c')
    logging.info(f'opened {type(dbm).__name__} db {args.db}')

    for fn in args.warc:
        with gzip.open(fn) as warc:
            find_duplicates(db, warc, args)

    db.close()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
