#!/usr/bin/env python3

# Run trafilatura to extract warc file text content.

import sys
import gzip
import json
import logging

import trafilatura

from argparse import ArgumentParser

from warcio.archiveiterator import ArchiveIterator


def argparser():
    ap = ArgumentParser()
    ap.add_argument('warc')
    ap.add_argument('-v', '--verbose', default=False, action='store_true')
    return ap


def get_id(record):
    return record.rec_headers.get_header('WARC-Record-ID')


def process_stream(flo):
    responses, total, empties, errors = 0, 0, 0, 0
    for record in ArchiveIterator(flo):
        total += 1
        if record.rec_type != 'response':
            continue
        responses += 1
        id_ = get_id(record)
        content = record.content_stream().read()
        if not content:
            empties += 1
            continue
        try:
            text_content = trafilatura.extract(content)
        except Exception as e:
            logging.error(f'failed extract for {id_}: {e}')
            errors += 1
            continue
        if not text_content:
            empties += 1
            continue
        escaped_text = json.dumps(text_content, ensure_ascii=False)
        print(f'{id_}\t{escaped_text}')
        if total % 1000 == 0:
            logging.info(f'processed {total} records, {responses} responses, '
                         f'{empties} with empty text content, {errors} errors')

    print(f'Done, processed {total} records, {responses} responses, '
          f'{empties} with empty text content, {errors} errors',
          file=sys.stderr)


def main(argv):
    args = argparser().parse_args(argv[1:])

    logging.basicConfig()
    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)

    with gzip.open(args.warc) as f:
        process_stream(f)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
