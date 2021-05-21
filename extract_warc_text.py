#!/usr/bin/env python3

# Run trafilatura to extract warc file text content.

import sys
import gzip
import json
import logging

import trafilatura

from argparse import ArgumentParser

from warcio.archiveiterator import ArchiveIterator

from common import (
    get_record_id,
    get_target_uri,
    get_payload_type,
    get_text_content,
    is_html_like_mime_type,
    is_unsupported_mime_type,
)

def argparser():
    ap = ArgumentParser()
    ap.add_argument('warc', nargs='+')
    ap.add_argument('-i', '--ids', metavar='ID[,ID...]', default=None,
                    help='Only extract text for given response IDs')
    ap.add_argument('-r', '--raw', default=False, action='store_true',
                    help='Output raw text without escapes')
    ap.add_argument('-x', '--xml', default=False, action='store_true',
                    help='Output XML')
    ap.add_argument('-v', '--verbose', default=False, action='store_true')
    ap.add_argument('-q', '--quiet', default=False, action='store_true')
    return ap


def process_stream(flo, options):
    responses, skipped, total, empties, errors, unsupported = 0, 0, 0, 0, 0, 0
    for record in ArchiveIterator(flo):
        total += 1
        if record.rec_type != 'response':
            continue
        responses += 1
        id_ = get_record_id(record)
        type_ = get_payload_type(record)
        if options.ids is not None and not any(i in id_ for i in options.ids):
            skipped += 1
            continue
        content = record.content_stream().read()
        if not content:
            empties += 1
            continue
        if is_unsupported_mime_type(type_):
            logging.info(f'unsupported payload type: {type_}')
            unsupported += 1
            continue
        if options.xml and not is_html_like_mime_type(type_):
            logging.info(f'unsupported for XML output: {type_}')
            unsupported += 1
            continue
        try:
            if options.xml:
                trafilatura_options = {
                    'output_format': 'xml',
                    'include_formatting': True,
                    'include_links': True,
                }
            else:
                trafilatura_options = {}
            text_content = get_text_content(id_, type_, content,
                                            trafilatura_options)
        except Exception as e:
            logging.error(f'failed extract for {id_}: {e}')
            errors += 1
            continue

        if not text_content:
            empties += 1
            continue
        if options.raw or options.xml:
            print(text_content)
        else:
            uri = get_target_uri(record)
            data = {
                'id': id_,
                'text': text_content,
                'meta': {
                    'target_uri': uri,
                },
            }
            print(json.dumps(data, sort_keys=True, ensure_ascii=False))

        if total % 1000 == 0:
            logging.info(f'processed {total} records, {responses} responses, '
                         f'{empties} with empty text content, {errors} errors')

    print(f'Done, processed {total} records, {responses} responses, '
          f'{skipped} skipped, {empties} empty text content, {errors} errors',
          file=sys.stderr)


def set_trafilatura_loglevel(level):
    try:
        trafilatura.core.LOGGER.setLevel(level)
    except:
        logging.warning('Failed to set trafilatura log level')


def main(argv):
    args = argparser().parse_args(argv[1:])
    if args.ids is not None:
        args.ids = args.ids.split(',')

    logging.basicConfig()
    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)
        set_trafilatura_loglevel(logging.WARNING)
    elif args.quiet:
        logging.getLogger().setLevel(logging.ERROR)
        set_trafilatura_loglevel(logging.CRITICAL)
    else:
        set_trafilatura_loglevel(logging.ERROR)

    for fn in args.warc:
        try:
            if not fn.endswith('.gz'):
                with open(fn, 'rb') as f:
                    process_stream(f, args)
            else:
                with gzip.open(fn) as f:
                    process_stream(f, args)
        except Exception as e:
            logging.error(f'failed processing {fn}: {e}')

if __name__ == '__main__':
    sys.exit(main(sys.argv))
