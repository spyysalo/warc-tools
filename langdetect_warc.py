#!/usr/bin/env python3

# Run langdetect on WARC file text content extracted with trafilatura.

import sys
import gzip
import datetime
import logging

import trafilatura

from argparse import ArgumentParser

from warcio.archiveiterator import ArchiveIterator
from langdetect import DetectorFactory, detect_langs


DetectorFactory.seed = 0    # Make langdetect deterministic


def argparser():
    ap = ArgumentParser()
    ap.add_argument('-v', '--verbose', default=False, action='store_true')
    ap.add_argument('warc', nargs='?', default=None)
    return ap


def now():
    return datetime.datetime.now()


def detect_content_languages(id_, content):
    if not content:
        return f'SKIP:EMPTY-CONTENT'
    try:
        text_content = trafilatura.extract(content)
    except Exception as e:
        logging.error(f'failed extract for {id_}: {e}')
        return f'SKIP:EXTRACT-ERROR: {e}'

    if not text_content:
        return f'SKIP:EMPTY-TEXT'
    try:
        langs = detect_langs(text_content)
    except Exception as e:
        logging.error(f'failed langdetect for {id_}: {e}')
        return f'SKIP:LANGDETECT-ERROR: {e}'
    return langs


def print_timed(msg, options, out=sys.stderr):
    if options.verbose:
        print(f'{msg} at {datetime.now()}', file=out, flush=True)


def get_id(record):
    return record.rec_headers.get_header('WARC-Record-ID')


def get_payload_type(record):
    return record.rec_headers.get_header('WARC-Identified-Payload-Type')


def is_extractable_type(type_):
    if type_ in { 'application/pdf' }:
        return False
    return True


def process_stream(flo):
    for record in ArchiveIterator(flo):
        if record.rec_type != 'response':
            continue
        id_ = get_id(record)
        logging.info(f'{id_}: START {now()}')
        type_ = get_payload_type(record)
        if not is_extractable_type(type_):
            print(f'{id_}\tSKIP:{type_}')
            langs = None
        else:
            content = record.content_stream().read()
            logging.info(f'{id_}:{type_}: READ {len(content)} {now()}')
            langs = detect_content_languages(id_, content)
            print(f'{id_}\t{langs}')
        logging.info(f'{id_}:{type_}: DONE, languages {langs} {now()}')


def set_trafilatura_loglevel(level):
    try:
        trafilatura.core.LOGGER.setLevel(level)
        trafilatura.utils.LOGGER.setLevel(level)
    except:
        logging.warning('Failed to set trafilatura log level')


def main(argv):
    args = argparser().parse_args(argv[1:])

    logging.basicConfig()
    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)
        set_trafilatura_loglevel(logging.ERROR)
    else:
        set_trafilatura_loglevel(logging.CRITICAL)

    if args.warc is None:
        process_stream(sys.stdin.buffer)    # default to STDIN
    else:
        with gzip.open(args.warc) as f:
            process_stream(f)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
