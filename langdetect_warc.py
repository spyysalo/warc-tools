#!/usr/bin/env python3

# Run langdetect warc file text content extracted with trafilatura.

import sys
import gzip
import logging

import trafilatura

from argparse import ArgumentParser

from warcio.archiveiterator import ArchiveIterator
from langdetect import DetectorFactory, detect_langs


DetectorFactory.seed = 0    # Make langdetect deterministic


def argparser():
    ap = ArgumentParser()
    ap.add_argument('warc')
    return ap


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


def get_id(record):
    return record.rec_headers.get_header('WARC-Record-ID')


def main(argv):
    args = argparser().parse_args(argv[1:])
    with gzip.open(args.warc) as f:
        for record in ArchiveIterator(f):
            if record.rec_type != 'response':
                continue
            id_ = get_id(record)
            content = record.content_stream().read()
            langs = detect_content_languages(id_, content)
            print(f'{id_}\t{langs}')


if __name__ == '__main__':
    sys.exit(main(sys.argv))
