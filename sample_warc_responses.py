#!/usr/bin/env python3

import sys
import random
import gzip
import logging

from argparse import ArgumentParser

from io import BytesIO
from warcio import WARCWriter
from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArcWarcRecord

try:
    import trafilatura
except:
    logging.warning('import trafilatura failed, --language not available')


ANY_LANGUAGE = 'any'


def argparser():
    ap = ArgumentParser()
    ap.add_argument('ratio', type=float)
    ap.add_argument('warc_in')
    ap.add_argument('warc_out')
    ap.add_argument('-l', '--language', default=ANY_LANGUAGE)
    ap.add_argument('-p', '--lang-prob', default=0.1)
    ap.add_argument('-s', '--seed', default=None, type=int)
    ap.add_argument('-v', '--verbose', default=False, action='store_true')
    return ap


def get_id(record):
    return record.rec_headers.get_header('WARC-Record-ID')


def copy_warc_record(record, payload):
    return ArcWarcRecord(
        record.format,
        record.rec_type,
        record.rec_headers,
        payload,
        record.http_headers,
        record.content_type,
        record.length
    )


def sample_warc_stream(ratio, warc_in, warc_out, options):
    if options.language != ANY_LANGUAGE:
        from langdetect import DetectorFactory, detect_langs
        DetectorFactory.seed = options.seed   # Make langdetect deterministic

    writer = WARCWriter(warc_out, gzip=True)

    responses, total, errors, empties, notlang = 0, 0, 0, 0, 0
    for total, record in enumerate(ArchiveIterator(warc_in), start=1):
        if total % 10000 == 0:
            logging.info(f'processed {total} records, {responses} responses, '
                         f'{errors} errors, {empties} empty, '
                         f'{notlang} not in target language')

        if record.rec_type != 'response':
            continue

        responses += 1
        id_ = get_id(record)

        if random.random() > ratio:
            continue

        if options.language != ANY_LANGUAGE:
            # Workaround for https://github.com/webrecorder/warcio/issues/114
            payload_copy = BytesIO(record.raw_stream.read())
            record = copy_warc_record(record, payload_copy)
            content = record.content_stream().read()
            payload_copy.seek(0)
            try:
                text_content = trafilatura.extract(content)
            except Exception as e:
                logging.error(f'failed extract for {id_}: {e}')
                errors += 1
                continue

            if not text_content:
                empties += 1
                continue

            try:
                langs = detect_langs(text_content)
            except Exception as e:
                logging.error(f'failed langdetect for {id_}: {e}')
                errors += 1
                continue

            target_lang = [l for l in langs if l.lang == options.language]
            target_lang = None if not target_lang else target_lang[0]
            if target_lang is None or target_lang.prob < options.lang_prob:
                notlang += 1
                continue
        try:
            writer.write_record(record)
        except Exception as e:
            logging.error(f'failed to write record: {e}')
            errors += 1


def set_trafilatura_loglevel(level):
    try:
        trafilatura.core.LOGGER.setLevel(level)
    except:
        logging.warning('Failed to set trafilatura log level')


def main(argv):
    args = argparser().parse_args(argv[1:])
    random.seed(args.seed)

    logging.basicConfig()
    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)
    set_trafilatura_loglevel(logging.ERROR)

    with gzip.open(args.warc_in) as warc_in:
        with open(args.warc_out, 'wb') as warc_out:
            sample_warc_stream(args.ratio, warc_in, warc_out, args)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
