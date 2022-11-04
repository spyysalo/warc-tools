#!/usr/bin/env python3

# Calculate hashes for text in WARC package.

import sys
import re
import os
import gzip
import base64
import logging

import mmh3

from collections import defaultdict
from glob import glob
from argparse import ArgumentParser

from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator


# Mime types for plain text
_PLAIN_TEXT_MIME_TYPES = {
    'text/plain',
}

# MIME types for (X)HTML and similar
_HTML_LIKE_MIME_TYPES = {
    'text/html',
    'application/xhtml+xml',
    'application/xml',
    'application/http',
}

# MIME types for which text extraction is currently implemented
_SUPPORTED_MIME_TYPES = _PLAIN_TEXT_MIME_TYPES | _HTML_LIKE_MIME_TYPES

# MIME types for which text extraction is not (currently) supported
_UNSUPPORTED_MIME_TYPES = {
    'application/pdf',    # maybe in the future?
    'application/rtf',    # maybe in the future?
    'application/rdf+xml',
    'application/atom+xml',
    'application/rss+xml',
    'application/json',
    'application/octet-stream',
    'application/zip',
    'text/calendar',
}

# "Main" MIME types for which text extraction is unsupported unless
# specifically identified with subtype in _SUPPORTED_MIME_TYPES
_UNSUPPORTED_MAIN_MIME_TYPES = {
    'image',
    'audio',
    'video',
    'application',
}


def argparser():
    ap = ArgumentParser()
    ap.add_argument('input', nargs='+', metavar='FILE-OR-DIR')
    ap.add_argument('-r', '--refers-to', default=False, action='store_true',
                    help='use "WARC-Refers-To" as ID (for WET files)')
    ap.add_argument('-n', '--no-norm', default=False, action='store_true',
                    help='do not normalize text before computing hash')
    ap.add_argument('-v', '--verbose', default=False, action='store_true')
    ap.add_argument('-q', '--quiet', default=False, action='store_true')
    return ap


def is_response(record):
    return record.rec_type == 'response'


def is_plain_text_mime_type(mime_type):
    return mime_type in _PLAIN_TEXT_MIME_TYPES


def is_html_like_mime_type(mime_type):
    return (
        mime_type in _HTML_LIKE_MIME_TYPES or
        any(mime_type.startswith(t) for t in _HTML_LIKE_MIME_TYPES)
    )


def is_unsupported_mime_type(mime_type):
    if mime_type in _SUPPORTED_MIME_TYPES:
        return False
    elif mime_type in _UNSUPPORTED_MIME_TYPES:
        return True
    for m in _SUPPORTED_MIME_TYPES:
        if mime_type.startswith(f'{m};'):
            return False
    for m in _UNSUPPORTED_MAIN_MIME_TYPES:
        if mime_type.startswith(f'{m}/'):
            return True
    return False


def get_target_uri(record):
    return record.rec_headers.get_header('WARC-Target-URI')


def get_record_id(record):
    return record.rec_headers.get_header('WARC-Record-ID')


def get_refers_to(record):
    return record.rec_headers.get_header('WARC-Refers-To')


def get_mime_type(record):
    type_ = record.rec_headers.get_header('WARC-Identified-Payload-Type')
    if type_ is not None:
        return type_
    else:
        return record.rec_headers.get_header('Content-Type')


def normalize_space(text):
    text = text.strip()
    text = re.sub(r'\n+', '\n', text)
    lines = text.split('\n')
    lines = [' '.join(line.split()) for line in lines]
    lines = [line for line in lines if line and not line.isspace()]
    text = '\n'.join(lines)
    return text


def extract_text_from_html(id_, uri, mime_type, content, args):
    soup = BeautifulSoup(content, features='html.parser') #features='lxml')

    # drop script and style elements (TODO: is this necessary?)
    for e in soup(['script', 'style']):
        e.extract()

    text = soup.get_text(separator='\n')
    text = normalize_space(text)
    return text


def get_text_content(id_, uri, mime_type, content, args):
    if is_plain_text_mime_type(mime_type):
        return content.decode('utf-8')
    elif is_html_like_mime_type(mime_type):
        return extract_text_from_html(id_, uri, mime_type, content, args)
    else:
        logging.error(f'unexpected MIME type {mime_type} for {id_}')
        # try anyway
        return extract_text_from_html(id_, uri, mime_type, content, args)


def write_stats(stats, label, out=sys.stderr):
    print(
        f'{label}',
        f'{stats["total"]} records,',
        f'{stats["responses"]} responses,',
        f'{stats["conversions"]} conversions,',
        f'{stats["empties"]} with empty text content,',
        f'{stats["unsupported"]} with unsupported type,',
        f'{stats["errors"]} errors',
        file=out
    )


def clean_text(text):
    if not text:
        return text
    text = text.replace('\x00', '')    # remove nulls
    return text


def normalize_text(text, args):
    if args.no_norm:
        return text
    else:
        # only use alphabetic sequences, separated by space
        return ' '.join(re.findall(r'[^\W\d_]+', text))


def clean_id(id_):
    start, end = '<urn:uuid:', '>'
    assert id_.startswith(start) and id_.endswith(end)
    return id_[len(start):-len(end)]


def compute_hash(text):
    try:
        hash_ = mmh3.hash128(text)
    except UnicodeEncodeError:
        # work around unicode issues
        text = text.encode('utf-8', 'replace').decode('utf-8')
        hash_ = mmh3.hash128(text)
    # compact representation
    return base64.b64encode(hash_.to_bytes(16, 'big')).decode()


def compute_hashes_stream(stream, stats, args):
    for record in ArchiveIterator(stream):
        stats['total'] += 1

        if record.rec_type == 'response':
            stats['responses'] += 1
        elif record.rec_type == 'conversion':
            stats['conversions'] += 1
        else:
            continue

        if not args.refers_to:
            id_ = get_record_id(record)
        else:
            id_ = get_refers_to(record)
        id_ = clean_id(id_)

        uri = get_target_uri(record)
        type_ = get_mime_type(record)
        content = record.content_stream().read()

        if not content:
            logging.warning(f'empty content: {id_}')
            stats['empties'] += 1
            continue

        if is_unsupported_mime_type(type_):
            logging.error(f'unsupported payload type: {type_}')
            stats['unsupported'] += 1
            continue

        try:
            text_content = get_text_content(id_, uri, type_, content, args)
        except Exception as e:
            logging.error(f'failed extract for {id_}: {e}')
            stats['errors'] += 1
            continue

        text_content = clean_text(text_content)
        text_content = normalize_text(text_content, args)

        if not text_content:
            logging.info(f'empty text content: {id_}')
            stats['empties'] += 1
            continue

        try:
            hash_ = compute_hash(text_content)
        except Exceptions as e:
            logging.error(f'computing hash for {id_}: {e}')
            stats['errors'] += 1
            continue

        if hash_ is None:
            try:
                logging.warning(f'hash is None for {id_}: "{text_content}"')
            except:
                logging.warning(f'hash is None for {id_}: [FAILED TO PRINT]')

        print(f'{id_}\t{hash_}')

        if stats['total'] % 1000 == 0 and not args.quiet:
            write_stats(stats, 'processed')


def compute_hashes(fn, stats, args):
    if not fn.endswith('.gz'):
        with open(fn, 'rb') as f:
            compute_hashes_stream(f, stats, args)
    else:
        with gzip.open(fn) as f:
            compute_hashes_stream(f, stats, args)


def configure_logging(args):
    logging.basicConfig()
    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)
    elif args.quiet:
        logging.getLogger().setLevel(logging.ERROR)


def main(argv):
    args = argparser().parse_args(argv[1:])

    configure_logging(args)

    stats = defaultdict(int)
    for fn in args.input:
        if os.path.isfile(fn):
            compute_hashes(fn, stats, args)
        else:
            paths = glob(f'{fn}/**/*.warc.gz', recursive=True)
            for p in sorted(paths):
                try:
                    compute_hashes(p, stats, args)
                except Exception as e:
                    logging.error(f'failed to convert {p}: {e}')
                    raise

    write_stats(stats, 'DONE.')


if __name__ == '__main__':
    sys.exit(main(sys.argv))
