#!/usr/bin/env python3

# Extract text from WARC files and convert inta a simple JSONL format
# with keys 'id', 'text', and 'meta'.

# TODO resolve overlap with other scripts

import sys
import os
import re
import gzip
import json
import random
import logging

import zstandard as zstd
import trafilatura
import justext
import inscriptis

from collections import defaultdict
from glob import glob
from argparse import ArgumentParser

from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
from goose3 import Goose
from w3lib.encoding import html_to_unicode

# workaround for high recursion in str(soup)
sys.setrecursionlimit(10000)

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

# HTML-to-text extractors
_EXTRACTORS = [
    'trafilatura',
    'justext',
    'beautifulsoup',
    'goose3',
    'inscriptis',
]


# HTML inline elements
# (from https://developer.mozilla.org/en-US/docs/Web/HTML/Inline_elements)
_INLINE_ELEMENTS = [
    'a',
    'abbr',
    'acronym',
    'audio',
    'b',
    'bdi',
    'bdo',
    'big',
    #'br', # Keep linebreak on <br>
    'button',
    'canvas',
    'cite',
    'code',
    'data',
    'datalist',
    'del',
    'dfn',
    'em',
    'embed',
    'i',
    'iframe',
    'img',
    'input',
    'ins',
    'kbd',
    'label',
    'map',
    'mark',
    'meter',
    'noscript',
    'object',
    'output',
    'picture',
    'progress',
    'q',
    'ruby',
    's',
    'samp',
    'script',
    'select',
    'slot',
    'small',
    'span',
    'strong',
    'sub',
    'sup',
    'svg',
    'template',
    'textarea',
    'time',
    'u',
    'tt',
    'var',
    'video',
    'wbr',
]


def argparser():
    ap = ArgumentParser()
    ap.add_argument('input', nargs='+', metavar='FILE-OR-DIR')
    ap.add_argument('-e', '--extractor', default='trafilatura',
                    choices=_EXTRACTORS + ['random'])
    ap.add_argument('-H', '--html', default=False, action='store_true',
                    help='output HTML instead of extracted text')
    ap.add_argument('-r', '--refers-to', default=False, action='store_true',
                    help='use "WARC-Refers-To" as ID (for WET files)')
    ap.add_argument('-s', '--sample', type=float, default=None,
                    help='sample given ratio of responses')
    ap.add_argument('-t', '--text-only', default=False, action='store_true',
                    help='output plain text instead of JSONL')
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


def get_record_date(record):
    return record.rec_headers.get_header('WARC-Date')


def get_content_length(record):
    return int(record.rec_headers.get_header('Content-Length'))


def get_mime_type(record):
    type_ = record.rec_headers.get_header('WARC-Identified-Payload-Type')
    if type_ is not None:
        return type_
    else:
        return record.rec_headers.get_header('Content-Type')


def justext_extract(content):
    paragraphs = justext.justext(content, justext.get_stoplist("Finnish"))
    paragraphs = [p for p in paragraphs if not p.is_boilerplate]
    return '\n\n'.join(p.text for p in paragraphs)


def trafilatura_extract(content, uri, args):
    options = {
        #'include_tables': False,
        #'favor_precision': True,
        #'favor_recall': True,
    }
    return trafilatura.extract(content, url=uri, **options)


def normalize_space(text):
    text = text.strip()
    text = re.sub(r'\n+', '\n', text)
    lines = text.split('\n')
    lines = [' '.join(line.split()) for line in lines]
    lines = [line for line in lines if line and not line.isspace()]
    text = '\n'.join(lines)
    return text


def beautifulsoup_extract(content, parser='html.parser'):
    soup = BeautifulSoup(content, features=parser)
    
    # drop script and style elements (TODO: unnecessary for get_text)
    for e in soup.find_all(['script', 'style']):
        e.extract()

    # drop undesirable elements
    for e in soup.find_all(['noscript']):
        e.extract()

    # maybe drop these?
    # for e in soup.find_all(['header', 'footer']):
    #     e.extract()

    # unwrap inline elements for get_text('\n')
    for e in soup.find_all(_INLINE_ELEMENTS):
        e.unwrap()

    # reparse to merge (see https://stackoverflow.com/questions/44679677)
    soup = BeautifulSoup(str(soup), features=parser)

    text = soup.get_text(separator='\n')
    text = normalize_space(text)

    return text


def inscriptis_extract(content):
    content = html_to_unicode(None, content)[1]    # TODO pass header
    #content = UnicodeDammit(content).unicode_markup    # alternative
    text = inscriptis.get_text(content)
    text = normalize_space(text)
    return text


def goose3_extract(content):
    g = Goose()
    a = g.extract(raw_html=content)
    return a.cleaned_text


def pretty_html(content, parser='html.parser'):
    soup = BeautifulSoup(content, features=parser)

    # drop script and style elements
    for e in soup.find_all(['script', 'style']):
        e.extract()

    return soup.prettify()


def extract_text_from_html(id_, uri, mime_type, content, args):
    if args.html:
        return pretty_html(content)

    if args.extractor == 'random':
        extractor = random.choice(_EXTRACTORS)
    else:
        extractor = args.extractor

    if extractor == 'trafilatura':
        return trafilatura_extract(content, uri, args)
    elif extractor == 'justext':
        return justext_extract(content)
    elif extractor == 'beautifulsoup':
        return beautifulsoup_extract(content)
    elif extractor == 'inscriptis':
        return inscriptis_extract(content)
    elif extractor == 'goose3':
        return goose3_extract(content)
    else:
        raise ValueError(extractor)


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
        f'{stats["skipped"]} skipped,',
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


def clean_id(id_):
    assert id_.startswith('<') and id_.endswith('>')
    return id_[1:-1]


def convert_warc_stream(stream, stats, args):
    for record in ArchiveIterator(stream):
        stats['total'] += 1

        if args.sample is not None and random.random() > args.sample:
            stats['skipped'] += 1
            continue

        if is_response(record):
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
        date = get_record_date(record)
        length = get_content_length(record)
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

        if not text_content:
            logging.info(f'empty text content: {id_}')
            stats['empties'] += 1
            continue

        if args.text_only:
            try:
                print(text_content)
            except UnicodeEncodeError:
                text_content = text_content.encode('utf-8', 'replace').decode('utf-8')
                print(text_content)                
        else:
            data = {
                'id': f'commoncrawl:{id_}',
                'text': text_content,
                'meta': {
                    'uri': uri,
                    'source_type': type_,
                    'download_date': date,
                    'source_length': length,
                },
            }
            try:
                print(json.dumps(data, ensure_ascii=False))
            except UnicodeEncodeError:
                data['text'] = data['text'].encode('utf-8', 'replace').decode('utf-8')
                print(json.dumps(data, ensure_ascii=False))

        if stats['total'] % 1000 == 0 and not args.quiet:
            write_stats(stats, 'processed')


def convert_warc(fn, stats, args):
    if fn.endswith('.gz'):
        with gzip.open(fn) as f:
            convert_warc_stream(f, stats, args)
    elif fn.endswith('.zst'):
        dctx = zstd.ZstdDecompressor(max_window_size=2**31)
        with zstd.open(fn, 'rb', dctx=dctx) as f:
            convert_warc_stream(f, stats, args)
    else:
        with open(fn, 'rb') as f:
            convert_warc_stream(f, stats, args)


def set_trafilatura_loglevel(level):
    try:
        trafilatura.core.LOGGER.setLevel(level)
        trafilatura.utils.LOGGER.setLevel(level)
    except:
        logging.warning('Failed to set trafilatura log level')


def configure_logging(args):
    logging.basicConfig()
    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)
        set_trafilatura_loglevel(logging.WARNING)
    elif args.quiet:
        logging.getLogger().setLevel(logging.ERROR)
        set_trafilatura_loglevel(logging.CRITICAL)
    else:
        set_trafilatura_loglevel(logging.ERROR)


def main(argv):
    args = argparser().parse_args(argv[1:])

    configure_logging(args)

    stats = defaultdict(int)
    for fn in args.input:
        if os.path.isfile(fn):
            convert_warc(fn, stats, args)
        else:
            paths = glob(f'{fn}/**/*.warc.gz', recursive=True)
            for p in sorted(paths):
                try:
                    convert_warc(p, stats, args)
                except Exception as e:
                    logging.error(f'failed to convert {p}: {e}')
                    raise

    write_stats(stats, 'DONE.')


if __name__ == '__main__':
    sys.exit(main(sys.argv))
