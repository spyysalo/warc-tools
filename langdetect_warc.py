#!/usr/bin/env python3

import sys
import os
import re
import gzip
import logging

import fasttext

from collections import defaultdict
from glob import glob
from argparse import ArgumentParser

from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup

from common import (
    get_record_id,
    get_target_uri,
    get_mime_type,
    is_plain_text_mime_type,
    is_html_like_mime_type,
    is_unsupported_mime_type,
)

# workaround for high recursion in str(soup)
sys.setrecursionlimit(10000)

# Default for regex defining "word"
DEFAULT_WORD_RE = r'\b[^\W\d_]{2,}\b'

# Prefix for fasttext labels
LABEL_PREFIX = '__label__'

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
    ap.add_argument('--label', default='fi',
                    help='label of target language')
    ap.add_argument('--keep-words', metavar='N', type=int, default=10,
                    help='keep if at number of target language words >= N')
    ap.add_argument('--keep-ratio', metavar='X', type=float, default=0.25,
                    help='keep if ratio of target language words >= X')
    ap.add_argument('--min-ratio', metavar='X', type=int, default=0.01,
                    help='discard if target language word ratio < X')
    ap.add_argument('--max-labels', type=int, default=10,
                    help='maximum number of labels to predict')
    ap.add_argument('--min-pred-words', type=int, default=1,
                    help='minimum number of words to predict language on')
    ap.add_argument('--threshold', type=float, default=0.999,
                    help='threshold for predicting target language')
    ap.add_argument('--invert', default=False, action='store_true')
    ap.add_argument('--word-regex', default=DEFAULT_WORD_RE,
                    help='regular expression defining "word" for --min-words')
    ap.add_argument('model', help='FastText model')
    ap.add_argument('input', nargs='+', metavar='FILE-OR-DIR')
    return ap


def is_response(record):
    return record.rec_type == 'response'


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


def extract_text_from_html(id_, uri, mime_type, content, args):
    return beautifulsoup_extract(content)


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


def clean_id(id_):
    assert id_.startswith('<') and id_.endswith('>')
    return id_[1:-1]


def replace_unicode_errors(string):
    return string.encode('utf-8', 'replace').decode('utf-8')


def target_label_probability(text, model, args):
    target_label = LABEL_PREFIX + args.label
    try:
        labels, probs = model.predict([text], k=args.max_labels)
    except:
        text = replace_unicode_errors(text)
        labels, probs = model.predict([text], k=args.max_labels)
    for label, prob in zip(labels[0], probs[0]):
        if label == target_label:
            return max(0.0, min(1.0, prob))
    return 0.0    # target label not found


def langid_by_line(text, model, args):
    lines = text.split('\n')
    total_words, target_language_words = 0, 0
    for line in lines:
        line = line.strip()

        if line.isspace() or not line:
            continue
        
        word_count = len(args.word_regex.findall(line))
        total_words += word_count
        
        if word_count < args.min_pred_words:
            continue    # too few words to predict

        prob = target_label_probability(line, model, args)

        if prob >= args.threshold:
            target_language_words += word_count

    return target_language_words, total_words


def keep_text(target_language_words, total_words, args):
    if total_words == 0:
        return False

    target_language_ratio = target_language_words / total_words

    # Filter by ratio first to reduce likely false positives
    if target_language_ratio < args.min_ratio:
        return False

    elif target_language_words >= args.keep_words:
        return True
    elif target_language_ratio >= args.keep_ratio:
        return True
    else:
        return False


def langdetect_warc_stream(stream, model, stats, args):
    for record in ArchiveIterator(stream):
        stats['total'] += 1

        if is_response(record):
            stats['responses'] += 1
        elif record.rec_type == 'conversion':
            stats['conversions'] += 1
        else:
            continue

        id_ = get_record_id(record)
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
            text = get_text_content(id_, uri, type_, content, args)
        except Exception as e:
            logging.error(f'failed extract for {id_}: {e}')
            stats['errors'] += 1
            continue

        text = clean_text(text)

        if not text:
            logging.info(f'empty text content: {id_}')
            stats['empties'] += 1
            continue

        target_language_words, total_words = langid_by_line(text, model, args)
        keep = keep_text(target_language_words, total_words, args)

        print(f'{id_}\t{target_language_words}\t{total_words}\t{keep}')
        
        # try:
        #     print(text_content)
        # except UnicodeEncodeError:
        #     text_content = replace_unicode_errors(text_content)
        #     print(text_content)                

        if stats['total'] % 1000 == 0:
            write_stats(stats, 'processed')


def langdetect_warc(fn, model, stats, args):
    if not fn.endswith('.gz'):
        with open(fn, 'rb') as f:
            langdetect_warc_stream(f, model, stats, args)
    else:
        with gzip.open(fn) as f:
            langdetect_warc_stream(f, model, stats, args)


def main(argv):
    args = argparser().parse_args(argv[1:])

    args.word_regex = re.compile(args.word_regex)

    model = fasttext.load_model(args.model)

    stats = defaultdict(int)
    for fn in args.input:
        if os.path.isfile(fn):
            langdetect_warc(fn, model, stats, args)
        else:
            paths = glob(f'{fn}/**/*.warc.gz', recursive=True)
            for p in sorted(paths):
                try:
                    langdetect_warc(p, model, stats, args)
                except Exception as e:
                    logging.error(f'failed to convert {p}: {e}')
                    raise

    write_stats(stats, 'DONE.')


if __name__ == '__main__':
    sys.exit(main(sys.argv))
