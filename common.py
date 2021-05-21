import sys
import logging

import trafilatura


# Mime types for plain text
_PLAIN_TEXT_MIME_TYPES = {
    'text/plain',
}

# MIME types for (X)HTML and similar
_HTML_LIKE_MIME_TYPES = {
    'text/html',
    'application/xhtml+xml',
    'application/xml',
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


def is_plain_text_mime_type(mime_type):
    return mime_type in _PLAIN_TEXT_MIME_TYPES


def is_html_like_mime_type(mime_type):
    return mime_type in _HTML_LIKE_MIME_TYPES


def is_unsupported_mime_type(mime_type):
    if mime_type in _SUPPORTED_MIME_TYPES:
        return False
    elif mime_type in _UNSUPPORTED_MIME_TYPES:
        return True
    for m in _UNSUPPORTED_MAIN_MIME_TYPES:
        if mime_type.startswith(f'{m}/'):
            return True
    return False


def get_target_uri(record):
    return record.rec_headers.get_header('WARC-Target-URI')


def get_record_id(record):
    return record.rec_headers.get_header('WARC-Record-ID')


def get_payload_type(record):
    return record.rec_headers.get_header('WARC-Identified-Payload-Type')


def get_text_content(id_, mime_type, content, trafilatura_options=None):
    if trafilatura_options is None:
        trafilatura_options = {}
    if is_plain_text_mime_type(mime_type):
        return content.decode('utf-8')
    elif is_html_like_mime_type(mime_type):
        return trafilatura.extract(content, **trafilatura_options)
    else:
        logging.warning(f'unexpected MIME type {mime_type} for {id_}')
        # try anyway
        return trafilatura.extract(content, **trafilatura_options)
