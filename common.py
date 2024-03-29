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


def get_payload_type(record):
    return record.rec_headers.get_header('WARC-Identified-Payload-Type')


def get_mime_type(record):
    type_ = record.rec_headers.get_header('WARC-Identified-Payload-Type')
    if type_ is not None:
        return type_
    else:
        return record.rec_headers.get_header('Content-Type')


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


def get_record_text_content(record, trafilatura_options=None):
    id_ = get_record_id(record)
    if record.rec_type != 'response':
        raise ValueError(f'non-response record {id_}')
    mime_type = get_payload_type(record)
    if is_unsupported_mime_type(mime_type):
        raise ValueError(f'unsupported mime type {mime_type} for {id_}')
    content = record.content_stream().read()
    if not content:
        raise ValueError(f'empty content for {id_}')
    try:
        text = get_text_content(id_, mime_type, content, trafilatura_options)
    except Exception as e:
        raise ValueError(f'error extracting text for {id_}: {e}')
    if text is None or not text:
        raise ValueError(f'empty extracted text for {id_}')
    return text


def set_trafilatura_loglevel(level):
    try:
        trafilatura.core.LOGGER.setLevel(level)
        trafilatura.utils.LOGGER.setLevel(level)
    except:
        logging.warning('Failed to set trafilatura log level')
