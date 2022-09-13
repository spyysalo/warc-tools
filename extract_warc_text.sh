#!/bin/bash

IN="$1"
OUT="$2"

mkdir -p $(dirname "$OUT")

set -euo pipefail

source venv/bin/activate

python extract_warc_text.py -q "$IN" > "$OUT"
