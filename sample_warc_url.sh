#!/bin/bash

# Run sample_warc_responses.py on a warc.gz URL

set -euo pipefail

# Command-line arguments
if [ $# -lt 4 ]; then
    echo -e "Usage: $0 RATIO LANGUAGE URL OUT [SEED]" >&2
    exit 1
fi

RATIO="$1"
LANGUAGE="$2"
URL="$3"
OUT="$4"
SEED="${5:-0}"

echo "----------------------------------------------------------------------"
echo "START $SLURM_JOBID: $(date): $URL"
echo "----------------------------------------------------------------------"

OUTDIR=$(dirname "$OUT")

mkdir -p "$OUTDIR"

# Create temporary directory and make sure it's wiped on exit
PWD=`pwd -P`
mkdir -p "$PWD/tmp"
TMPDIR=`mktemp -d -p $PWD/tmp`

function on_exit {
    echo "Removing $TMPDIR ..." >&2
    rm -rf "$TMPDIR"
}
trap on_exit EXIT

echo "Downloading \"$URL\" to $TMPDIR ..." >&2
wget -P "$TMPDIR" --no-verbose "$URL"

base=$(basename "$URL")
path="$TMPDIR/$base"

echo "Sampling $path ..." >&2
source venv/bin/activate
python sample_warc_responses.py -s "$SEED" -l "$LANGUAGE" "$RATIO" \
    "$path" "$OUT"

echo "Removing $path ..." >&2
rm -rf "$path"

echo "----------------------------------------------------------------------"
echo "END $SLURM_JOBID: $(date): $URL"
echo "----------------------------------------------------------------------"

echo "Finished samping $URL." >&2
echo `date` > ${OUT}.completed
