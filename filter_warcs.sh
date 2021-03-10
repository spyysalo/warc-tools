#!/bin/bash

# Run filter_warc.py on all warc.gz files in directory using GREASY.

# Slurm account
ACCOUNT=project_2001426


set -euo pipefail

module load greasy

if [ $# -ne 3 ]; then
    echo "Usage: $0 ID-FILE INPUT-DIR OUTPUT-DIR" >&2
    exit 1
fi

IDS="$1"
INDIR="$2"
OUTDIR="$3"

INDIR=${INDIR%/}    # Remove trailing slash, if any

if [ -e "$OUTDIR" ]; then
    echo "Output directory $OUTDIR exists, not clobbering. Exiting." >&2
    exit 1
fi

# Recreate INDIR subdirectory structure containing warc.gz files in OUTDIR
find "$INDIR" -name '*.warc.gz' | while read i; do 
    echo $(dirname ${i#$INDIR/}); 
done | sort | uniq \
    | while read d; do
    echo "Creating directory $OUTDIR/$d"
    mkdir -p "$OUTDIR/$d"
done

# Create temporary file for tasklist
PWD=`pwd -P`
mkdir -p "$PWD/tmp"
TASKLIST=`mktemp -p $PWD/tmp tasklist.XXX`

# Create tasklist
find "$INDIR" -name '*.warc.gz' | while read i; do
    o="$OUTDIR"/$(dirname ${i#$INDIR/})/$(basename $i .warc.gz).tsv
    echo "./filter_warc.sh $IDS $i $o"
done > $TASKLIST

sbatch-greasy $TASKLIST \
    --cores 1 \
    --nodes 10 \
    --time 5:00 \
    --account "$ACCOUNT"
