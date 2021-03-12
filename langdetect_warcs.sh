#!/bin/bash

# Run langdetect_warc.py on all warc.gz files in directory using
# GREASY.

# Slurm account
ACCOUNT=project_2001426


set -euo pipefail

module load greasy

if [ $# -ne 2 ]; then
    echo "Usage: $0 INPUT-DIR OUTPUT-DIR" >&2
    exit 1
fi

INDIR="$1"
OUTDIR="$2"

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
    echo "./langdetect_warc.sh $i > $o"
done > $TASKLIST

sbatch-greasy $TASKLIST \
    --cores 1 \
    --nodes 20 \
    --time 15:00 \
    --account "$ACCOUNT"
