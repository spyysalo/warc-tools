#!/bin/bash

# Run sample_warc_text.py on all warc.gz files in directory using GREASY.

# Slurm account
ACCOUNT=project_2004407    # FinnGen-data
#ACCOUNT=project_2004153    # From Common Crawl to clean web data

# Maximum number of GREASY steps to run
MAX_STEPS=200000

set -euo pipefail

if [ $# -ne 2 ]; then
    echo "Usage: $0 INPUT-DIR OUTPUT-DIR" >&2
    exit 1
fi

INDIR="$1"
OUTDIR="$2"

INDIR=${INDIR%/}    # Remove trailing slash, if any

if [ -e "$OUTDIR" ]; then
    read -n 1 -r -p "Output directory $OUTDIR exists. Continue? [y/n] "
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
	echo "OK, extracting for files missing from $OUTDIR."
    else
	echo "Exiting."
	exit 1
    fi
fi

# Create temporary file for tasklist
PWD=`pwd -P`
mkdir -p "$PWD/tmp"
TASKLIST=`mktemp -p $PWD/tmp tasklist.XXX`

# Create tasklist
count=0
skip=0
while read i; do
    if [ $count -ge $MAX_STEPS ]; then
	echo "MAX_STEPS ($MAX_STEPS) reached, skipping remaining" >&2
	break
    fi
    o="$OUTDIR"/$(dirname ${i#$INDIR/})/$(basename $i .warc.gz).tsv
    if [ -e "$o" ]; then
	# echo "$o exists, skipping $i" >&2
	skip=$((skip+1))
	if [ $((skip % 1000)) -eq 0 ]; then
	    echo "Skippped $skip ..." >&2
	fi
    else
	echo "./extract_text.sh $i $o"
	count=$((count+1))
    fi
done < <(find "$INDIR" -name '*.warc.gz') > "$TASKLIST"

echo "Wrote tasklist with $count tasks, skipped $skip." >&2
if [ $count -eq 0 ]; then
    rm "$TASKLIST"
    echo "All done, exiting without tasklist." >&2
    exit 0
fi

module load greasy

sbatch-greasy $TASKLIST \
    --cores 1 \
    --nodes 10 \
    --time 1:00 \
    --account "$ACCOUNT"
