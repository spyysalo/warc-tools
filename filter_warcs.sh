#!/bin/bash

# Run filter_warc.py on all warc.gz files in directory using GREASY.

# Slurm account
ACCOUNT=project_2001426

# Maximum number of steps to run
MAX_STEPS=40000

set -euo pipefail

if [ $# -ne 3 ]; then
    echo "Usage: $0 ID-FILE INPUT-DIR OUTPUT-DIR" >&2
    exit 1
fi

IDS="$1"
INDIR="$2"
OUTDIR="$3"

INDIR=${INDIR%/}    # Remove trailing slash, if any

if [ ! -e "$IDS" ]; then
    echo "No such file or directory: $IDS"
    exit 1
fi

if [ -e "$OUTDIR" ]; then
    read -n 1 -r -p "Output directory $OUTDIR exists. Continue? [y/n] "
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
	echo "OK, filtering files missing from $OUTDIR."
    else
	echo "Exiting."
	exit 1
    fi
fi

module load greasy

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
set +e
count=0
skip=0
find "$INDIR" -name '*.warc.gz' | while read i; do
    if [ $count -gt $MAX_STEPS ]; then
	echo "MAX_STEPS ($MAX_STEPS) reached, skipping remaining" >&2
	break
    fi
    o="$OUTDIR"/$(dirname ${i#$INDIR/})/$(basename $i)
    if [ -e "$o" ]; then
	# echo "$o exists, skipping $i" >&2
	skip=$((skip+1))
    else
	echo "./filter_warc.sh $IDS $i $o"
	count=$((count+1))
    fi
done > $TASKLIST
set -e
if [ $skip -gt 0 ]; then
    echo "skipped $skip files with existing outputs." >&2
fi

sbatch-greasy $TASKLIST \
    --cores 1 \
    --nodes 10 \
    --time 5:00 \
    --account "$ACCOUNT"
