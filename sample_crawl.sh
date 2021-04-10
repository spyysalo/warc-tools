#!/bin/bash

# Run sample_warc_responses.py on all warc.gz files in a Common Crawl
# using GREASY.

# Base URL for crawls
BASEURL="https://commoncrawl.s3.amazonaws.com"

# Slurm account
ACCOUNT=project_2001426

# Maximum number of GREASY steps to run
MAX_STEPS=10

INITIAL_RANDOM_SEED=6472
RANDOM_SEED_INCREMENT=163

set -euo pipefail

# Command-line arguments
if [ $# -lt 1 ]; then
    echo -e "Usage: $0 CRAWL-ID [SAMPLE-RATIO] [JOBS] [OUTDIR] [LANGUAGE]" >&2
    echo -e "Example: $0 CC-MAIN-2021-04 0.1 10 sampled" >&2
    exit 1
fi

CRAWLID="$1"
RATIO="${2:-0.1}"
JOBS="${3:-10}"
OUTDIR="${4:-sampled}"
LANGUAGE="${5:-any}"

if [ -e "$OUTDIR" ]; then
    read -n 1 -r -p "Output directory $OUTDIR exists. Continue? [y/n] "
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
	echo "OK, sampling files missing from $OUTDIR."
    else
	echo "Exiting."
	exit 1
    fi
fi

# Create temporary directory and make sure it's wiped on exit
PWD=`pwd -P`
mkdir -p "$PWD/tmp"
TMPDIR=`mktemp -d -p $PWD/tmp`

function on_exit {
    echo "Removing $TMPDIR ..." >&2
    rm -rf "$TMPDIR"
}
trap on_exit EXIT

# Download and unpack warc.paths.gz file
url="$BASEURL/crawl-data/$CRAWLID/warc.paths.gz"
echo "Downloading $url to $TMPDIR" >&2
wget -P "$TMPDIR" "$url"
gunzip "$TMPDIR/warc.paths.gz"

# Get longest common prefix of paths
prefix=$(python commonprefix.py --dir $TMPDIR/warc.paths)

# Recreate warc path directory structure in OUTDIR, excluding common
# prefix and "/warc" suffix
cat "$TMPDIR/warc.paths" | xargs dirname | \
    perl -pe 's|'"$prefix"'/?||; s|/warc$||' | sort | uniq | \
    while read p; do
    echo "Creating directory $OUTDIR/$p"
done

# Create temporary file for GREASY tasklist
PWD=`pwd -P`
mkdir -p "$PWD/tmp"
TASKLIST=`mktemp -p $PWD/tmp tasklist.XXX`

echo $TASKLIST

# Create tasklist
echo "Creating tasklist ..." >&2
set +e
count=0
seed=$INITIAL_RANDOM_SEED
cat "$TMPDIR/warc.paths" | while read p; do
    if [ $count -ge $MAX_STEPS ]; then
	echo "MAX_STEPS ($MAX_STEPS) reached, skipping remaining" >&2
	break
    fi
    url="$BASEURL/$p"
    # exclude common prefix and "/warc" suffix from output path
    dir=$(echo $(dirname "$p") | perl -pe 's|'"$prefix"'/?||; s|/warc$||')
    out="$OUTDIR/$dir/$(basename $p)"
    if [ -e "$out" ]; then
	echo "$out exists, skipping $p" >&2
    else
	echo "./sample_warc_url.sh $RATIO $LANGUAGE $url $out $seed"
	count=$((count+1))
	seed=$((seed+RANDOM_SEED_INCREMENT))
	if [ $((count % 1000)) -eq 0 ]; then
	    echo "Processed $count ..." >&2
	fi
    fi
done > $TASKLIST

module load greasy

sbatch-greasy $TASKLIST \
    --cores 1 \
    --nodes 10 \
    --time 60:00 \
    --account "$ACCOUNT"
