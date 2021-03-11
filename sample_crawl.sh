#!/bin/bash

set -euo pipefail

BASEURL="https://commoncrawl.s3.amazonaws.com"


# Command-line arguments
if [ $# -lt 1 ]; then
    echo -e "Usage: $0 CRAWL-ID [SAMPLE-RATIO] [JOBS] [OUTDIR]" >&2
    echo -e "Example: $0 CC-MAIN-2021-04 0.1 10 sampled" >&2
    exit 1
fi

CRAWLID="$1"
RATIO="${2:-0.1}"
JOBS="${3:-10}"
OUTDIR="${4:-sampled}"

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


# Download and warc.paths.gz file
url="$BASEURL/crawl-data/$CRAWLID/warc.paths.gz"
echo "Downloading $url to $TMPDIR" >&2
wget -P "$TMPDIR" "$url"

# Unpack and expand paths into URLs
gunzip "$TMPDIR/warc.paths.gz"
perl -pe "s|^|$BASEURL/|" "$TMPDIR/warc.paths" > "$TMPDIR/warc.urls"

# Unpack and split
total=$(wc -l < "$TMPDIR/warc.urls")
size=$(((total+JOBS-1)/JOBS))
split -l "$size" -d -a 4 "$TMPDIR/warc.urls" "$TMPDIR/warc.urls.part."

jobids=""
mkdir -p jobs
for urlfile in "$TMPDIR/warc.urls.part".*; do
    base=$(basename "$urlfile")
    split=${base#warc.urls.part.}
    jobid=$(sbatch sample_warc_urls.sh "$RATIO" "$urlfile" "$OUTDIR/$split" \
	| perl -pe 's/Submitted batch job //')
    echo "Started job $jobid" >&2
    jobids="$jobids $jobid"
    touch "jobs/$jobid"
    sleep 1
done

# Wait until all jobs are complete.
while true; do
    sleep 60
    not_finished=0
    for jobid in $jobids; do
	if [ -e "jobs/$jobid" ]; then
	    not_finished=$((not_finished+1))
	fi
    done
    if [ $not_finished -gt 0 ]; then
	echo "$not_finished/$JOBS jobs not finished ..." >&2
    else
	echo "All jobs finished." >&2
	break    # all done
    fi
done
