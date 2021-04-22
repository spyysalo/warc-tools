#!/bin/bash

# Run sample_warc_responses.py on all warc.gz files in a Common Crawl
# using GREASY.

# Base URL for crawls
BASEURL="https://commoncrawl.s3.amazonaws.com"

# Slurm account
ACCOUNT=project_2004153

# Maximum number of GREASY steps to run
MAX_STEPS=20000

INITIAL_RANDOM_SEED=6472
RANDOM_SEED_INCREMENT=163

set -euo pipefail

# Command-line arguments
if [ $# -lt 1 ]; then
    echo -e "Usage: $0 CRAWL-ID [SAMPLE-RATIO] [NODES] [OUTDIR] [LANGUAGE]" >&2
    echo -e "Example: $0 CC-MAIN-2021-04 0.1 10 sampled" >&2
    exit 1
fi

CRAWLID="$1"
RATIO="${2:-0.1}"
NODES="${3:-10}"
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

# Create temporary file for GREASY tasklist
PWD=`pwd -P`
mkdir -p "$PWD/tmp"
TASKLIST=`mktemp -p $PWD/tmp tasklist.XXX`

# Create tasklist
path_count=$(wc -l < "$TMPDIR/warc.paths")
echo "Creating tasklist $TASKLIST from $path_count paths ..." >&2
set +e
count=0
skip=0
seed=$INITIAL_RANDOM_SEED
while read p; do
    if [ $count -ge $MAX_STEPS ]; then
	echo "MAX_STEPS ($MAX_STEPS) reached, skipping remaining" >&2
	break
    fi
    url="$BASEURL/$p"
    # exclude common prefix and "/warc" suffix from output path
    dir=$(echo $(dirname "$p") | perl -pe 's|'"$prefix"'/?||; s|/warc$||')
    out="$OUTDIR/$dir/$(basename $p)"
    if [ -e "$out" ]; then
	# echo "$out exists, skipping $p" >&2
	skip=$((skip+1))
	if [ $((skip % 1000)) -eq 0 ]; then
	    echo "Skippped $skip ..." >&2
	fi
    else
	echo "./sample_warc_url.sh $RATIO $LANGUAGE $url $out $seed"
	count=$((count+1))
	seed=$((seed+RANDOM_SEED_INCREMENT))
	if [ $((count % 1000)) -eq 0 ]; then
	    echo "Processed $count ..." >&2
	fi
    fi
done < <(cat "$TMPDIR/warc.paths") > "$TASKLIST"

echo "Wrote tasklist with $count tasks, skipped $skip." >&2
if [ $count -eq 0 ]; then
    rm "$TASKLIST"
    echo "All done, exiting without tasklist." >&2
    exit 0
fi

JOB_TEMP=`mktemp -u greasy-job-XXX.sbatch`

module load greasy

sbatch-greasy $TASKLIST \
    --cores 1 \
    --nodes "$NODES" \
    --time 2:00:00 \
    --account "$ACCOUNT" \
    --file "$JOB_TEMP"

# Puhti-specific adjustment
perl -p -i -e 's/^(#SBATCH -p) small.*/$1 large/' "$JOB_TEMP"

echo "----------------------------------------"
echo " Wrote $JOB_TEMP, run the job with"
echo "     sbatch $JOB_TEMP"
echo "----------------------------------------"
