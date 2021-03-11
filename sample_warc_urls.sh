#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH -p small
#SBATCH -t 48:00:00
#SBATCH --ntasks-per-node=1
#SBATCH --account=Project_2001426
#SBATCH -o logs/%j.out
#SBATCH -e logs/%j.err

INITIAL_RANDOM_SEED=6472
RANDOM_SEED_INCREMENT=163

set -euo pipefail

RATIO="$1"
URLFILE="$2"
OUTDIR="$3"

mkdir -p "$OUTDIR"

# Create temporary directory and make sure it's wiped on exit
PWD=`pwd -P`
mkdir -p "$PWD/tmp"
TMPDIR=`mktemp -d -p $PWD/tmp`

function on_exit {
    echo "Removing $TMPDIR ..." >&2
    rm -rf "$TMPDIR"
    echo "Removing jobs/$SLURM_JOBID ..." >&2
    rm -f jobs/$SLURM_JOBID
}
trap on_exit EXIT

source venv/bin/activate

seed=$INITIAL_RANDOM_SEED
cat "$URLFILE" | while read url; do
    base=$(basename "$url")
    path="$TMPDIR/$base"
    out="$OUTDIR/$base"
    if [ -s "$out" ]; then
	echo "$out exists, skipping $url ..." >&2
	continue
    fi
    echo "Downloading \"$url\" to $TMPDIR ..." >&2
    wget -P "$TMPDIR" -nv "$url"
    echo "Sampling $path ..." >&2
    python sample_warc_responses.py -v -s $seed "$RATIO" "$path" "$out"
    echo "Removing $path ..." >&2
    rm -rf "$path"
    seed=$((seed+RANDOM_SEED_INCREMENT))
done

echo "Finished." >&2
