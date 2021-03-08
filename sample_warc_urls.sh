#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH -p medium
#SBATCH -t 36:00:00
#SBATCH --ntasks-per-node=1
#SBATCH --account=Project_2001426
#SBATCH -o logs/%j.out
#SBATCH -e logs/%j.err

RATIO="$1"
URLFILE="$2"
OUTDIR="$3"

source venv/bin/activate

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

cat "$URLFILE" | while read url; do
    echo "Downloading \"$url\" to $TMPDIR ..." >&2
    echo "wget -P \"$TMPDIR\" \"$url\"" >&2
    wget -P "$TMPDIR" "$url"
    base=$(basename "$url")
    path="$TMPDIR/$base"
    echo "Sampling $path ..." >&2
    python sample_warc_responses.py -v "$RATIO" "$path" "$OUTDIR/$base"
    echo "Removing $path ..." >&2
    rm -rf "$path"
done

echo "Finished." >&2
