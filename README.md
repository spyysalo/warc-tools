# warc-tools

Tools for working with Web ARChive files, based in part on
<https://github.com/sronnqvist/commoncrawl-fetch>.

Written to work in the [CSC](https://www.csc.fi/) puhti/mahti
environment, will very likely require modification to run in other
environments.

## Quickstart

Set up environment

```
python3 -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt
```

Sample 10% of `CC-MAIN-2021-04` crawl with 100 parallel jobs with
output into `10-percent-sample`.

```
./sample_crawl.sh CC-MAIN-2021-04 0.1 100 10-percent-sample
```

## Language detection

Run trafilatura text extraction and langdetect language extraction
on `.warc.gz` files in directory `10-percent-sample` with output
to directory `10-percent-sample-langdetect`. Requires
[GREASY](https://github.com/BSC-Support-Team/GREASY).

```
./langdetect_warcs.sh 10-percent-sample 10-percent-sample-langdetect
```
