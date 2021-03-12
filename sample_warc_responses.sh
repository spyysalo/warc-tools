#!/bin/bash

set -euo pipefail

source venv/bin/activate

python sample_warc_responses.py "$@"
