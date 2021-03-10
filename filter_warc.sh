#!/bin/bash

set -euo pipefail

source venv/bin/activate

python filter_warc.py "$@"
