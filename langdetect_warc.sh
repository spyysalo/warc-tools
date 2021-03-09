#!/bin/bash

set -euo pipefail

source venv/bin/activate

python langdetect_warc.py
