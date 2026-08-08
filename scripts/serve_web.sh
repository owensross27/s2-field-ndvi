#!/usr/bin/env bash
cd "$(dirname "$0")/../web" && exec ../.venv/bin/python -m RangeHTTPServer 8137
