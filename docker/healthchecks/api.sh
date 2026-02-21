#!/bin/sh
set -e
wget -qO- http://localhost:8000/health >/dev/null
