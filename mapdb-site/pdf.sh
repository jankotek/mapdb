#!/bin/bash

set -e
set -o pipefail

(cd doc && make latexpdf)

cp doc/_build/latex/MapDB.pdf down/mapdb-manual.pdf

rm doc/_build -rf
