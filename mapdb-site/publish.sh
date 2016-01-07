#!/bin/bash

set -e
set -o pipefail

(cd doc && make latexpdf)

rm down/mapdb-manual.pdf || true
cp doc/_build/latex/MapDB.pdf down/mapdb-manual.pdf

rm doc/_build -rf



make clean html

cd ../gh-pages
git pull

cp ../mapdb-site/_build/html/* . -rv
rm news.xml
cp blog/atom.xml news.xml -rv

git add -A
git commit -m "update site"
git push


cd ../mapdb-site
