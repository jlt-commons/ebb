#!/usr/bin/env bash
# Assertions this project's documentation build must satisfy.
#
# Run by the shared site workflow in jlt-commons/ci-builds against the freshly
# built _site, with BASE_PATH exported.
#
# Run it locally the same way, from a docs-engine checkout:
#   bb build <path to this repo> && cd <this repo> \
#     && BASE_PATH=/ebb bash docs/check-site.sh

set -euo pipefail
out=_site

test -f "$out/index.html"                      || { echo "no homepage generated"; exit 1; }
test -f "$out/guide/index.html"                 || { echo "no guide index generated"; exit 1; }
test -f "$out/guide/evaluation.html"            || { echo "evaluation.md did not build"; exit 1; }
test -f "$out/guide/conformance.html"           || { echo "conformance.md did not build"; exit 1; }
test -f "$out/guide/adr/001-fiber-affinity.html" || { echo "the ADR did not build"; exit 1; }
test -f "$out/css/screen.css"                   || { echo "static assets missing"; exit 1; }

# docs/guide/{evaluation,conformance}.md and docs/guide/adr/001-fiber-affinity.md
# are symlinks into doc/ - the real files test/ebb/conformance_test.clj and a
# dozen src/ebb/impl/*.clj comments point at by that path. If a symlink ever
# goes stale (target renamed, doc/ restructured), the build still succeeds
# (a broken symlink just fails to slurp) but silently, so check the rendered
# content actually has weight rather than only checking the file exists.
for page in evaluation conformance; do
  bytes=$(wc -c < "$out/guide/$page.html")
  test "$bytes" -gt 2000 || { echo "guide/$page.html is suspiciously small ($bytes bytes) - stale symlink?"; exit 1; }
done

! grep -rq '{{site-base}}' "$out"/index.html "$out"/404.html "$out"/guide/*.html "$out"/guide/adr/*.html \
  || { echo "unrendered template variable"; exit 1; }

# The failure mode this site's base path exists to prevent. Served at
# jlt-commons.github.io/ebb/, a root-absolute URL loads the ORGANIZATION
# site's asset instead of this project's. The page still renders, wearing
# the wrong clothes, so nothing but a check catches it.
if grep -ohE '(href|src)="/[^"]*"' "$out"/index.html "$out"/404.html "$out"/guide/*.html "$out"/guide/adr/*.html \
     | grep -vE "=\"$BASE_PATH/"; then
  echo "the URLs above escape $BASE_PATH and would resolve against the org site"
  exit 1
fi

echo "build looks correct: index, guide overview, evaluation, conformance and the ADR all present, every URL under $BASE_PATH"
