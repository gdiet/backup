#!/bin/sh
# Thanks to https://github.com/jfroche/docker-markdown
jq --slurp --raw-input '{"text": "\(.)", "mode": "markdown"}' < README.md | curl --data @- https://api.github.com/markdown > target/app/readme.html
