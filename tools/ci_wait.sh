#!/bin/sh
# ci_wait.sh : wait for the latest bench run, print key results
R=RishiP2006/shopping-sessions
L=${SCRATCH:-/tmp}/ci.log
id=$(gh run list -R $R --branch bench --limit 1 --json databaseId -q '.[0].databaseId')
until [ "$(gh run view $id -R $R --json status -q .status)" = "completed" ]; do sleep 10; done
gh run view $id -R $R --json conclusion,headSha -q '.conclusion + " " + .headSha'
gh run view $id -R $R --log > $L 2>&1
grep -E "fixtures passed|MATCH|DIFFER|base=|== SRCLINE| wall |task-clock|  [0-9]+\.[0-9]+%  " $L | grep -v '\[36;1m' | sed -E 's/^.*Z //' | cut -c1-150 | head -130
