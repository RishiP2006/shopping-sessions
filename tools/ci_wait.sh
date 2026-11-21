#!/bin/sh
# ci_wait.sh : wait for the latest bench run, print its key log lines
R=RishiP2006/shopping-sessions
id=$(gh run list -R $R --branch bench --limit 1 --json databaseId -q '.[0].databaseId')
until [ "$(gh run view $id -R $R --json status -q .status)" = "completed" ]; do sleep 10; done
gh run view $id -R $R --json conclusion,headSha -q '.conclusion + " " + .headSha'
gh run view $id -R $R --log 2>/dev/null | sed 's/^[^\t]*\t[^\t]*\t[^ ]* //' | grep -E "Model name|^CPU\(s\)|Thread\(s\)|L2|L3|fixtures passed|^MATCH|^DIFFER|== NCPU|wall|task-clock|cycles|instructions|branch-misses|cache-misses|elapsed|^ +[0-9]+\.[0-9]+%|rror" | cut -c1-180 | head -90
