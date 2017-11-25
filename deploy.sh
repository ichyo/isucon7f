#!/bin/bash

CURRENT_BRANCH=$(git symbolic-ref --short HEAD)

for HOST in isu1 isu2 isu3; do
  ssh $HOST "source ~/.profile; /home/isucon/deploy.sh $CURRENT_BRANCH"
done

curl -X POST -d "payload={\"text\": \"deploy $CURRENT_BRANCH\"}" https://hooks.slack.com/services/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
