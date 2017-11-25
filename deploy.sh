#!/bin/bash

CURRENT_BRANCH=$(git symbolic-ref --short HEAD)

for HOST in isu1 isu2 isu3; do
  ssh $HOST "source ~/.profile; /home/isucon/deploy.sh $CURRENT_BRANCH"
done
