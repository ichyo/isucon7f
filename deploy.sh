#!/bin/bash

BRANCH=$1

for HOST in isu1 isu2 isu3; do
  ssh $HOST "source ~/.profile; /home/isucon/deploy.sh $BRANCH"
done
