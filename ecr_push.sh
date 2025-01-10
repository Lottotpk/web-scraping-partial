#!/bin/bash

# Check if at least one argument is passed
if [ $# -lt 1 ]; then
  echo "Usage: $0 tag environment(stg, prod)"
  exit 1
fi

# Assign the first argument to a variable
BUILD_TAG=$1
ENV=$2

# Check the ENV variable and set AWS_ACC accordingly
if [ "$ENV" == "stg" ]; then
  AWS_ACC="481671850139"
else
  AWS_ACC="060553409788"
fi

echo "BUILD_TAG: $BUILD_TAG, ENV: $ENV, AWS_ACC: $AWS_ACC"

aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin $AWS_ACC.dkr.ecr.ap-southeast-1.amazonaws.com
docker build --platform linux/amd64 -t broker-research .
docker tag broker-research $AWS_ACC.dkr.ecr.ap-southeast-1.amazonaws.com/broker-research:$BUILD_TAG
docker push $AWS_ACC.dkr.ecr.ap-southeast-1.amazonaws.com/broker-research:$BUILD_TAG