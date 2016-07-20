#!/bin/bash

set -x

VPC=vpc-5fc4203a
INSTANCE=m4.4xlarge
IAMROLE=s3full
THREADS=32
MACHINE=default
DISK_SIZE=32

if [ "$1" = "aws-machine" ]
then
    MACHINE=aws-machine;
    docker-machine create -d amazonec2 \
                   --amazonec2-vpc-id=$VPC \
                   --amazonec2-instance-type=$INSTANCE \
                   --amazonec2-iam-instance-profile=$IAMROLE \
                   --amazonec2-root-size=$DISK_SIZE \
                   $MACHINE;
fi

eval $(docker-machine env $MACHINE)

docker build -t process-gddp .
docker run -t --name process-gddp \
       -e AWS_ACCESS_KEY_ID \
       -e AWS_SECRET_ACCESS_KEY \
       process-gddp:latest python process.py $THREADS
docker logs process-gddp > process-gddp.log
docker rm process-gddp

if [ "$1" = "aws-machine" ]
then
    docker-machine stop $MACHINE
    docker-machine rm $MACHINE
fi
