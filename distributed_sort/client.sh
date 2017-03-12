#!/bin/bash -i
if [ $OSTYPE = "linux" ]; then
    module load java/8/31
fi
java -cp .:/usr/local/Thrift/*:lib/ Client $1 $2