#!/bin/bash -i
if [ $OSTYPE = "linux" ]; then
    module load java/8/31
fi
java -Xmx1024m -cp .:/usr/local/Thrift/*:lib/ ComputeNode $1 $2
