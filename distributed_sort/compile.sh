#!/bin/bash -i
if [ $OSTYPE = "linux" ]; then
    module load java/8/31
fi
javac -g -cp ".:/usr/local/Thrift/*" lib/*.java
