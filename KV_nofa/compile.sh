#!/bin/bash
DIR=`cd $(dirname $0); pwd`
$DIR/config_hugepage.sh
g++ $DIR/selftest.cpp $DIR/allocator.cpp -Wfatal-errors -Wall -Wextra -Wshadow -Wno-unused -o $DIR/selftest -O3

g++ $DIR/mttest.cpp $DIR/allocator.cpp -O3 -o $DIR/mttest -lpthread
