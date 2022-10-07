#!/bin/bash

list="compile.sh config_hugepage.sh allocator.cpp allocator.h global.h kv.cpp main.h selftest.cpp"

server=worker8


rsync -v -e ssh $list $server:~/KV/
