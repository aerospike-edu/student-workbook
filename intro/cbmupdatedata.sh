#!/bin/bash
# ------------------------------
# Throw a 90% READ, 10% UPADTE load using C benchmark tool
# -
asbench -h 127.0.0.1 -p 3000 -n test -s testset -k 100000 -latency "7,1" -S 1 -o S100 -w RU,90 -t 30 -z 8
