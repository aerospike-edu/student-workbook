#!/bin/bash
# ------------------------------
# Load test data using C benchmark
# -
asbench -h 127.0.0.1 -p 3000 -n test -s testset -k 100000 --latency -S 1 -o S100 -w I -z 8 --output-file ./myrun1.out
