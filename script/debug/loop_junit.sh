#/bin/bash

cnt=0
while [ true ]; do 
    echo $cnt
    timeout 10m python3 ../script/testing/junit/run_junit.py --build-type=debug --query-mode=simple | tee log_junit.log
    test $? -ne 0 && break
    cnt=$((cnt + 1))
done