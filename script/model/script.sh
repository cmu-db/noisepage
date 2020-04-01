#!/bin/bash

rm -rf mini_runner_model_results/*
rm -rf trained_model/*
rm -rf stdout.txt

python3 mini_trainer.py 2>&1 | tee stdout.txt

current_time=$(date "+%Y%m%d%H%M%S")
current_time="archive_$current_time"
mkdir $current_time
cp -r mini_runner_model_results $current_time
cp -r trained_model $current_time
cp stdout.txt $current_time
