#!/bin/bash

set -e

# This script trains the OU and interference models for self-driving.
# The script must be run from the bin folder, e.g., noisepage/build/bin.
#
# Assumptions:
#  The following targets must have been built:
#   noisepage
#   execution_runners
# Temporaries:
#  noisepage.output
#  noisepage.exitcode
# Output:
#  training_data_ou/
#  trained_model_ou/
#  training_data_interference/
#  trained_model_interference/

# Quick hacky safety check.
FOLDER_NAME=$(basename "$(pwd)")
if [[ "$FOLDER_NAME" == "bin" ]]
then
  echo 'Training models. Please read the script header comment for details.'
else
  echo 'This script must be run from your bin folder!'
  exit 1
fi

set -x

# Remove any old output files.
rm -f ./noisepage.output
rm -f ./noisepage.exitcode
rm -rf ./training_data_ou
rm -rf ./trained_model_ou
rm -rf ./training_data_interference
rm -rf ./trained_model_interference

# Create training data and model result folders.
mkdir -p ./training_data_ou
mkdir -p ./trained_model_ou
mkdir -p ./training_data_interference
mkdir -p ./trained_model_interference

# Generate training data for OU model.
# This script runs the execution_runners target, which generates *SEQ*.csv files.
# The parameters to the execution_runners target are arbitrarily picked to complete tests within 10 minutes while
# still exercising all OUs and generating a reasonable amount of training data.
#
# Specifically, the parameters chosen are:
# - execution_runner_rows_limit=100, which sets the max number of rows/tuples processed to be 100 (small table).
# - rerun=0, which skips rerun since we are not testing benchmark performance here.
# - warm_num=1, which also tests the warm up phase for the execution_runners.
../benchmark/execution_runners --execution_runner_rows_limit=100 --rerun=0 --warm_num=1
mv *SEQ*.csv training_data_ou

# Generate training data for interference model.
# This script runs TPC-C with pipeline metrics enabled, generating pipeline.csv.
PYTHONPATH=../.. \
  python3 -m script.self_driving.forecasting.forecaster_standalone \
  --generate_data \
  --record_pipeline_metrics_with_counters \
  --pattern_iter=1
mv pipeline.csv training_data_interference/pipeline.csv

# Start up NoisePage with model server.
(BUILD_ABS_PATH=../.. PYTHONPATH=../.. \
  ./noisepage --messenger_enable --model_server_enable \
  --ou_model_save_path=./trained_model_ou/model_ou.pickle \
  --interference_model_save_path=./trained_model_interference/model_interference.pickle \
  > noisepage.output 2>&1 \
  ; echo $? > ./noisepage.exitcode) &

# Unfortunately, the model server connection happens AFTER the NoisePage startup message.
# If you don't wait for the ModelServer connection, it appears that training silently fails.
(tail -f noisepage.output &) | grep -q "ModelServer connected"

# Parse the NoisePage PID.
NOISEPAGE_PID=$(grep 'NoisePage' noisepage.output | grep -oP '(?<=PID=).*(?=\])')

# Tail the NoisePage output to stdout.
tail -f noisepage.output &
TAIL_PID=$!

# Train models.
psql -h localhost -U noisepage -p 15721 -c "set ou_model_input_path='training_data_ou/';"
psql -h localhost -U noisepage -p 15721 -c "set interference_model_input_path='training_data_interference/';"
psql -h localhost -U noisepage -p 15721 -c "set train_ou_model=true;"
psql -h localhost -U noisepage -p 15721 -c "set train_interference_model=true;"

# Clean up.
kill $NOISEPAGE_PID

# Wait for NoisePage to shut down.
while [ ! -f ./noisepage.exitcode ]
do
  sleep 1
done

# Print NoisePage exitcode, clean up.
echo "NoisePage exitcode: $(cat noisepage.exitcode)"
kill $TAIL_PID
rm ./noisepage.output
rm ./noisepage.exitcode

