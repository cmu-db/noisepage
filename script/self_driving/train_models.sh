#!/bin/bash

set -e

# This script trains the OU/interference/forecast models for self-driving.
# The script must be run from the bin folder, e.g., noisepage/build/bin.
#
# Arguments:
#  --train-ou             : Train OU model.
#  --train-interference   : Train interference model.
#  --train-forecast       : Train forecast model.
#  --keep-output          : Keep noisepage.output and noisepage.exitcode around after training.
#
# Assumptions:
#  The following targets must have been built:
#   noisepage
#   execution_runners
# Temporaries:
#  noisepage.output
#  noisepage.exitcode
# Output (depends on training selections):
#  training_data_ou/
#  trained_model_ou/
#  training_data_interference/
#  trained_model_interference/
#  training_data_forecast/
#  trained_model_forecast/

# Quick hacky safety check.
FOLDER_NAME=$(basename "$(pwd)")
if [[ "$FOLDER_NAME" != "bin" ]]
then
  echo 'This script must be run from your bin folder!'
  exit 1
fi

# Parse any arguments.
TRAIN_OU=false
TRAIN_INTERFERENCE=false
TRAIN_FORECAST=false
KEEP_OUTPUT=false

while test $# -gt 0; do
  case "$1" in
    --train-ou)
      TRAIN_OU=true
      shift
      ;;
    --train-interference)
      TRAIN_INTERFERENCE=true
      shift
      ;;
    --train-forecast)
      TRAIN_FORECAST=true
      shift
      ;;
    --keep-output)
      KEEP_OUTPUT=true
      shift
      ;;
    *)
      break
      ;;
  esac
done

echo "Training models."
echo "--train-ou: ${TRAIN_OU}"
echo "--train-interference: ${TRAIN_INTERFERENCE}"
echo "--train-forecast: ${TRAIN_FORECAST}"

set -x

# Remove any old output files.
rm -f ./noisepage.output
rm -f ./noisepage.exitcode

# Create training data and model result folders.
if [ "$TRAIN_OU" = true ] ; then
  rm -rf ./training_data_ou
  rm -rf ./trained_model_ou
  mkdir -p ./training_data_ou
  mkdir -p ./trained_model_ou
fi
if [ "$TRAIN_INTERFERENCE" = true ] ; then
  rm -rf ./training_data_interference
  rm -rf ./trained_model_interference
  mkdir -p ./training_data_interference
  mkdir -p ./trained_model_interference
fi
if [ "$TRAIN_FORECAST" = true ] ; then
  rm -rf ./training_data_forecast
  rm -rf ./trained_model_forecast
  mkdir -p ./training_data_forecast
  mkdir -p ./trained_model_forecast
fi

if [ "$TRAIN_OU" = true ] ; then
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
fi

if [ "$TRAIN_INTERFERENCE" = true ] ; then
  # Generate training data for interference model.
  # This script runs TPC-C with pipeline metrics enabled, generating pipeline.csv.
  PYTHONPATH=../.. \
    python3 -m script.self_driving.forecasting.forecaster_standalone \
    --generate_data \
    --record_pipeline_metrics_with_counters \
    --pattern_iter=1
  mv pipeline.csv training_data_interference/pipeline.csv
fi

if [ "$TRAIN_FORECAST" = true ] ; then
  # The forecaster_standalone script runs TPC-C with query trace enabled to generate training data for forecasting.
  # --pattern_iter determines how many times to run a sequence of TPC-C phases.
  # --pattern_iter is set to 3 (magic number) to generate enough data for training and testing.
  PYTHONPATH=../.. \
    python3 -m script.self_driving.forecasting.forecaster_standalone \
    --generate_data \
    --pattern_iter=3
fi

# Start up NoisePage with model server and pilot. The pilot is strictly speaking only necessary for forecast.
(BUILD_ABS_PATH=../.. PYTHONPATH=../.. \
  ./noisepage --messenger_enable --model_server_enable --use-pilot-thread \
  --ou_model_save_path=./trained_model_ou/model_ou.pickle \
  --interference_model_save_path=./trained_model_interference/model_interference.pickle \
  --forecast_model_save_path=./trained_model_forecast/model_forecast.pickle \
  --startup_ddl_path=startup.sql \
  > noisepage.output 2>&1 \
  ; echo $? > ./noisepage.exitcode) &

(tail -f noisepage.output &) | grep -q 'NoisePage'

# Parse the NoisePage PID.
NOISEPAGE_PID=$(grep 'NoisePage' noisepage.output | grep -oP '(?<=PID=).*(?=\])')

# Tail the NoisePage output to stdout.
tail -f noisepage.output &
TAIL_PID=$!

# Train models.
if [ "$TRAIN_OU" = true ] ; then
  psql -h localhost -U noisepage -p 15721 -c "set ou_model_input_path='training_data_ou/';"
  psql -h localhost -U noisepage -p 15721 -c "set train_ou_model=true;"
fi
if [ "$TRAIN_INTERFERENCE" = true ] ; then
  psql -h localhost -U noisepage -p 15721 -c "set interference_model_input_path='training_data_interference/';"
  psql -h localhost -U noisepage -p 15721 -c "set train_interference_model=true;"
fi
if [ "$TRAIN_FORECAST" = true ] ; then
  psql -h localhost -U noisepage -p 15721 -c "set train_forecast_model=true;"
fi

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

if [ "$KEEP_OUTPUT" = false ] ; then
  rm ./noisepage.output
  rm ./noisepage.exitcode
fi

set +x

# Remind the caller what the options were.
echo "Finished training models."
echo "--train-ou: ${TRAIN_OU}"
echo "--train-interference: ${TRAIN_INTERFERENCE}"
echo "--train-forecast: ${TRAIN_FORECAST}"