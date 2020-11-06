// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// Modified from the Apache Arrow project for the Terrier project.

#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
#include "common/macros.h"
#include "loggers/loggers_util.h"

int main(int argc, char **argv) {
  noisepage::LoggersUtil::Initialize();

  // Benchmark Config Environment Variables
  // Check whether we are being passed environment variables to override configuration parameter
  // for this benchmark run.
  const char *env_num_threads = std::getenv(noisepage::ENV_NUM_THREADS);
  if (env_num_threads != nullptr) noisepage::BenchmarkConfig::num_threads = atoi(env_num_threads);

  const char *env_logfile_path = std::getenv(noisepage::ENV_LOGFILE_PATH);
  if (env_logfile_path != nullptr) noisepage::BenchmarkConfig::logfile_path = std::string_view(env_logfile_path);

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();

  noisepage::LoggersUtil::ShutDown();

  return 0;
}
