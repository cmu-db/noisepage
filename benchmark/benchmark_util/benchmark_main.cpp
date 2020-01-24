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

#include <common/macros.h>

#include <iostream>

#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
#include "loggers/loggers_util.h"

int main(int argc, char **argv) {
  terrier::LoggersUtil::Initialize();

  // Check whether the environment variable is set to specify the number of
  // threads to use for this benchmark run.
  const char *env_num_threads = std::getenv(terrier::ENV_NUM_THREADS);
  terrier::BenchmarkConfig::num_threads_ = (env_num_threads != nullptr ? atoi(env_num_threads) : 1);
  std::cout << "# of Benchmark Threads: " << terrier::BenchmarkConfig::num_threads_ << std::endl;

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();

  terrier::LoggersUtil::ShutDown();

  return 0;
}
