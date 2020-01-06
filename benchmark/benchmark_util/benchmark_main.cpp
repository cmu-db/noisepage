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
#include "benchmark/benchmark.h"
#include "loggers/loggers_util.h"

int main(int argc, char **argv) {
  terrier::LoggersUtil::Initialize();

  const char *env_threads = std::getenv("TERRIER_BENCHMARK_CORES");
  if (env_threads == nullptr) {
  } else {
    benchmark::BENCHMARK_NUM_THREADS = atoi(env_threads);
  }

  TERRIER_ASSERT(benchmark::BENCHMARK_NUM_THREADS == 0, "Thread count not set.");
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();

  terrier::LoggersUtil::ShutDown();

  return 0;
}
