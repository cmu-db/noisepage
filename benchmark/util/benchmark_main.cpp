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
#include "loggers/catalog_logger.h"
#include "loggers/index_logger.h"
#include "loggers/main_logger.h"
#include "loggers/storage_logger.h"
#include "loggers/transaction_logger.h"

int main(int argc, char **argv) {
  // initialize loggers
  init_main_logger();
  terrier::storage::init_index_logger();
  terrier::storage::init_storage_logger();
  terrier::transaction::init_transaction_logger();
  terrier::catalog::init_catalog_logger();

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();

  // shutdown loggers
  spdlog::shutdown();
  return 0;
}
