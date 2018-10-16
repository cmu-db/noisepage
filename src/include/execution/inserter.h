//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// inserter.h
//
// Identification: src/include/execution/inserter.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/compilation_context.h"
#include "execution/consumer_context.h"
#include "storage/storage_defs.h"

namespace terrier {
namespace transaction {
class TransactionContext;
}  // namespace transaction

namespace storage {
class DataTable;
class VarlenPool;
}  // namespace storage

namespace execution {

// This class handles insertion of tuples from generated code. This avoids
// passing along information through translators, and is intialized once
// through its Init() outside the main loop
class Inserter {
 public:
  // Initializes the instance
  void Init(storage::DataTable *table, executor::ExecutionContext *executor_context);

  // Allocate the storage area that is to be reserved
  char *AllocateTupleStorage();

  // Insert a tuple
  void Insert();

  // Finalize the instance
  void TearDown();

 private:
  // No external constructor
  Inserter() = default;

 private:
  // Provided by its insert translator
  storage::DataTable *table_ = nullptr;
  executor::ExecutionContext *executor_context_ = nullptr;
  storage::TupleSlot slot_;
  storage::VarlenPool *pool_;

 private:
  DISALLOW_COPY_AND_MOVE(Inserter);
};

}  // namespace execution
}  // namespace terrier
