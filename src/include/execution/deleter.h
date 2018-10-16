#pragma once

#include <cstdint>

#include "common/macros.h"

namespace terrier {

namespace concurrency {
class TransactionContext;
}  // namespace concurrency

namespace execution {
class ExecutionContext;
}  // namespace execution

namespace storage {
class SqlTable;
class TupleSlot;
}  // namespace storage

namespace execution {

// This class handles deletion of tuples from generated code. It mainly exists
// to avoid passing along table information through translators. Instead, this
// class is initialized once (through Init()) outside the main loop.
class Deleter {
 public:
  // Constructor
  Deleter(storage::SqlTable *table, execution::ExecutionContext *executor_context);

  // Initializer this deleter instance using the provided transaction and table.
  // All tuples to be deleted occur within the provided transaction are from
  // the provided table
  static void Init(Deleter &deleter, storage::SqlTable *table, execution::ExecutionContext *executor_context);

  // Delete the tuple within the provided tile group ID (unique) at the provided
  // offset from the start of the tile group.
  void Delete(storage::TupleSlot slot);

 private:
  // The table the tuples are deleted from
  storage::SqlTable *const table_;

  // The executor context with which the current execution happens
  execution::ExecutionContext *const executor_context_;

 private:
  DISALLOW_COPY_AND_MOVE(Deleter);
};

}  // namespace execution
}  // namespace terrier
