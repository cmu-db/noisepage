//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_runtime.h
//
// Identification: src/include/execution/transaction_runtime.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include "storage/data_table.h"

namespace terrier {

namespace transaction {
class TransactionContext;
}  // namespace transaction

namespace execution {
class ExecutionContext;
}  // namespace execution

// TODO(Tianyu): Remove
namespace storage {
class TileGroup;
class TileGroupHeader;
}  // namespace storage

namespace type {
class Value;
}  // namespace type

namespace execution {
//===----------------------------------------------------------------------===//
// This class contains common runtime functions needed during query execution.
// These functions are used exclusively by the codegen component.
//===----------------------------------------------------------------------===//
class TransactionRuntime {
 public:
  // Perform a visibility check for all tuples in the given tile group with IDs
  // in the range [tid_start, tid_end) in the context of the given transaction
  // TODO(Tianyu): This is no longer useful in the new system since the new data table hand out visible tuples only
  static uint32_t PerformVisibilityCheck(transaction::TransactionContext &txn, storage::TileGroup &tile_group,
                                         uint32_t tid_start, uint32_t tid_end, uint32_t *selection_vector);

  // Perform a read operation for all tuples in the given tile group with IDs
  // in the range [tid_start, tid_end) in the context of the given transaction
  // TODO(Tianyu): Again, no longer relevant
  static uint32_t PerformVectorizedRead(transaction::TransactionContext &txn, storage::TileGroup &tile_group,
                                        uint32_t *selection_vector, uint32_t end_idx, bool is_for_update);
  // Check Ownership
  static bool IsOwner(concurrency::TransactionContext &txn, storage::TileGroupHeader *tile_group_header,
                      uint32_t tuple_offset);
  // Acquire Ownership
  static bool AcquireOwnership(concurrency::TransactionContext &txn, storage::TileGroupHeader *tile_group_header,
                               uint32_t tuple_offset);
  // Yield Ownership
  // Note: this should be called when failed after acquired ownership
  //       otherwise, ownership is yielded inside transaction functions
  static void YieldOwnership(concurrency::TransactionContext &txn, storage::TileGroupHeader *tile_group_header,
                             uint32_t tuple_offset);
};

}  // namespace execution
}  // namespace terrier
