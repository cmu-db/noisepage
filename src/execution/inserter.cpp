//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// inserter.cpp
//
// Identification: src/execution/inserter.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/inserter.h"
#include "common/container_tuple.h"
#include "concurrency/transaction_manager_factory.h"
#include "execution/transaction_runtime.h"
#include "executor/executor_context.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "storage/data_table.h"
#include "storage/tile.h"
#include "storage/tile_group.h"

namespace terrier::execution {

void Inserter::Init(storage::DataTable *table, executor::ExecutionContext *executor_context) {
  TERRIER_ASSERT(table && executor_context, "Arguments should not be nullptr");
  table_ = table;
  executor_context_ = executor_context;
}

char *Inserter::AllocateTupleStorage() {
  location_ = table_->GetEmptyTupleSlot(nullptr);

  // Get the tile offset assuming that it is a row store
  auto tile_group = table_->GetTileGroupById(location_.block);
  auto layout = tile_group->GetLayout();
  PELOTON_ASSERT(layout.IsRowStore());
  // layout is still a row store. Hence tile offset it 0
  tile_ = tile_group->GetTileReference(0);
  return tile_->GetTupleLocation(location_.offset);
}

peloton::type::AbstractPool *Inserter::GetPool() {
  // This should be called after AllocateTupleStorage()
  PELOTON_ASSERT(tile_);
  return tile_->GetPool();
}

void Inserter::Insert() {
  PELOTON_ASSERT(table_ && executor_context_ && tile_);
  auto *txn = executor_context_->GetTransaction();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  ContainerTuple<storage::TileGroup> tuple(table_->GetTileGroupById(location_.block).get(), location_.offset);
  ItemPointer *index_entry_ptr = nullptr;
  bool result = table_->InsertTuple(&tuple, location_, txn, &index_entry_ptr);
  if (result == false) {
    txn_manager.SetTransactionResult(txn, ResultType::FAILURE);
    return;
  }
  txn_manager.PerformInsert(txn, location_, index_entry_ptr);
  executor_context_->num_processed++;
}

void Inserter::TearDown() {
  // Updater object does not destruct its own data structures
  tile_.reset();
}

}  // namespace terrier::execution
