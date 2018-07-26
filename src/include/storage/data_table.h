#pragma once
#include <vector>
#include "common/concurrent_map.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"

namespace terrier::storage {
// TODO(Tianyu): Move this
class TransactionContext;

class DeltaRecord {
 private:
  DeltaRecord *next_;
  timestamp_t timestamp_;
  ProjectedRow delta_;
};

class DataTable {
 public:
  void Select(TransactionContext *txn, TupleSlot slot, ProjectedRow *buffer,
              const std::vector<uint16_t> &projection_list) {
    RawBlock *block = slot.GetBlock();
    auto it = layouts_.Find(block->layout_version_);
    PELOTON_ASSERT(it != layouts_.End());
    
  }

  TupleSlot Insert(TransactionContext *txn, const ProjectedRow &redo) { return TupleSlot(); }

  void Update(TransactionContext *txn, TupleSlot slot, const ProjectedRow &redo, const DeltaRecord &undo) {}

 private:
  ConcurrentMap<layout_version_t, TupleAccessStrategy> layouts_;
};
}  // namespace terrier::storage