#include "storage/access_observer.h"

#include "storage/block_compactor.h"

namespace noisepage::storage {
void AccessObserver::ObserveGCInvocation() {
  gc_epoch_++;
  for (auto it = last_touched_.begin(), end = last_touched_.end(); it != end;) {
    if (it->second + COLD_DATA_EPOCH_THRESHOLD < gc_epoch_) {
      compactor_->PutInQueue(it->first);
      it = last_touched_.erase(it);
    } else {
      ++it;
    }
  }
}

void AccessObserver::ObserveWrite(RawBlock *block) {
  // The compactor is only concerned with blocks that are already full. We assume that partially empty blocks are
  // always hot.
  if (block->GetInsertHead() == block->data_table_->accessor_.GetBlockLayout().NumSlots())
    last_touched_[block] = gc_epoch_;
}

}  // namespace noisepage::storage
