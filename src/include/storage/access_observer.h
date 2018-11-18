#pragma once

#include <map>
#include <unordered_map>
#include <utility>
#include "storage/storage_defs.h"

namespace terrier::storage {
class DataTable;
class BlockCompactor;
// TODO(Tianyu): Probably need to be smarter than this to identify true hot or cold data, but this
// will do for now.
#define COLD_DATA_EPOCH_THRESHOLD 10
class AccessObserver {
 public:
  explicit AccessObserver(BlockCompactor *compactor) : compactor_(compactor) {}
  void ObserveGCInvocation();
  void ObserveWrite(DataTable *table, TupleSlot slot);

 private:
  uint64_t gc_epoch_ = 0; // estimate time using the number of times GC has run
  // TODO(Tianyu): This is hardly a space efficient representation of blocks. However this should do
  // since we assume that only a small portion of the database will be hot
  // Here RawBlock * should suffice as a unique identifier of the block. Although a block can be
  // reused, that process should only be triggered through compaction, which happens only if the
  // reference to said block is identified as cold and leaves the table.
  std::unordered_map<RawBlock *, uint64_t> last_touched_;
  // Sorted table references by epoch. We can easily do range scans on this data structure to get
  // cold blocks given current epoch and some threshold for a block to be cold.
  std::map<uint64_t, std::unordered_map<RawBlock *, DataTable *>> table_references_by_epoch_;
  BlockCompactor *const compactor_;
};

}  // namespace terrier::storage