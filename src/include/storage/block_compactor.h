#pragma once
#include <forward_list>
#include <utility>
#include <cstdio>
#include "storage/storage_defs.h"
#include "storage/data_table.h"
namespace terrier::storage {

class BlockCompactor {
 public:
  void ProcessCompactionQueue() {
    std::forward_list<std::pair<RawBlock *, DataTable *>> to_process = std::move(compaction_queue_);
    for (auto &entry : to_process) {
      (void) entry;
      foo_++;
    }
  }

  void PutInQueue(const std::pair<RawBlock *, DataTable *> &entry) {
    compaction_queue_.push_front(entry);
  }

 private:
  std::forward_list<std::pair<RawBlock *, DataTable *>> compaction_queue_;
  volatile uint64_t foo_;
};
}  // namespace terrier::storage
