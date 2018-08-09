#pragma once
#include <vector>
#include "common/typedefs.h"
#include "storage/storage_defs.h"
#include "common/object_pool.h"
#include "storage/tuple_access_strategy.h"

namespace terrier::storage {
class DataTable;
}
namespace terrier::transaction {
// TODO(Tianyu): Change to be significant larger than a single record could be
#define UNDO_BUFFER_SEGMENT_SIZE (1 << 15)

class UndoBufferSegment {
 public:
  bool HasBytesLeft(uint32_t size) {
    return end_ + size <= UNDO_BUFFER_SEGMENT_SIZE;
  }

  storage::DeltaRecord *Reserve(uint32_t size) {
    end_ += size;
    return reinterpret_cast<storage::DeltaRecord *>(bytes_ + end_);
  }

  void Reset() {
    end_ = 0;
  }

 private:
  friend class UndoBuffer;
  uint32_t end_ = 0;
  byte bytes_[UNDO_BUFFER_SEGMENT_SIZE];
};

// TODO(Tianyu): Not thread-safe. We can probably just allocate thread-local buffers if we ever want
// multiple workers on the same transaction.
class UndoBuffer {
 public:
  class Iterator {
   public:
    storage::DeltaRecord &operator*() const {
      return *reinterpret_cast<storage::DeltaRecord *>((*curr_segment_)->bytes_ + segment_offset_);
    }

    storage::DeltaRecord *operator->() const {
      return reinterpret_cast<storage::DeltaRecord *>((*curr_segment_)->bytes_ + segment_offset_);
    }

    Iterator &operator++() {
      storage::DeltaRecord &me = this->operator*();
      segment_offset_ += me.Size();
      if (segment_offset_ == (*curr_segment_)->end_) {
        // need to advance into the next segment
        ++curr_segment_;
        segment_offset_ = 0;
      }
      return *this;
    }

    const Iterator operator++(int) {
      Iterator copy = *this;
      operator++();
      return copy;
    }

    bool operator==(const Iterator &other) const {
      return segment_offset_ == other.segment_offset_
          && curr_segment_ == other.curr_segment_;
    }

    bool operator!=(const Iterator &other) const {
      return !(*this == other);
    }

   private:
    friend class UndoBuffer;
    Iterator(std::vector<UndoBufferSegment *>::iterator curr_segment, uint32_t segment_offset)
        : curr_segment_(curr_segment), segment_offset_(segment_offset) {}
    std::vector<UndoBufferSegment *>::iterator curr_segment_;
    uint32_t segment_offset_;
  };

  explicit UndoBuffer(common::ObjectPool<UndoBufferSegment> *buffer_pool) : buffer_pool_(buffer_pool) {}

  ~UndoBuffer() {
    for (auto *segment : buffers_)
      buffer_pool_->Release(segment);
  }

  Iterator Begin() {
    return {buffers_.begin(), 0};
  }

  Iterator End() {
    return {buffers_.end(), 0};
  }

 private:
  friend class TransactionContext;
  storage::DeltaRecord *NewEntry(const uint32_t size) {
    if (!buffers_.back()->HasBytesLeft(size)) {
      // we are out of space in the buffer. Get a new buffer segment.
      UndoBufferSegment *new_segment = buffer_pool_->Get();
      new_segment->Reset();
      buffers_.push_back(new_segment);
    }
    return buffers_.back()->Reserve(size);
  }

  common::ObjectPool<UndoBufferSegment> *buffer_pool_;
  std::vector<UndoBufferSegment *> buffers_;
};

class TransactionContext {
 public:
  TransactionContext(timestamp_t start, timestamp_t txn_id, common::ObjectPool<UndoBufferSegment> *buffer_pool)
      : start_time_(start), txn_id_(txn_id), undo_buffer_(buffer_pool) {}

  timestamp_t StartTime() const {
    return start_time_;
  }

  timestamp_t TxnId() const {
    return txn_id_;
  }

  UndoBuffer &GetUndoBuffer() {
    return undo_buffer_;
  }

  storage::DeltaRecord *UndoRecordForUpdate(storage::DataTable *table,
                                            storage::TupleSlot slot,
                                            const storage::ProjectedRow &redo) {
    uint32_t size = storage::DeltaRecord::Size(redo);
    storage::DeltaRecord *result = undo_buffer_.NewEntry(size);
    return storage::DeltaRecord::InitializeDeltaRecord(result, txn_id_, slot, table, redo);
  }

  // TODO(Tianyu): Whether this flips a slot back to being unallocated,
  // or logically deleted (and GC deallocate it) is up for debate
  storage::DeltaRecord *UndoRecordForInsert(storage::DataTable *table,
                                            const storage::BlockLayout &layout,
                                            storage::TupleSlot slot) {
    // TODO(Tianyu): Remove magic constant
    // Pretty sure we want 1, the primary key column?
    uint32_t size = storage::DeltaRecord::Size(layout, {1});
    storage::DeltaRecord *result = undo_buffer_.NewEntry(size);
    return storage::DeltaRecord::InitializeDeltaRecord(result, txn_id_, slot, table, layout, {1});
  }

 private:
  const timestamp_t start_time_;
  const timestamp_t txn_id_;
  UndoBuffer undo_buffer_;
};
} // namespace terrier::transaction