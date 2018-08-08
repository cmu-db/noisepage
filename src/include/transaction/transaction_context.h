#pragma once
#include <vector>
#include "common/typedefs.h"
#include "storage/storage_defs.h"
#include "common/object_pool.h"

namespace terrier::transaction {
// TODO(Tianyu): Change to be significant larger than a single record could be
#define UNDO_BUFFER_SEGMENT_SIZE 1024

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
      segment_offset_ += me.size_;
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
  
  UndoBuffer() {
    for (auto *segment : buffers_)
      buffer_pool_->Release(segment);
  }

  storage::DeltaRecord *NewDeltaRecord(const storage::BlockLayout &layout, const std::vector<uint16_t> &col_ids) {
    uint32_t required_size = storage::DeltaRecord::Size(layout, col_ids);
    if (!buffers_.back()->HasBytesLeft(required_size)) {
      // we are out of space in the buffer. Get a new buffer segment.
      UndoBufferSegment *new_segment = buffer_pool_->Get();
      new_segment->Reset();
      buffers_.push_back(new_segment);
    }
    return buffers_.back()->Reserve(required_size);
  }

  Iterator Begin() {
    return {buffers_.begin(), 0};
  }

  Iterator End() {
    return {buffers_.end(), 0};
  }

 private:
  common::ObjectPool<UndoBufferSegment> *buffer_pool_;
  std::vector<UndoBufferSegment *> buffers_;
};

class TransationContext {
 public:
  timestamp_t StartTime() const {
    return start_time_;
  }

  timestamp_t TxnId() const {
    return txn_id_;
  }

  UndoBuffer &GetUndoBuffer() {
    return undo_buffer_;
  }
 private:
  const timestamp_t start_time_{0};
  const timestamp_t txn_id_{0};
  UndoBuffer undo_buffer_;
};
}