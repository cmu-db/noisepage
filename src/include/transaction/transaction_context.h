#pragma once
#include <vector>
#include "common/object_pool.h"
#include "common/typedefs.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"

namespace terrier::storage {
class DataTable;
}
namespace terrier::transaction {
// TODO(Tianyu): Size of this buffer can probably be optimized
// TODO(Tianyu): Put this into common::Constants?
#define UNDO_BUFFER_SEGMENT_SIZE (1 << 15)

// TODO(Tianyu): This code structure is probably generalizable to WAL, network, and a bunch
// of other places where we need resizable buffers that are reusable. So maybe at some point
// this can go live in common.
/**
 * An UndoBufferSegment is a piece of (reusable) memory used to hold undo records. The segment internally keeps track
 * of its memory usage.
 *
 * This class should not be used by itself. @see UndoBuffer.
 *
 * Not thread-safe.
 */
class UndoBufferSegment {
 public:
  /**
   * @param size the amount of bytes to check for
   * @return Whether this segment have enough space left for size many bytes
   */
  bool HasBytesLeft(uint32_t size) { return end_ + size <= UNDO_BUFFER_SEGMENT_SIZE; }

  /**
   * Reserve space for a delta record of given size to be written in this segment. The segment must have
   * enough space for that record.
   *
   * @param size the amount of bytes to reserve
   * @return pointer to the head of the allocated undo record
   */
  storage::DeltaRecord *Reserve(uint32_t size) {
    PELOTON_ASSERT(HasBytesLeft(size), "buffer segment allocation out of bounds");
    auto *result = reinterpret_cast<storage::DeltaRecord *>(bytes_ + end_);
    end_ += size;
    return result;
  }

  /**
   * Clears the buffer segment.
   */
  void Reset() { end_ = 0; }

 private:
  friend class UndoBuffer;
  byte bytes_[UNDO_BUFFER_SEGMENT_SIZE];
  uint32_t end_ = 0;
};

// TODO(Tianyu): Not thread-safe. We can probably just allocate thread-local buffers (or segments) if we ever want
// multiple workers on the same transaction.
/**
 * An UndoBuffer is a resizable buffer to hold UndoRecords.
 *
 * Pointers to UndoRecords and Iterators handed out from the UndoBuffer is guaranteed to be persistent. Adding new
 * entries to the buffer does not interfere with existing pointers and iterators.
 *
 * UndoRecords should not be removed from UndoBuffer until the transaction's end (only in the destructor)
 *
 * Not thread-safe
 */
class UndoBuffer {
 public:
  /**
   * Iterator to access DeltaRecords inside the undo buffer.
   *
   * Iterators are always guaranteed to be valid once handed out, provided that the UndoBuffer is not
   * destructed, regardless of modifications to the UndoBuffer.
   *
   * Not thread-safe
   */
  class Iterator {
   public:
    /**
     * @return reference to the underlying UndoRecord
     */
    storage::DeltaRecord &operator*() const {
      return *reinterpret_cast<storage::DeltaRecord *>((*curr_segment_)->bytes_ + segment_offset_);
    }

    /**
     * @return pointer to the underlying UndoRecord
     */
    storage::DeltaRecord *operator->() const {
      return reinterpret_cast<storage::DeltaRecord *>((*curr_segment_)->bytes_ + segment_offset_);
    }

    /**
     * prefix-increment
     * @return self-reference
     */
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

    /**
     * postfix-increment
     * @return iterator equal to this iterator before increment
     */
    const Iterator operator++(int) {
      Iterator copy = *this;
      operator++();
      return copy;
    }

    /**
     * Equality test
     * @param other iterator to compare to
     * @return if this is equal to other
     */
    bool operator==(const Iterator &other) const {
      return segment_offset_ == other.segment_offset_ && curr_segment_ == other.curr_segment_;
    }

    /**
     * Inequality test
     * @param other iterator to compare to
     * @return if this is not equal to other
     */
    bool operator!=(const Iterator &other) const { return !(*this == other); }

   private:
    friend class UndoBuffer;
    Iterator(std::vector<UndoBufferSegment *>::iterator curr_segment, uint32_t segment_offset)
        : curr_segment_(curr_segment), segment_offset_(segment_offset) {}
    std::vector<UndoBufferSegment *>::iterator curr_segment_;
    uint32_t segment_offset_;
  };

  /**
   * Constructs a new undo buffer, drawing its segments from the given buffer pool.
   * @param buffer_pool buffer pool to draw segments from
   */
  explicit UndoBuffer(common::ObjectPool<UndoBufferSegment> *buffer_pool) : buffer_pool_(buffer_pool) {}

  /**
   * Destructs this buffer, releases all its segments back to the buffer pool it draws from.
   */
  ~UndoBuffer() {
    for (auto *segment : buffers_) buffer_pool_->Release(segment);
  }

  /**
   * @return Iterator to the first element
   */
  Iterator begin() { return {buffers_.begin(), 0}; }

  /**
   * @return Iterator to the element following the last element
   */
  Iterator end() { return {buffers_.end(), 0}; }

 private:
  friend class TransactionContext;
  storage::DeltaRecord *NewEntry(const uint32_t size) {
    if (buffers_.empty() || !buffers_.back()->HasBytesLeft(size)) {
      // we are out of space in the buffer. Get a new buffer segment.
      UndoBufferSegment *new_segment = buffer_pool_->Get();
      PELOTON_ASSERT(reinterpret_cast<uintptr_t>(new_segment) % 8 == 0, "a delta entry should be aligned to 8 bytes");
      new_segment->Reset();
      buffers_.push_back(new_segment);
    }
    return buffers_.back()->Reserve(size);
  }

  common::ObjectPool<UndoBufferSegment> *buffer_pool_;
  std::vector<UndoBufferSegment *> buffers_;
};

/**
 * A transaction context encapsulates the information kept while the transaction is running
 */
class TransactionContext {
 public:
  /**
   * Constructs a new transaction context
   * @param start the start timestamp of the transaction
   * @param txn_id the id of the transaction, should be larger than all start time and commit time
   * @param buffer_pool the buffer pool to draw this transaction's undo buffer from
   */
  TransactionContext(timestamp_t start, timestamp_t txn_id, common::ObjectPool<UndoBufferSegment> *buffer_pool)
      : start_time_(start), txn_id_(txn_id), undo_buffer_(buffer_pool) {}

  /**
   * @return start time of this transaction
   */
  timestamp_t StartTime() const { return start_time_; }

  /**
   * @return id of this transaction
   */
  timestamp_t TxnId() const { return txn_id_; }

  /**
   * @return the undo buffer of this transaction
   */
  UndoBuffer &GetUndoBuffer() { return undo_buffer_; }

  /**
   * Reserve space on this transaction's undo buffer for a record to log the update given
   * @param table pointer to the updated DataTable object
   * @param slot the TupleSlot being updated
   * @param redo the content of the update
   * @return a persistent pointer to the head of a memory chunk large enough to hold the undo record
   */
  storage::DeltaRecord *UndoRecordForUpdate(storage::DataTable *table, storage::TupleSlot slot,
                                            const storage::ProjectedRow &redo) {
    uint32_t size = storage::DeltaRecord::Size(redo);
    storage::DeltaRecord *result = undo_buffer_.NewEntry(size);
    return storage::DeltaRecord::InitializeDeltaRecord(result, txn_id_, slot, table, redo);
  }

  /**
   * Reserve space on this transaction's undo buffer for a record to log the insert given
   * @param table pointer to the updated DataTable object
   * @param layout the layout of the insert target
   * @param slot the TupleSlot being updated
   * @return a persistent pointer to the head of a memory chunk large enough to hold the undo record
   */
  storage::DeltaRecord *UndoRecordForInsert(storage::DataTable *table, const storage::BlockLayout &layout,
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
}  // namespace terrier::transaction
