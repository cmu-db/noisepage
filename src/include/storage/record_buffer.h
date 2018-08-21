#pragma once
#include <vector>
#include "common/constants.h"
#include "common/object_pool.h"
#include "common/typedefs.h"
#include "storage/delta_record.h"

namespace terrier::storage {

/**
 * An UndoBufferSegment is a piece of (reusable) memory used to hold undo records. The segment internally keeps track
 * of its memory usage.
 *
 * This class should not be used by itself. @see UndoBuffer.
 *
 * Not thread-safe.
 */
class BufferSegment {
 public:
  /**
   * @param size the amount of bytes to check for
   * @return Whether this segment have enough space left for size many bytes
   */
  bool HasBytesLeft(uint32_t size) const { return end_ + size <= common::Constants::BUFFER_SEGMENT_SIZE; }

  /**
   * Reserve space for a delta record of given size to be written in this segment. The segment must have
   * enough space for that record.
   *
   * @param size the amount of bytes to reserve
   * @return pointer to the head of the allocated record
   */
  byte *Reserve(uint32_t size) {
    PELOTON_ASSERT(HasBytesLeft(size), "buffer segment allocation out of bounds");
    auto *result = bytes_ + end_;
    end_ += size;
    return result;
  }

  /**
   * Clears the buffer segment.
   */
  void Reset() { end_ = 0; }

 private:
  friend class UndoBuffer;
  byte bytes_[common::Constants::BUFFER_SEGMENT_SIZE];
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
   * Iterator to access UndoRecords inside the undo buffer.
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
    UndoRecord &operator*() const {
      return *reinterpret_cast<UndoRecord *>((*curr_segment_)->bytes_ + segment_offset_);
    }

    /**
     * @return pointer to the underlying UndoRecord
     */
    UndoRecord *operator->() const {
      return reinterpret_cast<UndoRecord *>((*curr_segment_)->bytes_ + segment_offset_);
    }

    /**
     * prefix-increment
     * @return self-reference
     */
    Iterator &operator++() {
      UndoRecord &me = this->operator*();
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
    Iterator(std::vector<BufferSegment *>::iterator curr_segment, uint32_t segment_offset)
        : curr_segment_(curr_segment), segment_offset_(segment_offset) {}
    std::vector<BufferSegment *>::iterator curr_segment_;
    uint32_t segment_offset_;
  };

  /**
   * Constructs a new undo buffer, drawing its segments from the given buffer pool.
   * @param buffer_pool buffer pool to draw segments from
   */
  explicit UndoBuffer(common::ObjectPool<BufferSegment> *buffer_pool) : buffer_pool_(buffer_pool) {}

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

  /**
   * @return true if UndoBuffer contains no UndoRecords, false otherwise
   */
  bool Empty() const { return buffers_.empty(); }

  /**
   * Reserve an undo record with the given size.
   * @param size the size of the undo record to allocate
   * @return a new undo record with at least the given size reserved
   */
  UndoRecord *NewEntry(const uint32_t size) {
    if (buffers_.empty() || !buffers_.back()->HasBytesLeft(size)) {
      // we are out of space in the buffer. Get a new buffer segment.
      BufferSegment *new_segment = buffer_pool_->Get();
      PELOTON_ASSERT(reinterpret_cast<uintptr_t>(new_segment) % 8 == 0, "a delta entry should be aligned to 8 bytes");
      new_segment->Reset();
      buffers_.push_back(new_segment);
    }
    return reinterpret_cast<UndoRecord *>(buffers_.back()->Reserve(size));
  }

 private:
  common::ObjectPool<BufferSegment> *buffer_pool_;
  std::vector<BufferSegment *> buffers_;
};
}  // namespace terrier::storage
