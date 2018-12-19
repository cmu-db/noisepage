#pragma once
#include <vector>
#include "common/constants.h"
#include "common/object_pool.h"
#include "common/strong_typedef.h"
#include "storage/undo_record.h"

namespace terrier::storage {

/**
 * A RecordBufferSegment is a piece of (reusable) memory used to hold undo records. The segment internally keeps track
 * of its memory usage.
 *
 * This class should not be used by itself. @see UndoBuffer, @see RedoBuffer
 *
 * Not thread-safe.
 */
class RecordBufferSegment {
 public:
  /**
   * @param size the amount of bytes to check for
   * @return Whether this segment have enough space left for size many bytes
   */
  bool HasBytesLeft(const uint32_t size) const { return size_ + size <= common::Constants::BUFFER_SEGMENT_SIZE; }

  /**
   * Reserve space for a delta record of given size to be written in this segment. The segment must have
   * enough space for that record.
   *
   * @param size the amount of bytes to reserve
   * @return pointer to the head of the allocated record
   */
  byte *Reserve(const uint32_t size) {
    TERRIER_ASSERT(HasBytesLeft(size), "buffer segment allocation out of bounds");
    auto *result = bytes_ + size_;
    size_ += size;
    return result;
  }

  /**
   * Clears the buffer segment.
   *
   * @return self pointer for chaining
   */
  RecordBufferSegment *Reset() {
    size_ = 0;
    return this;
  }

 private:
  template <class RecordType>
  friend class SegmentedBuffer;

  byte bytes_[common::Constants::BUFFER_SEGMENT_SIZE];
  uint32_t size_ = 0;
};

/**
 * Custom allocator used for the object pool of buffer segments
 */
class RecordBufferSegmentAllocator {
 public:
  /**
   * Allocates a new BufferSegment
   * @return a new buffer segment
   */
  RecordBufferSegment *New() {
    auto *result = new RecordBufferSegment;
    TERRIER_ASSERT(reinterpret_cast<uintptr_t>(result) % 8 == 0, "buffer segments should be aligned to 8 bytes");
    return result;
  }

  /**
   * Resets the given buffer segment for use
   * @param reused the buffer to reuse
   */
  void Reuse(RecordBufferSegment *const reused) { reused->Reset(); }

  /**
   * Delete the given buffer segment and frees the memory
   * @param ptr the buffer to delete
   */
  void Delete(RecordBufferSegment *const ptr) { delete ptr; }
};

/**
 * Type alias for an object pool handing out buffer segments
 */
using RecordBufferSegmentPool = common::ObjectPool<RecordBufferSegment, RecordBufferSegmentAllocator>;

// TODO(Tianyu): Not thread-safe. We can probably just allocate thread-local buffers (or segments) if we ever want
// multiple workers on the same transaction.
/**
 * An SegmentedBuffer is a resizable buffer to hold UndoRecords and RedoRecords.
 *
 * Pointers and Iterators handed out from the SegmentedBuffer is guaranteed to be persistent. Adding new
 * entries to the buffer does not interfere with existing pointers and iterators.
 *
 * records should not be removed from SegmentedBuffer until the transaction's end (only in the destructor)
 *
 * Not thread-safe
 */
template <class RecordType>
class SegmentedBuffer {
 public:
  /**
   * Iterator to access records inside the undo buffer.
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
    RecordType &operator*() const {
      return *reinterpret_cast<RecordType *>((*curr_segment_)->bytes_ + segment_offset_);
    }

    /**
     * @return pointer to the underlying UndoRecord
     */
    RecordType *operator->() const {
      return reinterpret_cast<RecordType *>((*curr_segment_)->bytes_ + segment_offset_);
    }

    /**
     * prefix-increment
     * @return self-reference
     */
    Iterator &operator++() {
      RecordType &me = this->operator*();
      segment_offset_ += me.Size();
      if (segment_offset_ == (*curr_segment_)->size_) {
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
    friend class SegmentedBuffer<RecordType>;
    Iterator(std::vector<RecordBufferSegment *>::iterator curr_segment, uint32_t segment_offset)
        : curr_segment_(curr_segment), segment_offset_(segment_offset) {}
    std::vector<RecordBufferSegment *>::iterator curr_segment_;
    uint32_t segment_offset_;
  };

  /**
   * Constructs a new undo buffer, drawing its segments from the given buffer pool.
   * @param buffer_pool buffer pool to draw segments from
   */
  explicit SegmentedBuffer(RecordBufferSegmentPool *buffer_pool) : buffer_pool_(buffer_pool) {}

  SegmentedBuffer() = default;

  SegmentedBuffer(SegmentedBuffer &&) = default;

  SegmentedBuffer &operator=(SegmentedBuffer &&) = default;

  DISALLOW_COPY(SegmentedBuffer);

  /**
   * Destructs this buffer, releases all its segments back to the buffer pool it draws from.
   */
  ~SegmentedBuffer() {
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
  byte *NewEntry(uint32_t size)  {
    if (buffers_.empty() || !buffers_.back()->HasBytesLeft(size)) {
      // we are out of space in the buffer. Get a new buffer segment.
      RecordBufferSegment *new_segment = buffer_pool_->Get();
      TERRIER_ASSERT(reinterpret_cast<uintptr_t>(new_segment) % 8 == 0, "a delta entry should be aligned to 8 bytes");
      buffers_.push_back(new_segment);
    }
    return buffers_.back()->Reserve(size);
  }

 private:
  RecordBufferSegmentPool *buffer_pool_;
  std::vector<RecordBufferSegment *> buffers_;
};
}  // namespace terrier::storage
