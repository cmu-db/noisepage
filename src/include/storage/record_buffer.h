#pragma once
#include <vector>
#include "common/constants.h"
#include "common/object_pool.h"
#include "common/typedefs.h"
#include "storage/undo_record.h"

namespace terrier::storage {

/**
 * An UndoBufferSegment is a piece of (reusable) memory used to hold undo records. The segment internally keeps track
 * of its memory usage.
 *
 * This class should not be used by itself. @see UndoBuffer, @see RedoBuffer
 *
 * Not thread-safe.
 */
class BufferSegment {
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
  BufferSegment *Reset() {
    size_ = 0;
    return this;
  }

  char *WritableHead() {
    return reinterpret_cast<char *>(bytes_);
  }

  uint32_t Size() const {
    return size_;
  }

 private:
  template<class RecordType>
  friend class IterableBufferSegment;

  friend class UndoBuffer;

  byte bytes_[common::Constants::BUFFER_SEGMENT_SIZE];
  uint32_t size_ = 0;
};

template<class RecordType>
class IterableBufferSegment {
 public:
  class Iterator {
   public:
    RecordType &operator*() const {
      return *reinterpret_cast<RecordType *>(segment_->bytes_ + segment_offset_);
    }

    RecordType *operator->() const {
      return reinterpret_cast<RecordType *>(segment_->bytes_ + segment_offset_);
    }

    Iterator &operator++() {
      RecordType &me = this->operator*();
      segment_offset_ += me.Size();
      return *this;
    }

    const Iterator operator++(int) {
      Iterator copy = *this;
      operator++();
      return copy;
    }

    bool operator==(const Iterator &other) const {
      return segment_offset_ == other.segment_offset_ && segment_ == other.segment_;
    }

    bool operator!=(const Iterator &other) const { return !(*this == other); }

   private:
    friend class IterableBufferSegment;
    Iterator(BufferSegment *segment, uint32_t segment_offset) : segment_(segment), segment_offset_(segment_offset) {}
    BufferSegment *segment_;
    uint32_t segment_offset_;
  };

  explicit IterableBufferSegment(BufferSegment *segment) : segment_(segment) {}

  Iterator begin() {
    return {segment_, 0};
  }

  Iterator end() {
    return {segment_, segment_->size_};
  }

 private:
  BufferSegment *segment_;
};

class RecordBufferSegmentAllocator {
 public:
  BufferSegment *New() {
    auto *result = new BufferSegment;
    TERRIER_ASSERT(reinterpret_cast<uintptr_t>(result) % 8 == 0, "buffer segments should be aligned to 8 bytes");
    return result;
  }

  void Reuse(BufferSegment *const reused) {
    reused->Reset();
  }

  void Delete(BufferSegment *const ptr) {
    delete[] reinterpret_cast<byte *>(ptr);
  }
};

using RecordBufferSegmentPool = common::ObjectPool<BufferSegment, RecordBufferSegmentAllocator>;

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
  explicit UndoBuffer(RecordBufferSegmentPool *buffer_pool) : buffer_pool_(buffer_pool) {}

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
  byte *NewEntry(uint32_t size);

 private:
  RecordBufferSegmentPool *buffer_pool_;
  std::vector<BufferSegment *> buffers_;
};

class LogManager;  // forward declaration

class RedoBuffer {
 public:
  explicit RedoBuffer(LogManager *log_manager) : log_manager_(log_manager) {}

  byte *NewEntry(uint32_t size);

  // TODO(Tianyu): Maybe need a better name?
  void Flush();

  bool LoggingDisabled() const {
    return log_manager_ == LOGGING_DISABLED;
  }

 private:
  LogManager *const log_manager_;
  BufferSegment *buffer_seg_ = nullptr;
};
}  // namespace terrier::storage
