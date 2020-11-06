#pragma once
#include <vector>

#include "common/constants.h"
#include "common/object_pool.h"
#include "common/strong_typedef.h"
#include "storage/undo_record.h"

namespace noisepage::transaction {
class TransactionManager;
}  // namespace noisepage::transaction

namespace noisepage::storage {

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
    NOISEPAGE_ASSERT(HasBytesLeft(size), "buffer segment allocation out of bounds");
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
  friend class IterableBufferSegment;

  friend class UndoBuffer;

  byte bytes_[common::Constants::BUFFER_SEGMENT_SIZE];
  uint32_t size_ = 0;
};

/**
 * A thin wrapper around a buffer segment to allow iteration through its contents as the given template type
 * @tparam RecordType records are treated as this type when iterating. Must expose an instance method Size() that
 *                    gives the size of the record in memory in bytes
 */
template <class RecordType>
class IterableBufferSegment {
 public:
  /**
   * Iterator for iterating through the records in the buffer
   */
  class Iterator {
   public:
    /**
     * @return reference to the underlying record
     */
    RecordType &operator*() const { return *reinterpret_cast<RecordType *>(segment_->bytes_ + segment_offset_); }

    /**
     * @return pointer to the underlying record
     */
    RecordType *operator->() const { return reinterpret_cast<RecordType *>(segment_->bytes_ + segment_offset_); }

    /**
     * prefix increment
     * @return self-reference
     */
    Iterator &operator++() {
      RecordType &me = this->operator*();
      segment_offset_ += me.Size();
      return *this;
    }

    /**
     * postfix increment
     * @return iterator that is equal to this before increment
     */
    Iterator operator++(int) {
      Iterator copy = *this;
      operator++();
      return copy;
    }

    /**
     * equality check
     * @param other the other iterator to compare to
     * @return if the two iterators point to the same underlying record
     */
    bool operator==(const Iterator &other) const {
      return segment_offset_ == other.segment_offset_ && segment_ == other.segment_;
    }

    /**
     * inequality check
     * @param other the other iterator to comapre to
     * @return if the two iterators point to different underlying records
     */
    bool operator!=(const Iterator &other) const { return !(*this == other); }

   private:
    friend class IterableBufferSegment;
    Iterator(RecordBufferSegment *segment, uint32_t segment_offset)
        : segment_(segment), segment_offset_(segment_offset) {}
    RecordBufferSegment *segment_;
    uint32_t segment_offset_;
  };

  /**
   * Instantiates an IterableBufferSegment as a wrapper around a buffer segment
   * @param segment
   */
  explicit IterableBufferSegment(RecordBufferSegment *segment) : segment_(segment) {}

  /**
   * @return iterator to the first element
   */
  Iterator begin() { return {segment_, 0}; }  // NOLINT for STL name compability

  /**
   * @return iterator to the second element
   */
  Iterator end() { return {segment_, segment_->size_}; }  // NOLINT for STL name compability

 private:
  RecordBufferSegment *segment_;
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
    NOISEPAGE_ASSERT(reinterpret_cast<uintptr_t>(result) % 8 == 0, "buffer segments should be aligned to 8 bytes");
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
    Iterator operator++(int) {
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
    Iterator(std::vector<RecordBufferSegment *>::iterator curr_segment, uint32_t segment_offset)
        : curr_segment_(curr_segment), segment_offset_(segment_offset) {}
    std::vector<RecordBufferSegment *>::iterator curr_segment_;
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
  Iterator begin() { return {buffers_.begin(), 0}; }  // NOLINT for STL name compability

  /**
   * @return Iterator to the element following the last element
   */
  Iterator end() { return {buffers_.end(), 0}; }  // NOLINT for STL name compability

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

  /**
   * @return a pointer to the beginning of the last record requested, or nullptr if no record exists.
   */
  byte *LastRecord() const { return last_record_; }

 private:
  RecordBufferSegmentPool *buffer_pool_;
  std::vector<RecordBufferSegment *> buffers_;
  byte *last_record_ = nullptr;
};

class LogManager;  // forward declaration

/**
 * A RedoBuffer is a fixed-sized buffer to hold RedoRecords.
 *
 * Not thread-safe
 */
class RedoBuffer {
 public:
  /**
   * Initializes a new RedoBuffer, working with the given LogManager
   * @param log_manager the log manager this redo buffer talks to, or nullptr if logging is disabled
   * @param buffer_pool The buffer pool to draw buffer segments from. Must be the same buffer pool the log manager uses.
   */
  RedoBuffer(LogManager *log_manager, RecordBufferSegmentPool *buffer_pool)
      : has_flushed_(false), log_manager_(log_manager), buffer_pool_(buffer_pool) {}

  /**
   * Reserve a redo record with the given size, in bytes. The returned pointer is guaranteed to be valid until NewEntry
   * is called again, or when the buffer is explicitly flushed by the call Finish().
   * @param size the size of the redo record to allocate
   * @return a new redo record with at least the given size reserved
   */
  byte *NewEntry(uint32_t size);

  /**
   * Flush all contents of the redo buffer to be logged out, effectively closing this redo buffer. No further entries
   * can be written to this redo buffer after the function returns.
   * @param flush_buffer whether the transaction holding this RedoBuffer should flush the its redo buffer
   */
  void Finalize(bool flush_buffer);

  /**
   * @return a pointer to the beginning of the last record requested, or nullptr if no record exists.
   */
  byte *LastRecord() const { return last_record_; }

  /**
   * @return true if this buffer has previously flushed to the log manager
   */
  bool HasFlushed() const { return has_flushed_; }

  /**
   * Reset the RedoBuffer to empty
   */
  void Reset() {
    if (buffer_seg_ != nullptr) buffer_seg_->Reset();
  }

 private:
  // Flag to denote if this RedoBuffer has flushed records to the log manager already.
  // We use this to determine if we should write an abort record, since we only need to write an abort record if this
  // buffer has previously flushed logs to the log manager. In the case of recovery, the abort record helps it discard
  // changes from aborted txns
  bool has_flushed_;
  LogManager *const log_manager_;
  RecordBufferSegmentPool *const buffer_pool_;
  RecordBufferSegment *buffer_seg_ = nullptr;
  // reserved for aborts where we will potentially need to garbage collect the last operation (which caused the abort)
  byte *last_record_ = nullptr;
};
}  // namespace noisepage::storage
