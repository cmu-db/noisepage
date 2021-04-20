#pragma once

#include <algorithm>
#include <utility>
#include <vector>

namespace noisepage::common {

/**
 * Describes a chunked array. A chunked array batch allocates Size number of elements
 * of type Type at a given time. The premise for this is to amortize comparably
 * expensive memory allocation calls over a large number of inserts.
 *
 * Unlike a std::vector iterator that can get invalidated whenever an element is
 * added to the container, ChunkedArray::Iterator remains valid as new elements
 * are pushed.
 */
template <class Type, size_t Size>
class ChunkedArray {
 public:
  /**
   * Describes a single chunk
   */
  template <class TypeT, size_t SizeT>
  class ChunkSlot {
   public:
    /** Indicates current slot to write to in slots_ */
    size_t cur_idx_ = 0;
    /** Flat array of SizeT slots of Type */
    Type slots_[SizeT];
  };

  ChunkedArray() = default;

  ~ChunkedArray() = default;

  /**
   * Clears all the data
   */
  void Clear() { chunks_.clear(); }

  /**
   * Add an element
   * @param obj Object to insert
   */
  void Push(Type &&obj) {
    if (chunks_.empty() || chunks_.back().cur_idx_ >= Size) {
      chunks_.emplace_back();
    }

    chunks_.back().slots_[chunks_.back().cur_idx_++] = std::move(obj);
  }

  /**
   * Acquire ownership of another ChunkedArray's data
   * @param merge Other ChunkedArray's data to own
   */
  void Merge(ChunkedArray<Type, Size> *merge) {
    chunks_.insert(chunks_.end(), std::make_move_iterator(merge->chunks_.begin()),
                   std::make_move_iterator(merge->chunks_.end()));
  }

  /**
   * Iterator
   */
  template <class TypeT, size_t SizeT>
  class Iterator {
   public:
    /** Empty constructor. */
    Iterator() noexcept = default;

    /**
     * Constructor
     * @param chunks Pointer to the chunks vector
     */
    explicit Iterator(typename std::vector<ChunkSlot<TypeT, SizeT>> *chunks) noexcept : chunks_(chunks) {}

    /**
     * Constructor
     * @param chunks Pointer to the chunks vector
     * @param chunk_pos Chunk to start reading from
     */
    Iterator(typename std::vector<ChunkSlot<TypeT, SizeT>> *chunks, std::size_t chunk_pos) noexcept
        : chunks_(chunks), chunks_pos_(chunk_pos) {}

    /** @return The current element. */
    Type &operator*() const noexcept { return (*chunks_)[chunks_pos_].slots_[cur_pos_]; }

    /**
     * Pre-increment
     * NOTE: This is not implemented in terms of += to optimize for the cases when
     * the offset is known.
     * @return the (same) updated iterator
     */
    Iterator &operator++() noexcept {
      cur_pos_++;
      if (cur_pos_ >= (*chunks_)[chunks_pos_].cur_idx_) {
        // Slide to the next chunk
        chunks_pos_++;
        cur_pos_ = 0;
      }
      return *this;
    }

    /**
     * Post-increment
     * @return the new incremented iterator
     */
    Iterator operator++(int) noexcept {
      Iterator copy(*this);
      ++(*this);
      return copy;
    }

    /**
     * Equality
     * @param that other iterator to compare to
     * @return whether the two iterators are in the same position
     */
    bool operator==(const Iterator &that) const noexcept {
      return chunks_ == that.chunks_ && chunks_pos_ == that.chunks_pos_ && cur_pos_ == that.cur_pos_;
    }

    /**
     * Difference
     * @param that other iterator to compare to
     * @return whether the two iterators are in different positions
     */
    bool operator!=(const Iterator &that) const noexcept { return !(this->operator==(that)); }

   private:
    typename std::vector<ChunkSlot<TypeT, SizeT>> *chunks_;
    std::size_t chunks_pos_ = 0;
    std::size_t cur_pos_ = 0;
  };

  /**
   * @return iterator to first element
   */
  Iterator<Type, Size> begin() noexcept {  // NOLINT
    if (chunks_.empty()) {
      return Iterator<Type, Size>(nullptr, 0);
    }
    return Iterator<Type, Size>(&chunks_);
  }

  /**
   * @return iterator to indicate end of data
   */
  Iterator<Type, Size> end() noexcept {  // NOLINT
    if (chunks_.empty()) {
      return Iterator<Type, Size>(nullptr, 0);
    }
    return Iterator<Type, Size>(&chunks_, chunks_.size());
  }

 private:
  std::vector<ChunkSlot<Type, Size>> chunks_;
};

}  // namespace noisepage::common
