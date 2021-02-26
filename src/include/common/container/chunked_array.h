#pragma once

#include <vector>

namespace noisepage::common {

template <class Type, size_t Size>
class ChunkedArray {
 public:
  template <class TypeT, size_t SizeT>
  class ChunkSlot {
   public:
    size_t cur_idx_ = 0;
    Type slots_[SizeT];
  };

  ChunkedArray() = default;

  ~ChunkedArray() = default;

  void clear() { chunks_.clear(); }

  void push(Type &&obj) {
    if (chunks_.empty() || chunks_.back().cur_idx_ >= Size) {
      chunks_.emplace_back();
    }

    chunks_.back().slots_[chunks_.back().cur_idx_++] = std::move(obj);
  }

  void merge(ChunkedArray<Type, Size> &merge) {
    chunks_.insert(chunks_.end(), std::make_move_iterator(merge.chunks_.begin()),
                   std::make_move_iterator(merge.chunks_.end()));
  }

  /**
   * Random access iterator.
   */
  template <class TypeT, size_t SizeT>
  class Iterator {
   public:
    /** Empty constructor. */
    Iterator() noexcept = default;

    /**
     * Constructor for existing vector
     * @param chunks_iter iterator over the chunks
     * @param position initial position to iterator from
     * @param element_size size of individual elements
     */
    Iterator(typename std::vector<ChunkSlot<TypeT, SizeT>>::iterator chunks_iter) noexcept
        : chunks_iter_(chunks_iter) {}

    /** @return The current element. */
    Type &operator*() const noexcept { return (*chunks_iter_).slots_[cur_pos_]; }

    /**
     * Pre-increment
     * NOTE: This is not implemented in terms of += to optimize for the cases when
     * the offset is known.
     * @return the (same) updated iterator
     */
    Iterator &operator++() noexcept {
      cur_pos_++;
      if (cur_pos_ >= (*chunks_iter_).cur_idx_) {
        ++chunks_iter_;
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
      return chunks_iter_ == that.chunks_iter_ && cur_pos_ == that.cur_pos_;
    }

    /**
     * Difference
     * @param that other iterator to compare to
     * @return whether the two iterators are in different positions
     */
    bool operator!=(const Iterator &that) const noexcept { return !(this->operator==(that)); }

   private:
    typename std::vector<ChunkSlot<TypeT, SizeT>>::iterator chunks_iter_;
    std::size_t cur_pos_ = 0;
  };

  Iterator<Type, Size> begin() noexcept {  // NOLINT
    if (chunks_.empty()) {
      return Iterator<Type, Size>();
    }
    return Iterator<Type, Size>(chunks_.begin());
  }

  Iterator<Type, Size> end() noexcept {  // NOLINT
    if (chunks_.empty()) {
      return Iterator<Type, Size>();
    }
    return Iterator<Type, Size>(chunks_.end());
  }

 private:
  std::vector<ChunkSlot<Type, Size>> chunks_;
};

}  // namespace noisepage::common
