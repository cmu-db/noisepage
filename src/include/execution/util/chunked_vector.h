#pragma once

#include <algorithm>
#include <utility>

#include "execution/util/common.h"
#include "execution/util/region.h"
#include "execution/util/region_containers.h"

namespace tpl::util {

class ChunkedVectorRandomIterator;

/// A ChunkedVector is similar to STL's std::vector, but with three important
/// distinctions: ChunkedVectors are untyped and are not templated;
/// ChunkedVectors do not guarantee physical contiguity of all elements, though
/// the majority of elements are stored contiguously; ChunkedVectors ensures
/// that pointers into the container are not invalidated through insertions.
///
/// ChunkedVectors are composed of a list of fixed-sized memory chunks and one
/// active chunk. Elements \a within a chunk are stored contiguously, and new
/// elements are inserted into the active chunk (i.e., the most recently
/// allocated chunk and the last chunk in the list of chunks). Appending new
/// elements is an amortized constant O(1) time operation; random access lookups
/// are also constant O(1) time operations. Iteration performance is comparable
/// to std::vector since the majority of elements are contiguous.
///
/// This class is useful (and usually faster) when you don't need to rely on
/// contiguity of elements, or when you do not know the number of insertions
/// into the vector apriori. In fact, when the number of insertions is unknown,
/// a chunked vector will be roughly 2x faster than a std::vector.
class ChunkedVector {
 public:
  // clang-format off
  // We store 256 elements in each chunk of the vector
  static constexpr const u32 kLogNumElementsPerChunk = 8;
  static constexpr const u32 kNumElementsPerChunk = (1u << kLogNumElementsPerChunk);
  static constexpr const u32 kChunkPositionMask = kNumElementsPerChunk - 1;
  // clang-format on

  ChunkedVector(util::Region *region, std::size_t element_size) noexcept;
  ~ChunkedVector() noexcept;

  // -------------------------------------------------------
  // Iterators
  // -------------------------------------------------------

  ChunkedVectorRandomIterator begin() noexcept;
  ChunkedVectorRandomIterator end() noexcept;

  // -------------------------------------------------------
  // Element access
  // -------------------------------------------------------

  /// Checked indexed lookup
  byte *at(std::size_t idx);
  const byte *at(std::size_t idx) const;

  /// Unchecked indexed lookup
  byte *operator[](std::size_t idx) noexcept;
  const byte *operator[](std::size_t idx) const noexcept;
  byte *front() noexcept;
  const byte *front() const noexcept;
  byte *back() noexcept;
  const byte *back() const noexcept;

  // -------------------------------------------------------
  // Modification
  // -------------------------------------------------------

  /// Append a new entry at the end of the vector, returning a contiguous memory
  /// space where the element can be written to by the caller
  byte *append() noexcept;
  void push_back(const byte *elem);
  void pop_back();

  // -------------------------------------------------------
  // Size/Capacity
  // -------------------------------------------------------

  /// Is this vector empty?
  bool empty() const noexcept { return size() == 0; }

  /// Return the number of elements in the chunked vector
  std::size_t size() const noexcept { return num_elements_; }

  /// Given the size (in bytes) of an individual element, compute the size of
  /// each chunk in the chunked vector
  static constexpr std::size_t ChunkAllocSize(std::size_t element_size) { return kNumElementsPerChunk * element_size; }

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  std::size_t element_size() const noexcept { return element_size_; }

 private:
  // Allocate a new chunk
  void AllocateChunk();

 private:
  // The memory allocator we use to acquire memory chunks
  util::Region *region_;

  // The list of pointers to all chunks
  util::RegionVector<byte *> chunks_;

  // The current position in the last chunk and the position of the end
  std::size_t active_chunk_idx_;
  byte *position_;
  byte *end_;

  // The size of the elements this vector stores
  std::size_t element_size_;
  std::size_t num_elements_;
};

// ---------------------------------------------------------
// GenericChunkedVector Iterator
// ---------------------------------------------------------

/// An iterator over the elements in a generic chunked-vector
class ChunkedVectorRandomIterator {
 public:
  // Random Iterator typedefs.
  using difference_type = i64;
  using value_type = byte *;
  using iterator_category = std::random_access_iterator_tag;
  using pointer = byte **;
  using reference = byte *&;

  ChunkedVectorRandomIterator() noexcept : chunks_iter_(), element_size_(0), curr_(nullptr) {}

  ChunkedVectorRandomIterator(util::RegionVector<byte *>::iterator chunks_iter, byte *position,
                              std::size_t element_size) noexcept
      : chunks_iter_(chunks_iter), element_size_(element_size), curr_(position) {
    if (*chunks_iter + ChunkedVector::ChunkAllocSize(element_size) == position) {
      ++chunks_iter_;
      curr_ = *chunks_iter_;
    }
  }

  // Dereference
  byte *operator*() const noexcept { return curr_; }

  // In place addition
  ChunkedVectorRandomIterator &operator+=(const i64 &offset) {
    // The size (in bytes) of one chunk
    const i64 chunk_size = ChunkedVector::ChunkAllocSize(element_size_);

    // The total number of bytes between the new and current position
    const i64 byte_offset = offset * static_cast<i64>(element_size_) + (curr_ - *chunks_iter_);

    // Offset of the new chunk relative to the current chunk
    i64 chunk_offset;

    // Optimize for the common case where offset is relatively small. This
    // reduces the number of integer divisions.
    if (byte_offset < chunk_size && byte_offset >= 0) {
      chunk_offset = 0;
    } else if (byte_offset >= chunk_size && byte_offset < 2 * chunk_size) {
      chunk_offset = 1;
    } else if (byte_offset < 0 && byte_offset > (-chunk_size)) {
      chunk_offset = -1;
    } else {
      // When offset is large, division can't be avoided. Force rounding towards
      // negative infinity when the offset is negative.
      chunk_offset = (byte_offset - (offset < 0) * (chunk_size - 1)) / chunk_size;
    }

    // Update the chunk pointer
    chunks_iter_ += chunk_offset;

    // Update the pointer within the new current chunk
    curr_ = *chunks_iter_ + byte_offset - chunk_offset * chunk_size;

    // Finish
    return *this;
  }

  // In place subtraction
  ChunkedVectorRandomIterator &operator-=(const i64 &offset) {
    *this += (-offset);
    return *this;
  }

  // Addition
  const ChunkedVectorRandomIterator operator+(const i64 &offset) const {
    ChunkedVectorRandomIterator copy(*this);
    copy += offset;
    return copy;
  }

  // Subtraction
  const ChunkedVectorRandomIterator operator-(const i64 &offset) const {
    ChunkedVectorRandomIterator copy(*this);
    copy -= offset;
    return copy;
  }

  // Pre-increment
  // NOTE: This is not implemented in terms of += to optimize for the cases when
  // the offset is known.
  ChunkedVectorRandomIterator &operator++() noexcept {
    const i64 chunk_size = ChunkedVector::ChunkAllocSize(element_size_);
    const i64 byte_offset = static_cast<i64>(element_size_) + (curr_ - *chunks_iter_);
    // NOTE: an explicit if statement is a bit faster despite the possibility of
    // branch misprediction.
    if (byte_offset >= chunk_size) {
      ++chunks_iter_;
      curr_ = *chunks_iter_ + (byte_offset - chunk_size);
    } else {
      curr_ += element_size_;
    }
    return *this;
  }

  // Post-increment
  const ChunkedVectorRandomIterator operator++(int) noexcept {
    ChunkedVectorRandomIterator copy(*this);
    ++(*this);
    return copy;
  }

  // Pre-decrement
  // NOTE: This is not implemented in terms of += to optimize for the cases when
  // the offset is known.
  ChunkedVectorRandomIterator &operator--() noexcept {
    const i64 chunk_size = ChunkedVector::ChunkAllocSize(element_size_);
    const i64 byte_offset = -static_cast<i64>(element_size_) + (curr_ - *chunks_iter_);
    // NOTE: an explicit if statement is a bit faster despite the possibility of
    // branch misprediction.
    if (byte_offset < 0) {
      --chunks_iter_;
      curr_ = *chunks_iter_ + byte_offset + chunk_size;
    } else {
      curr_ -= element_size_;
    }
    return *this;
  }

  // Post-decrement
  const ChunkedVectorRandomIterator operator--(int) noexcept {
    ChunkedVectorRandomIterator copy(*this);
    ++(*this);
    return copy;
  }

  // Indexing
  byte *operator[](const i64 &idx) const noexcept { return *(*this + idx); }

  // Equality
  bool operator==(const ChunkedVectorRandomIterator &that) const noexcept { return curr_ == that.curr_; }

  // Difference
  bool operator!=(const ChunkedVectorRandomIterator &that) const noexcept { return !(this->operator==(that)); }

  // Less than
  bool operator<(const ChunkedVectorRandomIterator &that) const noexcept {
    if (chunks_iter_ != that.chunks_iter_) return chunks_iter_ < that.chunks_iter_;
    return curr_ < that.curr_;
  }

  // Greater than
  bool operator>(const ChunkedVectorRandomIterator &that) const noexcept {
    return this->operator!=(that) && !(this->operator<(that));
  }

  // Less than or equal to
  bool operator<=(const ChunkedVectorRandomIterator &that) const noexcept { return !(this->operator>(that)); }

  // Greater than or equal to
  bool operator>=(const ChunkedVectorRandomIterator &that) const noexcept { return !(this->operator<(that)); }

  difference_type operator-(const ChunkedVectorRandomIterator &that) const noexcept {
    const i64 chunk_size = ChunkedVector::ChunkAllocSize(element_size_);
    const i64 elem_size = static_cast<i64>(element_size_);

    return ((chunks_iter_ - that.chunks_iter_) * chunk_size +
            ((curr_ - *chunks_iter_) - (that.curr_ - *that.chunks_iter_))) /
           elem_size;
  }

 private:
  util::RegionVector<byte *>::iterator chunks_iter_;
  std::size_t element_size_;
  byte *curr_;
};

// ---------------------------------------------------------
// ChunkedVector implementation
// ---------------------------------------------------------

inline ChunkedVector::ChunkedVector(util::Region *region, std::size_t element_size) noexcept
    : region_(region),
      chunks_(region),
      active_chunk_idx_(0),
      position_(nullptr),
      end_(nullptr),
      element_size_(element_size),
      num_elements_(0) {
  chunks_.reserve(4);
}

inline ChunkedVector::~ChunkedVector() noexcept {
  const std::size_t chunk_size = ChunkAllocSize(element_size());
  for (auto *chunk : chunks_) {
    region_->Deallocate(chunk, chunk_size);
  }
}

inline ChunkedVectorRandomIterator ChunkedVector::begin() noexcept {
  if (empty()) {
    return ChunkedVectorRandomIterator();
  }
  return ChunkedVectorRandomIterator(chunks_.begin(), chunks_[0], element_size());
}

inline ChunkedVectorRandomIterator ChunkedVector::end() noexcept {
  if (empty()) {
    return ChunkedVectorRandomIterator();
  }
  return ChunkedVectorRandomIterator(chunks_.end() - 1, position_, element_size());
}

inline byte *ChunkedVector::at(size_t idx) {
  if (idx > size()) {
    throw std::out_of_range("Out-of-range access");
  }
  return (*this)[idx];
}

inline const byte *ChunkedVector::at(size_t idx) const {
  if (idx > size()) {
    throw std::out_of_range("Out-of-range access");
  }
  return (*this)[idx];
}

inline byte *ChunkedVector::operator[](std::size_t idx) noexcept {
  const std::size_t chunk_idx = idx >> kLogNumElementsPerChunk;
  const std::size_t chunk_pos = idx & kChunkPositionMask;
  return chunks_[chunk_idx] + (element_size() * chunk_pos);
}

inline const byte *ChunkedVector::operator[](std::size_t idx) const noexcept {
  const std::size_t chunk_idx = idx >> kLogNumElementsPerChunk;
  const std::size_t chunk_pos = idx & kChunkPositionMask;
  return chunks_[chunk_idx] + (element_size() * chunk_pos);
}

inline byte *ChunkedVector::front() noexcept { return chunks_[0]; }

inline const byte *ChunkedVector::front() const noexcept { return chunks_[0]; }

inline byte *ChunkedVector::back() noexcept { return this->operator[](size() - 1); }

inline const byte *ChunkedVector::back() const noexcept { return this->operator[](size() - 1); }

inline void ChunkedVector::AllocateChunk() {
  std::size_t alloc_size = ChunkAllocSize(element_size());
  byte *new_chunk = static_cast<byte *>(region_->Allocate(alloc_size));
  chunks_.push_back(new_chunk);
  active_chunk_idx_ = chunks_.size() - 1;
  position_ = new_chunk;
  end_ = new_chunk + alloc_size;
}

inline byte *ChunkedVector::append() noexcept {
  if (position_ == end_) {
    if (chunks_.empty() || active_chunk_idx_ == chunks_.size() - 1) {
      AllocateChunk();
    } else {
      position_ = chunks_[++active_chunk_idx_];
      end_ = position_ + ChunkAllocSize(element_size());
    }
  }

  byte *const result = position_;
  position_ += element_size_;
  num_elements_++;
  return result;
}

inline void ChunkedVector::push_back(const byte *const elem) {
  byte *dest = append();
  std::memcpy(dest, elem, element_size());
}

inline void ChunkedVector::pop_back() {
  TPL_ASSERT(!empty(), "Popping empty vector");
  if (position_ == chunks_[active_chunk_idx_]) {
    end_ = chunks_[--active_chunk_idx_] + ChunkAllocSize(element_size());
    position_ = end_;
  }

  position_ -= element_size();
  num_elements_--;
}

// ---------------------------------------------------------
// Templated ChunkedVector
// ---------------------------------------------------------

// A typed chunked vector. We use this to make the tests easier to understand.
template <typename T>
class ChunkedVectorT {
 public:
  explicit ChunkedVectorT(util::Region *region) noexcept : vec_(region, sizeof(T)) {}

  // -------------------------------------------------------
  // Iterators
  // -------------------------------------------------------

  class Iterator {
   public:
    using difference_type = ChunkedVectorRandomIterator::difference_type;
    using value_type = T;
    using iterator_category = std::random_access_iterator_tag;
    using pointer = T *;
    using reference = T &;

    explicit Iterator(ChunkedVectorRandomIterator iter) : iter_(iter) {}

    Iterator() : iter_() {}

    T &operator*() const noexcept { return *reinterpret_cast<T *>(*iter_); }

    Iterator &operator+=(const i64 &offset) noexcept {
      iter_ += offset;
      return *this;
    }

    Iterator &operator-=(const i64 &offset) noexcept {
      iter_ -= offset;
      return *this;
    }

    const Iterator operator+(const i64 &offset) const noexcept { return Iterator(iter_ + offset); }

    const Iterator operator-(const i64 &offset) const noexcept { return Iterator(iter_ - offset); }

    Iterator &operator++() noexcept {
      ++iter_;
      return *this;
    }

    const Iterator operator++(int) noexcept { return Iterator(iter_++); }

    Iterator &operator--() noexcept {
      --iter_;
      return *this;
    }

    const Iterator operator--(int) noexcept { return Iterator(iter_--); }

    T &operator[](const i64 &idx) const noexcept { return *reinterpret_cast<T *>(iter_[idx]); }

    bool operator==(const Iterator &that) const { return iter_ == that.iter_; }

    bool operator!=(const Iterator &that) const { return iter_ != that.iter_; }

    bool operator<(const Iterator &that) const { return iter_ < that.iter_; }

    bool operator<=(const Iterator &that) const { return iter_ <= that.iter_; }

    bool operator>(const Iterator &that) const { return iter_ > that.iter_; }

    bool operator>=(const Iterator &that) const { return iter_ >= that.iter_; }

    difference_type operator-(const Iterator &that) const { return iter_ - that.iter_; }

   private:
    ChunkedVectorRandomIterator iter_;
  };

  Iterator begin() { return Iterator(vec_.begin()); }
  Iterator end() { return Iterator(vec_.end()); }

  // -------------------------------------------------------
  // Element access
  // -------------------------------------------------------

  T &operator[](std::size_t idx) noexcept;
  const T &operator[](std::size_t idx) const noexcept;
  T &front() noexcept;
  const T &front() const noexcept;
  T &back() noexcept;
  const T &back() const noexcept;

  // -------------------------------------------------------
  // Size/Capacity
  // -------------------------------------------------------

  bool empty() const noexcept { return vec_.empty(); }
  std::size_t size() const noexcept { return vec_.size(); }

  // -------------------------------------------------------
  // Modifiers
  // -------------------------------------------------------

  template <class... Args>
  void emplace_back(Args &&... args);
  void push_back(const T &elem);
  void push_back(T &&elem);
  void pop_back();

 private:
  // The generic vector
  ChunkedVector vec_;
};

// ---------------------------------------------------------
// ChunkedVectorT Implementation
// ---------------------------------------------------------

template <typename T>
T &ChunkedVectorT<T>::operator[](std::size_t idx) noexcept {
  return *reinterpret_cast<T *>(vec_[idx]);
}

template <typename T>
const T &ChunkedVectorT<T>::operator[](std::size_t idx) const noexcept {
  return *reinterpret_cast<T *>(vec_[idx]);
}

template <typename T>
T &ChunkedVectorT<T>::front() noexcept {
  return *reinterpret_cast<T *>(vec_.front());
}

template <typename T>
const T &ChunkedVectorT<T>::front() const noexcept {
  return *reinterpret_cast<const T *>(vec_.front());
}

template <typename T>
T &ChunkedVectorT<T>::back() noexcept {
  return *reinterpret_cast<T *>(vec_.back());
}

template <typename T>
const T &ChunkedVectorT<T>::back() const noexcept {
  return *reinterpret_cast<const T *>(vec_.back());
}

template <typename T>
template <class... Args>
inline void ChunkedVectorT<T>::emplace_back(Args &&... args) {
  T *space = reinterpret_cast<T *>(vec_.append());
  new (space) T(std::forward<Args>(args)...);
}

template <typename T>
inline void ChunkedVectorT<T>::push_back(const T &elem) {
  T *space = reinterpret_cast<T *>(vec_.append());
  new (space) T(elem);
}

template <typename T>
inline void ChunkedVectorT<T>::push_back(T &&elem) {
  T *space = reinterpret_cast<T *>(vec_.append());
  new (space) T(std::move(elem));
}

template <typename T>
inline void ChunkedVectorT<T>::pop_back() {
  T &removed = back();
  vec_.pop_back();
  removed.~T();
}

}  // namespace tpl::util
