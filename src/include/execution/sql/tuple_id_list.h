#pragma once

#include <algorithm>
#include <iosfwd>
#include <string>
#include <type_traits>

#include "common/macros.h"
#include "execution/util/bit_vector.h"
#include "execution/util/execution_common.h"

namespace noisepage::execution::sql {

/**
 * TupleIdList is an ordered set of tuple IDs (TID) used during query execution to efficiently
 * represent valid tuples in a vector projection. TupleIdLists have a maximum capacity, usually 1024
 * or 2048 TIDs sourced from DEFAULT_VECTOR_SIZE, and a size reflecting the number of tuples
 * in the list. A TupleIdList with capacity C can store all TIDs in the range [0,C) in sorted order.
 *
 * Basic Operations:
 * ----------------
 * TID existence checks, adding TIDs, and removing TIDs from a TupleIdList are all constant-time
 * operations. Intersection, union, and difference of TupleIdLists are linear in the capacity of the
 * list.
 *
 * Usage:
 * @code
 * auto tid_list = TupleIdList(40);   // tid_list can store all TIDs in the range [0, 40)
 * auto v = tid_list.Contains(1);     // v is false since TID 1 is not in the list
 * tid_list.AddRange(8, 12);          // tid_list = [8, 9, 10, 11]
 * v = tid_list.Contains(10);         // v is true
 * tid_list.Remove(10);               // tid_list = [8, 9, 11]
 * @endcode
 *
 * Iteration:
 * ----------
 * Users can iterate the TIDs in a TupleIdList through TupleIdList::ForEach() or using
 * TupleIdListIterator.
 *
 * Usage:
 * @code
 * auto tid_list = TupleIdList(40);
 * for (auto iter = TupleIdListIterator(&tid_list); iter.HasNext(); iter.Advance()) {
 *   auto tid = iter.GetCurrentTupleId();
 *   // tid is an active tuple ID in the list
 * }
 * @endcode
 *
 * Filtering:
 * -----------
 * TIDs in a TupleIdList can be filtered through TupleIdList::Filter().
 *
 * Usage:
 * @code
 * auto tid_list = TupleIdList(8);
 * tid_list.AddAll();                                      // tid_list = [0, 1, 2, 3, 4, 5, 6, 7]
 * tid_list.Filter([](auto tid) { return tid % 2 == 0; }); // tid_list = [0, 2, 4, 6]
 * @endcode
 *
 * Implementation:
 * ---------------
 * TupleIdList is implemented as a very thin wrapper around a bit vector. They occupy 128 or 256
 * bytes to represent 1024 or 2048 tuples, respectively. Using a bit vector as the underlying data
 * structure enables efficient implementations of list intersection, union, and difference required
 * during expression evaluation. Moreover, bit vectors are amenable to auto-vectorization by the
 * compiler.
 *
 * The primary drawback of bit vectors is iteration: dense TID lists (also known as selection index
 * vectors) are faster to iterate over than bit vectors, more so when the selectivity of the vector
 * is low.
 */
class TupleIdList {
 public:
  /** The underlying bit vector for this TupleIdList. */
  using BitVectorType = util::BitVector<uint64_t>;

  /**
   * An iterator over the TIDs in a Tuple ID list.
   *
   * @warning While this exists for convenience, it is very slow and should only be used when the
   * loop is driven by an external controller. When possible, design your algorithms around the
   * callback-based iteration functions in TupleIdList such as TupleIdList::Iterate() and
   * TupleIdList::Filter() as they perform more than 3x faster!!!!!
   */
  class ConstIterator {
   public:
    /**
     * @return The TID the iterator is currently positioned at in the list.
     */
    uint32_t operator*() const noexcept { return current_position_; }

    /**
     * Advance the iterator to the next TID in the list.
     * @return This updated iterator.
     */
    ConstIterator &operator++() noexcept {
      current_position_ = bv_.FindNext(current_position_);
      return *this;
    }

    /**
     * Advance the iterator by @em n elements, positioned at the end of such an advancement would
     * exhaust the iterator.
     * @param n The number of elements to skip.
     * @return This updated iterator.
     */
    ConstIterator &operator+=(uint32_t n) noexcept {
      while (n-- > 0) {
        ++(*this);
      }
      return *this;
    }

    /**
     * Return a new iterator positioned @em n elements ahead of this iterator.
     * @param n The number of elements to skip over.
     * @return A new iterator @em n elements ahead in the list.
     */
    ConstIterator operator+(const uint32_t n) const noexcept {
      ConstIterator iter = *this;
      iter += n;
      return iter;
    }

    /**
     * @return True if this iterator is contextually equivalent to the provided iterator. In this
     *         case, equivalency is defined as pointing to the same TID in the same TID list.
     */
    bool operator==(const ConstIterator &that) const noexcept {
      return &bv_ == &that.bv_ && current_position_ == that.current_position_;
    }

    /**
     * @return True if this iterator is not contextually equivalent to the provided iterator.
     */
    bool operator!=(const ConstIterator &that) const noexcept { return !(*this == that); }

   private:
    friend class TupleIdList;

    ConstIterator(const BitVectorType &bv, uint32_t position) : bv_(bv), current_position_(position) {}

    explicit ConstIterator(const BitVectorType &bv) : ConstIterator(bv, bv.FindFirst()) {}

   private:
    const BitVectorType &bv_;
    uint32_t current_position_;
  };

  /**
   * Construct an empty list capable of storing at least @em num_tuples tuple IDs.
   * @param num_tuples The maximum number of tuples in the list.
   */
  explicit TupleIdList(uint32_t num_tuples) : bit_vector_(num_tuples) {}

  /**
   * This class cannot be copied (but can be moved).
   */
  DISALLOW_COPY(TupleIdList);

  /**
   * Move constructor.
   */
  TupleIdList(TupleIdList &&other) = default;

  /**
   * Move assignment.
   * @param other The TID list whose contents to move into this instance.
   * @return This TID list instance.
   */
  TupleIdList &operator=(TupleIdList &&other) = default;

  /**
   * Resize the list to be able to store at least @em num_tuples tuple IDs. If growing the list, the
   * contents of the list remain unchanged. If shrinking the list, tuples whose IDs are greater than
   * the new capacity are removed.
   *
   * @code
   * auto tids = TupleIdList(10);  // tids = []
   * tids.AddAll();                // tids = [0,1,2,3,4,5,6,7,8,9]
   * tids.Resize(20);              // tids = [0,1,2,3,4,5,6,7,8,9]
   * tids.Add(11);                 // tids = [0,1,2,3,4,5,6,7,8,9,11]
   * tids.Resize(4);               // tids = [0,1,2,3]
   * @endcode
   *
   * @param num_tuples The number of TIDs to list should be able to store.
   */
  void Resize(const uint32_t num_tuples) { bit_vector_.Resize(num_tuples); }

  /**
   * @return True if the given TID @em tid is in the list; false otherwise.
   */
  bool Contains(const uint32_t tid) const { return bit_vector_.Test(tid); }

  /**
   * @return True if this list contains all TIDs in the range [0, capacity); false otherwise.
   */
  bool IsFull() const { return bit_vector_.All(); }

  /**
   * @return True if this list is empty; false otherwise.
   */
  bool IsEmpty() const { return bit_vector_.None(); }

  /**
   * Add the tuple ID @em tid to this list.
   * @pre The given TID must be in the range [0, capacity) of this list.
   * @param tid The ID of the tuple.
   */
  void Add(const uint32_t tid) { bit_vector_.Set(tid); }

  /**
   * Add all TIDs in the range [start_tid, end_tid) to this list. Note the half-open interval!
   * @param start_tid The left inclusive range boundary.
   * @param end_tid The right exclusive range boundary.
   */
  void AddRange(const uint32_t start_tid, const uint32_t end_tid) { bit_vector_.SetRange(start_tid, end_tid); }

  /**
   * Add all tuple IDs this list can support.
   */
  void AddAll() { bit_vector_.SetAll(); }

  /**
   * Conditionally add or remove the tuple with ID @em tid depending on the value of @em enable. If
   * @em enable is true, the tuple is added to the list (with proper duplicate handling). If false,
   * the tuple is removed from the list (if it exists).
   *
   * This method is faster than an explicit if-then-else handled by the client because it can avoid
   * branching. Use this method if you are modifying a TupleIdList in a hot loop.
   *
   * @param tid The ID of the tuple to conditionally add to the list.
   * @param enable The flag indicating if the tuple is added or removed.
   */
  void Enable(const uint32_t tid, const bool enable) { bit_vector_.Set(tid, enable); }

  /**
   * Remove the tuple with the given ID from the list.
   * @param tid The ID of the tuple.
   */
  void Remove(const uint32_t tid) { bit_vector_.Unset(tid); }

  /**
   * Assign all tuple IDs in @em other to this list.
   * @param other The list to copy all TIDs from.
   */
  void AssignFrom(const TupleIdList &other) { bit_vector_.Copy(other.bit_vector_); }

  /**
   * Intersect the set of tuple IDs in this list with the tuple IDs in the provided list.
   * @param other The list to intersect with.
   */
  void IntersectWith(const TupleIdList &other) { bit_vector_.Intersect(other.bit_vector_); }

  /**
   * Union the set of tuple IDs in this list with the tuple IDs in the provided list.
   * @param other The list to union with.
   */
  void UnionWith(const TupleIdList &other) { bit_vector_.Union(other.bit_vector_); }

  /**
   * Remove all tuple IDs from this list that are also present in the provided list.
   * @param other The list to unset from.
   */
  void UnsetFrom(const TupleIdList &other) { bit_vector_.Difference(other.bit_vector_); }

  /**
   * Filter the TIDs in this list based on the given unary filtering function.
   * @tparam P A unary functor that accepts a 32-bit tuple ID and returns true if the tuple ID
   *           remains in the list, and false if the tuple should be removed from the list.
   * @param p The filtering function.
   */
  template <typename P>
  void Filter(P p) {
    if (IsFull()) {
      bit_vector_.UpdateFull(p);
    } else {
      bit_vector_.UpdateSetBits(p);
    }
  }

  /**
   * Build a list of TIDs from the IDs in the input selection vector.
   * @param sel_vector The selection vector.
   * @param size The number of elements in the selection vector.
   */
  void BuildFromSelectionVector(const sel_t *sel_vector, uint32_t size);

  /**
   * Convert the given selection match vector to a TID list. The match vector is assumed to be
   * boolean-like array, but with saturated values. This means that the value 'true' or 1 is
   * physically encoded as all-1s, i.e., a true value is the 8-byte value 255 = 11111111b, and the
   * value 'false' or 0 is encoded as all zeros. This is typically used during selections which
   * naturally produce saturated match vectors.
   * @param matches The match vector.
   * @param size The number of elements in the match vector.
   */
  void BuildFromMatchVector(const uint8_t *const matches, const uint32_t size) {
    bit_vector_.SetFromBytes(matches, size);
  }

  /**
   * Remove all tuples from the list.
   */
  void Clear() { bit_vector_.Reset(); }

  /**
   * @return The internal bit vector representation of the list.
   */
  BitVectorType *GetMutableBits() { return &bit_vector_; }

  /**
   * @return The number of active tuples in the list.
   */
  uint32_t GetTupleCount() const { return bit_vector_.CountOnes(); }

  /**
   * @return The capacity of the TID list.
   */
  uint32_t GetCapacity() const { return bit_vector_.GetNumBits(); }

  /**
   * @return The selectivity of the list as a fraction in the range [0.0, 1.0].
   */
  float ComputeSelectivity() const { return bit_vector_.ComputeDensity(); }

  /**
   * Convert this tuple ID list into a dense selection index vector.
   * @param[out] sel_vec The output selection vector.
   * @return The number of elements in the generated selection vector.
   */
  [[nodiscard]] uint32_t ToSelectionVector(sel_t *sel_vec) const;

  /**
   * Iterate all TIDs in this list.
   * @tparam F Functor type which must take a single unsigned integer parameter.
   * @param f The callback to invoke for each TID in the list.
   */
  template <typename F>
  void ForEach(F f) const {
    if (IsFull()) {
      for (uint32_t i = 0, n = GetCapacity(); i < n; i++) {
        f(i);
      }
    } else {
      bit_vector_.IterateSetBits(f);
    }
  }

  /**
   * @return A string representation of this list.
   */
  std::string ToString() const;

  /**
   * Print a string representation of this vector to the output stream.
   * @param stream Where the string representation is printed to.
   */
  void Dump(std::ostream &stream) const;

  // -------------------------------------------------------
  // C++ operator overloads.
  // -------------------------------------------------------

  /**
   * Access the TID at the provided index in this list.
   * @warning No bounds checking is performed.
   * @param i The index of the element to select.
   * @return The value of the element at the given index.
   */
  std::size_t operator[](const std::size_t i) const {
    NOISEPAGE_ASSERT(i < GetTupleCount(), "Out-of-bounds list access");
    return bit_vector_.NthOne(i);
  }

  /**
   * Assign the TIDs in @em tids to this list. Any previous contents are cleared out.
   * @warning THIS IS ONLY FOR TESTING. DO NOT USE OTHERWISE!
   * @param tids TIDs to add to the list.
   * @return This list.
   */
  TupleIdList &operator=(std::initializer_list<uint32_t> tids) {
    if (!std::all_of(tids.begin(), tids.end(), [this](auto tid) { return tid < GetCapacity(); })) {
      throw std::out_of_range("Not all TIDs can be added to this list");
    }

    Clear();
    for (const auto tid : tids) {
      Add(tid);
    }

    return *this;
  }

  // -------------------------------------------------------
  // STL iterators.
  // -------------------------------------------------------

  /**
   * @return An iterator positioned at the first element in the TID list.
   */
  ConstIterator Begin() { return ConstIterator(bit_vector_); }

  /**
   * @return A const iterator position at the first element in the TID list.
   */
  ConstIterator Begin() const { return ConstIterator(bit_vector_); }

  /**
   * @return An iterator positioned at the end of the list.
   */
  ConstIterator End() { return ConstIterator(bit_vector_, BitVectorType::INVALID_POS); }

  /**
   * @return A const iterator position at the end of the list.
   */
  ConstIterator End() const { return ConstIterator(bit_vector_, BitVectorType::INVALID_POS); }

 private:
  // The validity bit vector
  BitVectorType bit_vector_;
};

/**
 * A forward-only resettable iterator over the contents of a TupleIdList. This iterator is heavy-
 * handed because it will materialize all IDs into an internal array. As such, this iterator
 * shouldn't be used unless you intend to iterate over the entire list.
 */
class TupleIdListIterator {
 public:
  /**
   * Create an iterator over the TID list @em tid_list.
   * @param tid_list The list to iterate over.
   */
  explicit TupleIdListIterator(const TupleIdList *tid_list) : tid_list_(tid_list), size_(0), curr_idx_(0) {
    NOISEPAGE_ASSERT(tid_list->GetCapacity() <= common::Constants::K_DEFAULT_VECTOR_SIZE, "TIDList too large");
    Reset();
  }

  /**
   * @return True if there are more TIDs in the iterator.
   */
  bool HasNext() const { return curr_idx_ != size_; }

  /**
   * Advance the iterator to the next TID. If the iterator has exhausted all TIDS in the list,
   * TupleIdListIterator::HasNext() will return false after this call completes.
   */
  void Advance() { curr_idx_++; }

  /**
   * Reset the iterator to the start of the list.
   */
  void Reset() {
    curr_idx_ = 0;
    size_ = tid_list_->ToSelectionVector(sel_vector_);
  }

  /**
   * @return The current TID.
   */
  sel_t GetCurrentTupleId() const { return sel_vector_[curr_idx_]; }

 private:
  // The list we're iterating over
  const TupleIdList *tid_list_;

  // The materialized selection vector
  sel_t sel_vector_[common::Constants::K_DEFAULT_VECTOR_SIZE];
  sel_t size_;
  sel_t curr_idx_;
};

}  // namespace noisepage::execution::sql
