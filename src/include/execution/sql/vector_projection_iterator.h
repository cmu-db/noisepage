#pragma once

#include <algorithm>
#include <array>
#include <initializer_list>
#include <limits>
#include <type_traits>

#include "common/constants.h"
#include "common/macros.h"
#include "execution/sql/tuple_id_list.h"
#include "execution/sql/vector_projection.h"
#include "execution/util/bit_util.h"

namespace noisepage::execution::sql {

/**
 * A tuple-at-a-time iterator over VectorProjections. The iterator gives the <i>view</i> of
 * individual tuple access, but does not physically materialize full tuples in memory. Tuples can
 * be filtered out of the underlying projection (again without moving or copying any data) through
 * VectorProjectionIterator::Match() which considers the tuple that the iterator is positioned at.
 *
 * A VectorProjectionIterator must be constructed with a VectorProjection that's to be iterated.
 * Iteration occurs over all active/visible tuples in the underlying projection. Tuples which are
 * filtered out during iteration are immediately reflected in the underlying VectorProjection in
 * its filtered TID list.
 *
 * If a VectorProjectionIterator is constructed with both a VectorProjection and a TupleIdList, only
 * tuples whose TIDs are in the input list are visited during iteration. Tuples which are filtered
 * out during iteration are removed from the provided TupleIdList; the vector projection's filtered
 * TID list is not modified.
 *
 * In the example below, the vector projection is filtered.
 * @code
 * // vector_proj.IsFiltered() = false
 * auto iter = VectorProjectionIterator(vector_proj);
 * iter.RunFilter([] { .. filter logic ... });
 * // vector_proj.IsFiltered() = true
 * @endcode
 *
 * In the example below, the results of the filter are preserved in the input list. The vector
 * projection is unmodified!
 * @code
 * // vector_proj.IsFiltered() = false
 * auto iter = VectorProjectionIterator(vector_proj, tid_list);
 * iter.RunFilter([] { .. filter logic ... });
 * // vector_proj.IsFiltered() = false
 * // tid_list contains TIDs of all valid tuples
 * @endcode
 *
 * <h3>Iteration API</h3>:
 *
 * A different iteration API exists depending on whether the underlying vector projection has been
 * filtered or not. Users must query filtration status before iteration through
 * VectorProjectionIterator::IsFiltered().
 *
 * @code
 * VectorProjectionIterator iter = ...
 * if (iter.IsFiltered()) {
 *   for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
 *     // do work
 *   }
 * } else {
 *   for (; iter.HasNext(); iter.Advance()) {
 *     // do work
 *   }
 * }
 * @endcode
 *
 * The above template exists only for TPL programs, since lambdas don't exist there. For you regular
 * C++ folks, use VectorProjectionIterator::ForEach().
 */
class VectorProjectionIterator {
  using SelectionVector = std::array<sel_t, common::Constants::K_DEFAULT_VECTOR_SIZE>;

  // A full selection vector containing all indexes in [0,common::Constants::K_DEFAULT_VECTOR_SIZE].
  static constexpr SelectionVector FULL_INCREMENTAL_SELECTION_VECTOR = []() noexcept {
    SelectionVector ret{};
    for (sel_t i = 0; i < common::Constants::K_DEFAULT_VECTOR_SIZE; i++) ret[i] = i;
    return ret;
  }
  ();

 public:
  /**
   * Create an empty iterator over an empty projection.
   */
  VectorProjectionIterator() = default;

  /**
   * Create an iterator over the given projection.
   * @param vector_projection The projection to iterate.
   */
  explicit VectorProjectionIterator(VectorProjection *vector_projection) : VectorProjectionIterator() {
    SetVectorProjection(vector_projection);
  }

  /**
   * Create an iterator over the given projection, but only iterate over the TIDs in the given list.
   * Update the TID list if any tuples in the projection are unmatched.
   * @param vector_projection The projection to iterate.
   * @param tid_list The list of TIDs to iterate the projection with.
   */
  VectorProjectionIterator(VectorProjection *vector_projection, TupleIdList *tid_list) : VectorProjectionIterator() {
    SetVectorProjection(vector_projection, tid_list);
  }

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(VectorProjectionIterator);

  /**
   * @return True if the iterator is empty.
   */
  bool IsEmpty() const { return GetSelectedTupleCount() == 0; }

  /**
   * @return True if the vector projection we're iterating over is filtered; false otherwise.
   */
  bool IsFiltered() const { return !tid_list_->IsFull(); }

  /**
   * Reset this iterator to begin iteration over the given projection @em vector_projection.
   * @param vector_projection The vector projection to iterate over.
   */
  void SetVectorProjection(VectorProjection *vector_projection) {
    SetVectorProjection(vector_projection, &vector_projection->owned_tid_list_);
  }

  /**
   * Reset this iterator to begin iteration over @em vector_projection, but only over the tuples
   * contained in the TID list @em list.
   * @param vector_projection THe projection to iterate over.
   * @param tid_list The list of TIDs to iterate over.
   */
  void SetVectorProjection(VectorProjection *vector_projection, TupleIdList *tid_list) {
    Init(vector_projection, tid_list);
  }

  /**
   * @return The vector projection that we're iterating over.
   */
  VectorProjection *GetVectorProjection() const { return vector_projection_; }

  /**
   * Get a pointer to the value in the column at index @em col_idx.
   * @tparam T The desired data type stored in the vector projection.
   * @tparam nullable Whether the column is NULL-able.
   * @param col_idx The index of the column to read from.
   * @param[out] null null Whether the given column is null.
   * @return The typed value at the current iterator position in the column.
   */
  template <typename T, bool Nullable>
  const T *GetValue(uint32_t col_idx, bool *null) const;

  /**
   * Set the value of the column at index @em col_idx for the tuple the iterator is currently
   * positioned at to @em val. If the column is NULL-able, the NULL bit is also set to the provided
   * NULL value @em null.
   * @tparam T The desired primitive data type of the column.
   * @tparam Nullable Whether the column is NULL-able.
   * @param col_idx The index of the column to write to.
   * @param val The value to write.
   * @param null Whether the value is NULL.
   */
  template <typename T, bool Nullable>
  void SetValue(uint32_t col_idx, T val, bool null);

  /**
   * @return The current position in the vector projection.
   */
  sel_t GetPosition() const;

  /**
   * Set the current iterator position.
   * @tparam IsFiltered Is this VPI filtered?
   * @param idx The index the iteration should jump to
   */
  template <bool IsFiltered>
  void SetPosition(uint32_t idx);

  /**
   * Advance the iterator by one tuple.
   */
  void Advance();

  /**
   * Advance the iterator by one to the next valid tuple in the filtered projection.
   */
  void AdvanceFiltered() { Advance(); }

  /**
   * Mark the tuple this iterator is currently positioned at as valid or invalid.
   * @param matched True if the current tuple is valid; false otherwise
   */
  void Match(bool matched);

  /**
   * Does the iterator have another tuple?
   * @return True if there is more input tuples; false otherwise
   */
  bool HasNext() const;

  /**
   * Does the iterator have another tuple after the filter has been applied?
   * @return True if there is more input tuples; false otherwise
   */
  bool HasNextFiltered() const { return HasNext(); }

  /**
   * Reset iteration to the beginning of the vector projection.
   */
  void Reset();

  /**
   * Reset iteration to the beginning of the filtered vector projection.
   */
  void ResetFiltered() { Reset(); }

  /**
   * Run a functor over all active tuples in the vector projection.
   * @warning While non-const, the callback functor should treat this as a const method and avoid
   *          mutating the iterator during iteration.
   * @tparam F Functor whose signature is equivalent to: <code>void f();</code>
   * @param f A callback functor.
   */
  template <typename F>
  void ForEach(F f);

  /**
   * Run a functor simultaneously over all active tuples in all provided input iterators.
   * @tparam F Functor whose signature is equivalent to: <code>void f();</code>
   * @param iters The iterators to run the functor over.
   * @param f A callback functor.
   */
  template <typename F>
  static void SynchronizedForEach(std::initializer_list<VectorProjectionIterator *> iters, F f);

  /**
   * Run a tuple-at-a-time predicate over all active tuples in the vector projection.
   * @tparam P Predicate functor whose signature is equivalent to: <code>bool f();</code>
   * @param p The predicate functor that returns a boolean indicating if the current tuple is valid.
   */
  template <typename P>
  void RunFilter(P p);

  /**
   * @return The number of selected tuples after any filters have been applied.
   */
  uint32_t GetSelectedTupleCount() const { return vector_projection_->GetSelectedTupleCount(); }

  /**
   * @return The total number of tuples in the projection, including filtered out tuples.
   */
  uint32_t GetTotalTupleCount() const { return vector_projection_->GetTotalTupleCount(); }

  /** @return The vector projection's current tuple slot. */
  storage::TupleSlot GetCurrentSlot() const { return vector_projection_->GetTupleSlot(GetPosition()); }

 private:
  void Init(VectorProjection *vector_projection, TupleIdList *tid_list) {
    NOISEPAGE_ASSERT(vector_projection != nullptr, "NULL projection");
    NOISEPAGE_ASSERT(tid_list != nullptr, "NULL TID list");

    vector_projection_ = vector_projection;

    tid_list_ = tid_list;

    if (!tid_list_->IsFull()) {
      size_ = tid_list_->ToSelectionVector(sel_vector_.data());
    } else {
      sel_vector_ = FULL_INCREMENTAL_SELECTION_VECTOR;
      size_ = tid_list_->GetCapacity();
    }

    sel_vector_read_idx_ = 0;
    sel_vector_write_idx_ = 0;
  }

 public:
  /** The vector projection we're iterating over. */
  VectorProjection *vector_projection_{nullptr};

  /**
   * The list of TIDs to iterate over in the projection.
   * This list is also updated when iteration is filtered.
   */
  TupleIdList *tid_list_{nullptr};

  /**
   * The selection vector used to filter the vector projection.
   * This is a materialized copy of the vector projection's tuple ID list!
   * Because it's a cached copy, VPI ensures the two are kept in sync.
   */
  SelectionVector sel_vector_{0};

  /**
   * The number of elements in the projection.
   * If filtered, size is the number of elements in the selection vector.
   * Otherwise, it is the total number of elements in the vector projection.
   */
  sel_t size_{0};

  /** The next slot in the selection vector to read from. */
  sel_t sel_vector_read_idx_{0};

  /** The next slot in the selection vector to write into. */
  sel_t sel_vector_write_idx_{0};
};

// ---------------------------------------------------------
//
// Implementation below
//
// ---------------------------------------------------------

// The below methods are inlined in the header on purpose for performance.
// Please do not move them unless you know what you're doing.

// Note: The getting and setter functions operate on the underlying vector's raw
// data rather than going through Vector::GetValue() or Vector::SetValue(). This
// assumes the user is aware of the underlying vector's type and its NULL-ness
// property. We take advantage of that here by offering templatized accessor
// functions optimized for both NULL and non-NULL cases.

template <typename T, bool Nullable>
inline const T *VectorProjectionIterator::GetValue(uint32_t col_idx, bool *null) const {
  // The vector we'll read from
  const Vector *col_vector = vector_projection_->GetColumn(col_idx);
  // The current position in the projection.
  const sel_t curr_idx = GetPosition();

  if constexpr (Nullable) {  // NOLINT
    NOISEPAGE_ASSERT(null != nullptr, "Missing output variable for NULL indicator");
    *null = col_vector->null_mask_[curr_idx];
  }

  const T *RESTRICT data = reinterpret_cast<const T *>(col_vector->data_);
  return &data[curr_idx];
}

template <typename T, bool Nullable>
inline void VectorProjectionIterator::SetValue(uint32_t col_idx, const T val, bool null) {
  // The vector we'll write into
  Vector *col_vector = vector_projection_->GetColumn(col_idx);
  // The current position in the projection.
  const sel_t curr_idx = GetPosition();

  // If the column is NULL-able, we check the NULL indication flag before
  // writing into the columns's underlying data array. If the column isn't
  // NULL-able, we can skip the NULL check and directly write into the column
  // data array.

  if constexpr (Nullable) {
    col_vector->null_mask_[curr_idx] = null;
    if (!null) {
      reinterpret_cast<T *>(col_vector->data_)[curr_idx] = val;
    }
  } else {  // NOLINT
    reinterpret_cast<T *>(col_vector->data_)[curr_idx] = val;
  }
}

inline sel_t VectorProjectionIterator::GetPosition() const { return sel_vector_[sel_vector_read_idx_]; }

template <bool Filtered>
inline void VectorProjectionIterator::SetPosition(uint32_t idx) {
  NOISEPAGE_ASSERT(idx < GetSelectedTupleCount(), "Out of bounds access");
  sel_vector_read_idx_ = idx;
}

inline void VectorProjectionIterator::Advance() { sel_vector_read_idx_++; }

inline void VectorProjectionIterator::Match(bool matched) {
  // Update the cached selection vector
  const sel_t curr_idx = GetPosition();
  sel_vector_[sel_vector_write_idx_] = curr_idx;
  sel_vector_write_idx_ += static_cast<uint32_t>(matched);

  // Update the TID list
  tid_list_->Enable(curr_idx, matched);
}

inline bool VectorProjectionIterator::HasNext() const { return sel_vector_read_idx_ < size_; }

inline void VectorProjectionIterator::Reset() {
  // Update the projection counts
  vector_projection_->RefreshFilteredTupleIdList();

  // Reset index positions
  size_ = tid_list_->GetTupleCount();
  sel_vector_read_idx_ = 0;
  sel_vector_write_idx_ = 0;
}

template <typename F>
inline void VectorProjectionIterator::ForEach(F f) {
  // Ensure callback conforms to expectation
  static_assert(std::is_invocable_r_v<void, F>, "Callback must be a no-arg void-return function");

  if (IsFiltered()) {
    for (; HasNextFiltered(); AdvanceFiltered()) {
      f();
    }
  } else {
    for (; HasNext(); Advance()) {
      f();
    }
  }

  Reset();
}

// static
template <typename F>
inline void VectorProjectionIterator::SynchronizedForEach(std::initializer_list<VectorProjectionIterator *> iters,
                                                          F f) {
  // Ensure callback conforms to expectation
  static_assert(std::is_invocable_r_v<void, F>, "Callback must be a no-arg void-return function");

  // Either all provided iterators are filtered or non are.
  NOISEPAGE_ASSERT(std::all_of(iters.begin(), iters.end(), [](auto vpi) { return vpi->IsFiltered(); }) ||
                       std::none_of(iters.begin(), iters.end(), [](auto vpi) { return vpi->IsFiltered(); }),
                   "All iterators must have the same filtration status");

  // No-op if list is empty.
  if (iters.size() == 0) {
    return;
  }

  // Fire.
  if ((*iters.begin())->IsFiltered()) {
    for (; std::all_of(iters.begin(), iters.end(), [](auto vpi) { return vpi->HasNextFiltered(); });
         std::for_each(iters.begin(), iters.end(), [](auto vpi) { vpi->AdvanceFiltered(); })) {
      f();
    }
  } else {
    for (; std::all_of(iters.begin(), iters.end(), [](auto vpi) { return vpi->HasNext(); });
         std::for_each(iters.begin(), iters.end(), [](auto vpi) { vpi->Advance(); })) {
      f();
    }
  }
}

template <typename P>
inline void VectorProjectionIterator::RunFilter(P p) {
  // Ensure filter function conforms to expected form
  static_assert(std::is_invocable_r_v<bool, P>, "Predicate must take no arguments and return bool");

  if (IsFiltered()) {
    for (; HasNextFiltered(); AdvanceFiltered()) {
      Match(p());
    }
  } else {
    for (; HasNext(); Advance()) {
      Match(p());
    }
  }

  ResetFiltered();
}

}  // namespace noisepage::execution::sql
