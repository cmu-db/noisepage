#pragma once

#include <limits>
#include <type_traits>
#include "storage/projected_columns.h"

#include "common/macros.h"
#include "execution/util/bit_util.h"
#include "execution/util/execution_common.h"
#include "type/type_id.h"

namespace terrier::execution::sql {
/**
 * An iterator over projections. A ProjectedColumnsIterator allows both
 * tuple-at-a-time iteration over a vector projection and vector-at-a-time
 * processing. There are two separate APIs for each and interleaving is
 * supported only to a certain degree. This class exists so that we can iterate
 * over a projection multiples times and ensure processing always only
 * on filtered items.
 */
class EXPORT ProjectedColumnsIterator {
  static constexpr const uint32_t K_INVALID_POS = std::numeric_limits<uint32_t>::max();

 public:
  /**
   * Create an empty iterator over an empty projection
   */
  ProjectedColumnsIterator();

  /**
   * Create an iterator over the given projection @em projected_column
   * @param projected_column The projection to iterate over
   */
  explicit ProjectedColumnsIterator(storage::ProjectedColumns *projected_column);

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(ProjectedColumnsIterator);

  /**
   * Has this vector projection been filtered? Does it have a selection vector?
   * @return True if filtered; false otherwise.
   */
  bool IsFiltered() const { return selection_vector_[0] != K_INVALID_POS; }

  /**
   * Reset this iterator to begin iteration over the given projection @em projected_column
   * @param projected_column The projection to iterate over
   */
  void SetProjectedColumn(storage::ProjectedColumns *projected_column);

  // -------------------------------------------------------
  // Tuple-at-a-time API
  // -------------------------------------------------------

  /**
   * Get a pointer to the value in the column at index @em col_idx
   * @tparam T The desired data type stored in the vector projection
   * @tparam nullable Whether the column is NULLable
   * @param col_idx The index of the column to read from
   * @param[out] null null Whether the given column is null
   * @return The typed value at the current iterator position in the column
   */
  template <typename T, bool nullable>
  const T *Get(uint32_t col_idx, bool *null) const;

  /**
   * Set the current iterator position
   * @tparam IsFiltered Is this iterator filtered?
   * @param idx The index the iteration should jump to
   */
  template <bool IsFiltered>
  void SetPosition(uint32_t idx);

  /**
   * Advance the iterator by one tuple
   */
  void Advance();

  /**
   * Advance the iterator by a one tuple to the next valid tuple in the
   * filtered vector projection.
   */
  void AdvanceFiltered();

  /**
   * Mark the current tuple (i.e., the one the iterator is currently positioned
   * at) as matched (valid) or unmatched (invalid).
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
  bool HasNextFiltered() const;

  /**
   * Reset iteration to the beginning of the vector projection
   */
  void Reset();

  /**
   * Reset iteration to the beginning of the filtered vector projection
   */
  void ResetFiltered();

  /**
   * Run a function over each active tuple in the projection. This is a
   * read-only function (despite it being non-const), meaning the callback must
   * not modify the state of the iterator, but should only query it using const
   * functions!
   * @tparam F The function type
   * @param fn A callback function
   */
  template <typename F>
  void ForEach(const F &fn);

  /**
   * Run a generic tuple-at-a-time filter over all active tuples in the
   * projection
   * @tparam F The generic type of the filter function. This can be any
   *           functor-like type including raw function pointer, functor or
   *           std::function
   * @param filter A function that accepts a const version of this PCI and
   *               returns true if the tuple pointed to by the PCI is valid
   *               (i.e., passes the filter) or false otherwise
   */
  template <typename F>
  void RunFilter(const F &filter);

  // -------------------------------------------------------
  // Vectorized API
  // -------------------------------------------------------

  /**
   * Union to store the filter value according to its type.
   */
  union FilterVal {
    /**
     * an int8_t filter value
     */
    int8_t ti_;
    /**
     * an int16_t filter value
     */
    int16_t si_;
    /**
     * an int32_t filter value
     */
    int32_t i_;
    /**
     * an int64_t filter value
     */
    int64_t bi_;
  };

  /**
   * Creates a filter value according to the given type.
   * @param val filter value
   * @param type type of the value
   * @return filter val of the given type
   */
  FilterVal MakeFilterVal(int64_t val, type::TypeId type) {
    switch (type) {
      case type::TypeId::TINYINT:
        return FilterVal{.ti_ = static_cast<int8_t>(val)};
      case type::TypeId::SMALLINT:
        return FilterVal{.si_ = static_cast<int16_t>(val)};
      case type::TypeId::INTEGER:
        return FilterVal{.i_ = static_cast<int32_t>(val)};
      case type::TypeId::BIGINT:
        return FilterVal{.bi_ = static_cast<int64_t>(val)};
      default:
        throw std::runtime_error("Filter not supported on type");
    }
  }

  /**
   * Filter the column at index @em col_idx by the given constant value @em val.
   * @tparam Op The filtering operator.
   * @param col_idx The index of the column in the projection to filter.
   * @param type The type of the column.
   * @param val The value to filter on.
   * @return The number of selected elements.
   */
  template <template <typename> typename Op>
  uint32_t FilterColByVal(uint32_t col_idx, type::TypeId type, FilterVal val);

  /**
   * Filter the column at index @em col_idx_1 with the contents of the column
   * at index @em col_idx_2.
   * @tparam Op The filtering operator.
   * @param col_idx_1 The index of the first column to compare.
   * @param type_1 the Type of the first column.
   * @param col_idx_2 The index of the second column to compare.
   * @param type_2 The type of the second column.
   * @return The number of selected elements.
   */
  template <template <typename> typename Op>
  uint32_t FilterColByCol(uint32_t col_idx_1, type::TypeId type_1, uint32_t col_idx_2, type::TypeId type_2);

  /**
   * Return the number of selected tuples after any filters have been applied
   */
  uint32_t NumSelected() const { return num_selected_; }

 private:
  // Filter a column by a constant value
  template <typename T, template <typename> typename Op>
  uint32_t FilterColByValImpl(uint32_t col_idx, T val);

  // Filter a column by a second column
  template <typename T, template <typename> typename Op>
  uint32_t FilterColByColImpl(uint32_t col_idx_1, uint32_t col_idx_2);

 private:
  // The selection vector used to filter the ProjectedColumns
  alignas(common::Constants::CACHELINE_SIZE) uint32_t selection_vector_[common::Constants::K_DEFAULT_VECTOR_SIZE];

  // The projected column we are iterating over.
  storage::ProjectedColumns *projected_column_{nullptr};

  // The current raw position in the ProjectedColumns we're pointing to
  uint32_t curr_idx_{0};

  // The number of tuples from the projection that have been selected (filtered)
  uint32_t num_selected_{0};

  // The next slot in the selection vector to read from
  uint32_t selection_vector_read_idx_{0};

  // The next slot in the selection vector to write into
  uint32_t selection_vector_write_idx_{0};
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

// The below methods are inlined in the header on purpose for performance.
// Please don't move them.

// Retrieve a single column value (and potentially its NULL indicator) from the
// desired column's input data
template <typename T, bool Nullable>
inline const T *ProjectedColumnsIterator::Get(uint32_t col_idx, bool *null) const {
  // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
  if constexpr (Nullable) {
    TERRIER_ASSERT(null != nullptr, "Missing output variable for NULL indicator");
    *null = !projected_column_->ColumnNullBitmap(static_cast<uint16_t>(col_idx))->Test(curr_idx_);
  }
  const T *col_data = reinterpret_cast<T *>(projected_column_->ColumnStart(static_cast<uint16_t>(col_idx)));
  return &col_data[curr_idx_];
}

template <bool Filtered>
inline void ProjectedColumnsIterator::SetPosition(uint32_t idx) {
  TERRIER_ASSERT(idx < NumSelected(), "Out of bounds access");
  if constexpr (Filtered) {
    TERRIER_ASSERT(IsFiltered(), "Attempting to set position in unfiltered PCI");
    selection_vector_read_idx_ = idx;
    curr_idx_ = selection_vector_[selection_vector_read_idx_];
  } else {  // NOLINT
    TERRIER_ASSERT(!IsFiltered(), "Attempting to set position in filtered PCI");
    curr_idx_ = idx;
  }
}

inline void ProjectedColumnsIterator::Advance() { curr_idx_++; }

inline void ProjectedColumnsIterator::AdvanceFiltered() { curr_idx_ = selection_vector_[++selection_vector_read_idx_]; }

inline void ProjectedColumnsIterator::Match(bool matched) {
  selection_vector_[selection_vector_write_idx_] = curr_idx_;
  selection_vector_write_idx_ += matched ? 1 : 0;
}

inline bool ProjectedColumnsIterator::HasNext() const { return curr_idx_ < projected_column_->NumTuples(); }

inline bool ProjectedColumnsIterator::HasNextFiltered() const { return selection_vector_read_idx_ < NumSelected(); }

inline void ProjectedColumnsIterator::Reset() {
  const auto next_idx = selection_vector_[0];
  curr_idx_ = (next_idx == K_INVALID_POS ? 0 : next_idx);
  selection_vector_read_idx_ = 0;
  selection_vector_write_idx_ = 0;
}

inline void ProjectedColumnsIterator::ResetFiltered() {
  curr_idx_ = selection_vector_[0];
  num_selected_ = selection_vector_write_idx_;
  selection_vector_read_idx_ = 0;
  selection_vector_write_idx_ = 0;
}

template <typename F>
inline void ProjectedColumnsIterator::ForEach(const F &fn) {
  // Ensure function conforms to expected form
  static_assert(std::is_invocable_r_v<void, F>, "Iteration function must be a no-arg void-return function");

  if (IsFiltered()) {
    for (; HasNextFiltered(); AdvanceFiltered()) {
      fn();
    }
  } else {
    for (; HasNext(); Advance()) {
      fn();
    }
  }

  Reset();
}

template <typename F>
inline void ProjectedColumnsIterator::RunFilter(const F &filter) {
  // Ensure filter function conforms to expected form
  static_assert(std::is_invocable_r_v<bool, F>, "Filter function must be a no-arg function returning a bool");

  if (IsFiltered()) {
    for (; HasNextFiltered(); AdvanceFiltered()) {
      bool valid = filter();
      Match(valid);
    }
  } else {
    for (; HasNext(); Advance()) {
      bool valid = filter();
      Match(valid);
    }
  }

  // After the filter has been run on the entire projected column, we need to
  // ensure that we reset it so that clients can query the updated state of the
  // PCI, and subsequent filters operate only on valid tuples potentially
  // filtered out in this filter.
  ResetFiltered();
}

}  // namespace terrier::execution::sql
