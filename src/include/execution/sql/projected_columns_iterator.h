#pragma once

#include <limits>
#include <type_traits>
#include "storage/projected_columns.h"

#include "execution/util/bit_util.h"
#include "execution/util/common.h"
#include "execution/util/macros.h"
#include "type/type_id.h"

namespace tpl::sql {
using terrier::storage::ProjectedColumns;
using terrier::type::TypeId;
/// An iterator over ProjectedColumns. A ProjectedColumnsIterator allows both
/// tuple-at-a-time iteration over a ProjectedColumns and vector-at-a-time
/// processing. There are two separate APIs for each and interleaving is
/// supported only to a certain degree. This class exists so that we can iterate
/// over a ProjectedColumns multiples times and ensure processing always only
/// on filtered items.
class ProjectedColumnsIterator {
  static constexpr const u32 kInvalidPos = std::numeric_limits<u32>::max();

 public:
  /// Default Constructor. Does not set a ProjectedColumns
  ProjectedColumnsIterator();

  /// Constructor with a ProjectedColumns.
  explicit ProjectedColumnsIterator(ProjectedColumns *projected_column);

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(ProjectedColumnsIterator);

  /// Has this ProjectedColumns been filtered?
  /// \return True if filtered; false otherwise
  bool IsFiltered() const { return selection_vector_[0] != kInvalidPos; }

  /// Set the ProjectedColumns to iterate over
  /// \param projected_column The projected column
  void SetProjectedColumn(ProjectedColumns *projected_column);

  // -------------------------------------------------------
  // Tuple-at-a-time API
  // -------------------------------------------------------

  /// Get a pointer to the value in the column at index col_idx
  /// \tparam T The desired data type stored in the ProjectedColumns
  /// \tparam nullable Whether the column is NULLable
  /// \param col_idx The index of the column to read from
  /// \param[out] null Whether the given column is null
  /// \return The typed value at the current iterator position in the column
  template <typename T, bool nullable>
  const T *Get(u32 col_idx, bool *null) const;

  /// Advance the iterator by a single row
  void Advance();

  /// Advance the iterator by a single entry to the next valid tuple in the
  /// filtered ProjectedColumns
  void AdvanceFiltered();

  /// Mark the current tuple as matched/valid (or unmatched/invalid)
  /// \param matched True if the current tuple is matched; false otherwise
  void Match(bool matched);

  /// Does the iterator have another tuple?
  /// \return True if there is more input tuples; false otherwise
  bool HasNext() const;

  /// Does the iterator have another tuple after the filter has been applied
  /// \return True if there is more input tuples; false otherwise
  bool HasNextFiltered() const;

  /// Reset iteration to the beginning of the ProjectedColumns
  void Reset();

  /// Reset iteration to the beginning of the filtered ProjectedColumns
  void ResetFiltered();

  /// Fun a function over each active tuple in the ProjectedColumns. This is a
  /// read-only function (despite it being non-const), meaning the callback must
  /// not modify the state of the iterator, but should only query it using const
  /// functions!
  /// \param fn A callback function
  template <typename F>
  void ForEach(const F &fn);

  /// Run a generic tuple-at-a-time filter over all active tuples in the
  /// ProjectedColumns
  /// \tparam F The generic type of the filter function. This can be any
  /// functor-like type including raw function pointer, functor or std::function
  /// \param filter A function that accepts a const version of this PCI and
  /// returns true if the tuple pointed to by the PCI is valid (i.e., passes the
  /// filter) or false otherwise
  template <typename F>
  void RunFilter(const F &filter);

  // -------------------------------------------------------
  // Vectorized API
  // -------------------------------------------------------

  /// Union to store the filter value according to its type.
  union FilterVal {
    /// an i8 filter value
    i8 ti;
    /// an i16 filter value
    i16 si;
    /// an i32 filter value
    i32 i;
    /// an i64 filter value
    i64 bi;
  };

  /// Creates a filter value according to the given type.
  /// \param val filter value
  /// \param type type of the value
  FilterVal MakeFilterVal(i64 val, TypeId type) {
    switch (type) {
      case TypeId::TINYINT:
        return FilterVal{.ti = static_cast<i8>(val)};
      case TypeId::SMALLINT:
        return FilterVal{.si = static_cast<i16>(val)};
      case TypeId::INTEGER:
        return FilterVal{.i = static_cast<i32>(val)};
      case TypeId::BIGINT:
        return FilterVal{.bi = static_cast<i64>(val)};
      default:
        throw std::runtime_error("Filter not supported on type");
    }
  }

  /// Filter the given column by the given value
  /// \tparam Compare The comparison function
  /// \param col_idx The index of the column in projected column to filter
  /// \param type the type of filter value
  /// \param val The value to filter on
  /// \return The number of selected elements
  template <template <typename> typename Op>
  u32 FilterColByVal(u32 col_idx, terrier::type::TypeId type, FilterVal val);

  /// Return the number of selected tuples after any filters have been applied
  /// \return The number of selected tuples
  u32 num_selected() const { return num_selected_; }

 private:
  // Filter a column by a constant value
  template <typename T, template <typename> typename Op>
  u32 FilterColByValImpl(u32 col_idx, T val);

 private:
  // The projected column we are iterating over.
  ProjectedColumns *projected_column_{nullptr};

  // The current raw position in the ProjectedColumns we're pointing to
  u32 curr_idx_{0};

  // The number of tuples from the projection that have been selected (filtered)
  u32 num_selected_{0};

  // The selection vector used to filter the ProjectedColumns
  u32 selection_vector_[kDefaultVectorSize];

  // The next slot in the selection vector to read from
  u32 selection_vector_read_idx_{0};

  // The next slot in the selection vector to write into
  u32 selection_vector_write_idx_{0};
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

// The below methods are inlined in the header on purpose for performance.
// Please don't move them.

// Retrieve a single column value (and potentially its NULL indicator) from the
// desired column's input data
template <typename T, bool Nullable>
inline const T *ProjectedColumnsIterator::Get(u32 col_idx, bool *null) const {
  if constexpr (Nullable) {
    TPL_ASSERT(null != nullptr, "Missing output variable for NULL indicator");
    *null = !projected_column_->ColumnNullBitmap(static_cast<u16>(col_idx))->Test(curr_idx_);
  }
  const T *col_data = reinterpret_cast<T *>(projected_column_->ColumnStart(static_cast<u16>(col_idx)));
  return &col_data[curr_idx_];
}

inline void ProjectedColumnsIterator::Advance() { curr_idx_++; }

inline void ProjectedColumnsIterator::AdvanceFiltered() { curr_idx_ = selection_vector_[++selection_vector_read_idx_]; }

inline void ProjectedColumnsIterator::Match(bool matched) {
  selection_vector_[selection_vector_write_idx_] = curr_idx_;
  selection_vector_write_idx_ += matched ? 1 : 0;
}

inline bool ProjectedColumnsIterator::HasNext() const { return curr_idx_ < projected_column_->NumTuples(); }

inline bool ProjectedColumnsIterator::HasNextFiltered() const { return selection_vector_read_idx_ < num_selected(); }

inline void ProjectedColumnsIterator::Reset() {
  const auto next_idx = selection_vector_[0];
  curr_idx_ = (next_idx == kInvalidPos ? 0 : next_idx);
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
  // VPI, and subsequent filters operate only on valid tuples potentially
  // filtered out in this filter.
  ResetFiltered();
}

}  // namespace tpl::sql
