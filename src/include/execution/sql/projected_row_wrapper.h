#pragma once
#include "execution/util/execution_common.h"
#include "storage/projected_row.h"

namespace noisepage::execution::sql {

/**
 * Wrapper around projected rows.
 * TODO(Amadou): This is not really necessary. The bytecode handlers can directly call the PR functions. But it's
 * nice to have it as a separate class for now.
 */
class EXPORT ProjectedRowWrapper {
 public:
  /**
   * Constructor
   */
  explicit ProjectedRowWrapper(storage::ProjectedRow *pr) : pr_{pr} {}

  /**
   * Get a pointer to the value in the column at index @em col_idx
   * @tparam T The desired data type stored in the vector projection
   * @tparam Nullable Whether the column is NULLable
   * @param col_idx The index of the column to read from
   * @param[out] null null Whether the given column is null
   * @return The typed value at the current iterator position in the column
   */
  template <typename T, bool Nullable>
  const T *Get(uint16_t col_idx, bool *null) const {
    // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
    if constexpr (Nullable) {
      NOISEPAGE_ASSERT(null != nullptr, "Missing output variable for NULL indicator");
      *null = pr_->IsNull(col_idx);
    }
    return reinterpret_cast<T *>(pr_->AccessWithNullCheck(col_idx));
  }

  /**
   * Sets the index key value at the given index
   * @tparam T type of value
   * @param col_idx index of the key
   * @param value value to write
   * @param null whether the value is null
   */
  template <typename T, bool Nullable>
  void Set(uint16_t col_idx, T value, bool null) {
    if constexpr (Nullable) {
      if (null) {
        pr_->SetNull(static_cast<uint16_t>(col_idx));
      } else {
        *reinterpret_cast<T *>(pr_->AccessForceNotNull(col_idx)) = value;
      }
    } else {  // NOLINT
      *reinterpret_cast<T *>(pr_->AccessForceNotNull(col_idx)) = value;
    }
  }

  /**
   * @return the projected row
   */
  storage::ProjectedRow *Get() { return pr_; }

 private:
  storage::ProjectedRow *pr_;
};
}  // namespace noisepage::execution::sql
