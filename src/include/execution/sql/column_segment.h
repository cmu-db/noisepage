#pragma once

#include "execution/sql/data_types.h"
#include "execution/util/bit_util.h"
#include "execution/util/common.h"
#include "execution/util/macros.h"

namespace tpl::sql {

/// A column segment represents a compact array of column values along with a
/// dense positionally aligned bitmap indicating whether the column value is
/// NULL.
class ColumnSegment {
 public:
  ColumnSegment(const Type &type, byte *data, u32 *null_bitmap,
                u32 num_tuples) noexcept
      : type_(type),
        data_(data),
        null_bitmap_(null_bitmap),
        num_tuples_(num_tuples) {}

  ColumnSegment(ColumnSegment &&other) noexcept
      : type_(other.type_),
        data_(other.data_),
        null_bitmap_(other.null_bitmap_),
        num_tuples_(other.num_tuples_) {
    other.data_ = nullptr;
    other.null_bitmap_ = nullptr;
  }

  DISALLOW_COPY(ColumnSegment);

  ~ColumnSegment() {
    if (data_ != nullptr) {
      std::free(data_);
      std::free(null_bitmap_);
    }
  }

  /// Read the value of type \ref T at the given index within the column's data
  /// \tparam T The type of the value to read. We make no assumptions on copy
  /// \param idx
  /// \return
  template <typename T>
  const T &TypedAccessAt(u32 idx) const {
    TPL_ASSERT(idx < num_tuples(), "Invalid row index!");
    const T *typed_data = reinterpret_cast<const T *>(data_);
    return typed_data[idx];
  }

  /// Is the value at the given index NULL
  /// \param idx The index to check
  /// \return True if the value is null; false otherwise
  bool IsNullAt(u32 idx) const {
    return util::BitUtil::Test(null_bitmap_, idx);
  }

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  const Type &type() const { return type_; }

  u32 num_tuples() const { return num_tuples_; }

 private:
  friend class ColumnVectorIterator;

  auto *AccessRaw(u32 idx) const { return &data_[idx]; }

  auto *AccessRawNullBitmap(u32 idx) const { return &null_bitmap_[idx]; }

 private:
  const Type &type_;
  byte *data_;
  u32 *null_bitmap_;
  u32 num_tuples_;
};

}  // namespace tpl::sql
