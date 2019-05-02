#pragma once

#include <string>
#include <utility>
#include <vector>

#include "execution/sql/data_types.h"
#include "execution/sql/sql.h"

namespace tpl::sql {

/// A class to capture the physical schema layout
class Schema {
 public:
  struct ColumnInfo {
    std::string name;
    const Type &type;
    ColumnEncoding encoding;

    ColumnInfo(std::string name, const Type &type,
               ColumnEncoding encoding = ColumnEncoding::None)
        : name(std::move(name)), type(type), encoding(encoding) {}

    // TODO(pmenon): Fix me to change based on encoding
    u32 StorageSize() const {
      switch (type.type_id()) {
        case TypeId::Boolean: {
          return sizeof(i8);
        }
        case TypeId::SmallInt: {
          return sizeof(i16);
        }
        case TypeId::Date:
        case TypeId::Integer: {
          return sizeof(i32);
        }
        case TypeId::BigInt: {
          return sizeof(i64);
        }
        case TypeId::Decimal: {
          return sizeof(i128);
        }
        case TypeId::Char: {
          auto *char_type = type.As<CharType>();
          return char_type->length() * sizeof(i8);
        }
        case TypeId::Varchar: {
          return 16;
        }
        default: {
          TPL_UNLIKELY("Impossible type");
          return 0;
        }
      }
    }
  };

  explicit Schema(std::vector<ColumnInfo> &&cols) : cols_(std::move(cols)) {}

  const ColumnInfo *GetColumnInfo(u32 col_idx) const { return &cols_[col_idx]; }

  u32 num_columns() const { return static_cast<u32>(columns().size()); }

  const std::vector<ColumnInfo> &columns() const { return cols_; }

 private:
  // The metadata for each column. This is immutable after construction.
  const std::vector<ColumnInfo> cols_;
};

}  // namespace tpl::sql
