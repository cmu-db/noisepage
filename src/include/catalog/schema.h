#pragma once
#include <string>
#include <utility>
#include <vector>
#include "common/constants.h"
#include "common/macros.h"
#include "common/typedefs.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"
#include "type/type_util.h"

namespace terrier::catalog {
class Schema {
 public:
  class Column {
   public:
    Column(std::string name, const type::TypeId type, const bool nullable, const col_oid_t oid)
        : name_(std::move(name)),
          type_(type),
          attr_size_(type::TypeUtil::GetTypeSize(type_)),
          nullable_(nullable),
          inlined_(true),
          oid_(oid) {
      if (attr_size_ == 0) {
        attr_size_ = 8;
        inlined_ = false;
      }
      TERRIER_ASSERT(attr_size_ == 1 || attr_size_ == 2 || attr_size_ == 4 || attr_size_ == 8,
                     "Attribute size must be 1, 2, 4, or 8 bytes.");
      TERRIER_ASSERT(type_ != type::TypeId::INVALID, "Attribute type cannot be INVALID.");
    }
    std::string GetName() const { return name_; }
    bool GetNullable() const { return nullable_; }
    uint8_t GetAttrSize() const { return attr_size_; }
    type::TypeId GetType() const { return type_; }

   private:
    const std::string name_;
    const type::TypeId type_;
    uint8_t attr_size_;
    const bool nullable_;
    bool inlined_;
    const col_oid_t oid_;
    // TODO(Matt): default value would go here
    // Value default_;
  };

  explicit Schema(std::vector<Column> columns) : columns_(std::move(columns)) {
    TERRIER_ASSERT(!columns_.empty() && columns_.size() <= common::Constants::MAX_COL,
                   "Number of columns must be between 1 and 32767.");
  }
  Column GetColumn(const col_id_t col_id) const { return columns_[static_cast<uint16_t>(col_id)]; }
  const std::vector<Column> &GetColumns() const { return columns_; }

 private:
  const std::vector<Column> columns_;
};
}  // namespace terrier::catalog
