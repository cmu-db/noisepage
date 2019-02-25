#pragma once
#include <string>
#include <utility>
#include <vector>
#include "common/constants.h"
#include "common/macros.h"
#include "common/strong_typedef.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"
#include "type/type_util.h"

namespace terrier::plan_node {

/**
 * Internal object for representing output columns of a plan node. This object is to be differentiated from
 * catalog::Schema, which contains all columns of a table.
 */
class OutputSchema {
 public:
  /**
   * This object contains output columns of a plan node which can consist of columns that exist in the catalog
   * or intermediate columns.
   */
  class Column {
   public:
    /**
     * Instantiates a Column object, primary to be used for building a Schema object
     * @param name column name
     * @param type SQL type for this column
     * @param nullable true if the column is nullable, false otherwise
     * @param oid internal unique identifier for this column
     */
    Column(std::string name, const type::TypeId type, const bool nullable, const catalog::col_oid_t oid)
        : name_(std::move(name)),
          type_(type),
          attr_size_(type::TypeUtil::GetTypeSize(type_)),
          nullable_(nullable),
          inlined_(true),
          oid_(oid) {
      if (attr_size_ == VARLEN_COLUMN) {
        // this is a varlen attribute
        // attr_size_ is actual size + high bit via GetTypeSize
        inlined_ = false;
      }
      TERRIER_ASSERT(
          attr_size_ == 1 || attr_size_ == 2 || attr_size_ == 4 || attr_size_ == 8 || attr_size_ == VARLEN_COLUMN,
          "Attribute size must be 1, 2, 4, 8 or VARLEN_COLUMN bytes.");
      TERRIER_ASSERT(type_ != type::TypeId::INVALID, "Attribute type cannot be INVALID.");
    }
    /**
     * @return column name
     */
    const std::string &GetName() const { return name_; }
    /**
     * @return true if the column is nullable, false otherwise
     */
    bool GetNullable() const { return nullable_; }
    /**
     * @return size of the attribute in bytes. Varlen attributes have the sign bit set.
     */
    uint8_t GetAttrSize() const { return attr_size_; }
    /**
     * @return true if the attribute is inlined, false if it's a pointer to a varlen entry
     */
    bool GetInlined() const { return inlined_; }
    /**
     * @return SQL type for this column
     */
    type::TypeId GetType() const { return type_; }
    /**
     * @return internal unique identifier for this column
     */
    catalog::col_oid_t GetOid() const { return oid_; }

   private:
    const std::string name_;
    const type::TypeId type_;
    uint8_t attr_size_;
    const bool nullable_;
    bool inlined_;
    const catalog::col_oid_t oid_;
    // TODO(Matt): default value would go here
    // Value default_;
  };

  /**
   * Instantiates a OutputSchema object from a vector of previously-defined Columns
   * @param columns description of this SQL table's schema as a collection of Columns
   */
  explicit OutputSchema(std::vector<Column> columns) : columns_(std::move(columns)) {
    TERRIER_ASSERT(!columns_.empty() && columns_.size() <= common::Constants::MAX_COL,
                   "Number of columns must be between 1 and 32767.");
  }
  /**
   * @param col_id offset into the schema specifying which Column to access
   * @return description of the schema for a specific column
   */
  Column GetColumn(const storage::col_id_t col_id) const {
    TERRIER_ASSERT((!col_id) < columns_.size(), "column id is out of bounds for this Schema");
    return columns_[!col_id];
  }
  /**
   * @return description of this SQL table's schema as a collection of Columns
   */
  const std::vector<Column> &GetColumns() const { return columns_; }

 private:
  const std::vector<Column> columns_;
};
}  // namespace terrier::catalog
