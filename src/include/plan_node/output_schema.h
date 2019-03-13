#pragma once
#include <string>
#include <utility>
#include <vector>
#include "common/constants.h"
#include "common/hash_util.h"
#include "common/macros.h"
#include "common/strong_typedef.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"
#include "type/type_util.h"

namespace terrier::plan_node {

/**
 * Internal object for representing output columns of a plan node. This object is to be differentiated from
 * catalog::Schema, which contains all columns of a table. This object can contain partial
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
     * @param oid internal unique identifier for this column
     */
    Column(std::string name, const type::TypeId type, const catalog::col_oid_t oid)
        : name_(std::move(name)), type_(type), oid_(oid) {
      TERRIER_ASSERT(type_ != type::TypeId::INVALID, "Attribute type cannot be INVALID.");
    }
    /**
     * @return column name
     */
    const std::string &GetName() const { return name_; }
    /**
     * @return SQL type for this column
     */
    type::TypeId GetType() const { return type_; }
    /**
     * @return internal unique identifier for this column
     */
    catalog::col_oid_t GetOid() const { return oid_; }
    /**
     * @return the hashed value for this column based on name and OID
     */
    common::hash_t Hash() const {
      common::hash_t hash = common::HashUtil::Hash(name_);
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(oid_));
      return hash;
    }
    /**
     * @return whether the two columns are equal
     */
    bool operator==(const Column &rhs) const { return name_ == rhs.name_ && type_ == rhs.type_ && oid_ == rhs.oid_; }
    /**
     * Inequality check
     * @param rhs other
     * @return true if the two columns are not equal
     */
    bool operator!=(const Column &rhs) const { return !operator==(rhs); }

   private:
    const std::string name_;
    const type::TypeId type_;
    const catalog::col_oid_t oid_;
  };

  /**
   * Instantiates a OutputSchema object from a vector of previously-defined Columns
   * @param collection of columns
   */
  explicit OutputSchema(std::vector<Column> columns) : columns_(std::move(columns)) {
    TERRIER_ASSERT(!columns_.empty() && columns_.size() <= common::Constants::MAX_COL,
                   "Number of columns must be between 1 and 32767.");
  }

  /**
   * Copy constructs an OutputSchema.
   * @param other the OutputSchema to be copied
   */
  OutputSchema(const OutputSchema &other) = default;

  /**
   * @param col_id offset into the schema specifying which Column to access
   * @return description of the schema for a specific column
   */
  Column GetColumn(const storage::col_id_t col_id) const {
    TERRIER_ASSERT((!col_id) < columns_.size(), "column id is out of bounds for this Schema");
    return columns_[!col_id];
  }
  /**
   * @return the vector of columns that are part of this schema
   */
  const std::vector<Column> &GetColumns() const { return columns_; }

  /**
   * Hashes the current OutputSchema.
   */
  common::hash_t Hash() const {
    common::hash_t hash = common::HashUtil::Hash(columns_.size());
    for (auto const &column : columns_) {
      hash = common::HashUtil::CombineHashes(hash, column.Hash());
    }
    return hash;
  }

  /**
   * Equality check.
   * @param rhs other
   * @return true if the two OutputSchema are the same
   */
  bool operator==(const OutputSchema &rhs) const {
    if (columns_.size() != rhs.columns_.size()) {
      return false;
    }
    for (size_t i = 0; i < columns_.size(); i++) {
      if (columns_[i] != rhs.columns_[i]) {
        return false;
      }
    }
    return true;
  }

  /**
     * Inequality check
     * @param rhs other
     * @return true if the two OutputSchema are not equal
     */
  bool operator!=(const OutputSchema &rhs) const { return !operator==(rhs); }

  /**
   * Make a copy of this OutputSchema
   * @return shared pointer to the copy
   */
  std::shared_ptr<OutputSchema> Copy() const { return std::make_shared<OutputSchema>(*this); }

 private:
  const std::vector<Column> columns_;
};
}  // namespace terrier::plan_node
