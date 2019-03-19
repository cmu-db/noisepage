#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "common/constants.h"
#include "common/hash_util.h"
#include "common/macros.h"
#include "common/strong_typedef.h"
#include "parser/expression/abstract_expression.h"
#include "plan_node/plan_node_defs.h"
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
    Column(std::string name, const type::TypeId type, const bool nullable, const catalog::col_oid_t oid)
        : name_(std::move(name)), type_(type), nullable_(nullable), oid_(oid) {
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
     * @return true if the column is nullable, false otherwise
     */
    bool GetNullable() const { return nullable_; }
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
    const bool nullable_;
    const catalog::col_oid_t oid_;
  };

  /**
   * An intermediate column produced by plan nodes
   */
  struct DerivedColumn {
    Column column_;                                     // Intermediate column
    std::shared_ptr<parser::AbstractExpression> expr_;  // The expression used to derive the intermediate column

    /**
     * Instantiate a derived column
     * @param column an intermediate column
     * @param expr the expression used to derive the intermediate column
     */
    DerivedColumn(Column column, std::shared_ptr<parser::AbstractExpression> expr)
        : column_(std::move(column)), expr_(std::move(expr)) {}

    /**
     * Hash the current DerivedColumn.
     */
    common::hash_t Hash() const {
      common::hash_t hash = common::HashUtil::Hash(column_);
      hash = common::HashUtil::CombineHashes(hash, expr_->Hash());
      return hash;
    }
  };

  /**
   * Define a mapping of an offset into a vector of columns of an OutputSchema to an intermediate column produced by a
   * plan node
   */
  using DerivedTarget = std::pair<uint32_t, const DerivedColumn>;

  /**
   * Generic specification of a direct map between the columns of two output schema
   *        < NEW_offset , <tuple_index (left or right tuple), OLD_offset>    >
   */
  using DirectMap = std::pair<uint32_t, std::pair<uint32_t, uint32_t>>;

  /**
   * Instantiates a OutputSchema object from a vector of previously-defined Columns
   * @param columns collection of columns
   * @param targets mapping of intermediate columns (DerivedColumn) to the collection of columns
   * @param direct_map_list direct mapping of columns, in terms of offsets, from the child plan node's OutputSchema to
   * this plan node's OutputSchema
   *
   */
  explicit OutputSchema(std::vector<Column> columns, std::vector<DerivedTarget> targets = std::vector<DerivedTarget>(),
                        std::vector<DirectMap> direct_map_list = std::vector<DirectMap>())
      : columns_(std::move(columns)), targets_(std::move(targets)), direct_map_list_(std::move(direct_map_list)) {
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
   * Hash the current OutputSchema.
   */
  common::hash_t Hash() const {
    common::hash_t hash = common::HashUtil::Hash(columns_.size());
    for (auto const &column : columns_) {
      hash = common::HashUtil::CombineHashes(hash, column.Hash());
    }
    for (auto const &target : targets_) {
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(target.first));
      hash = common::HashUtil::CombineHashes(hash, target.second.Hash());
    }
    for (auto const &direct_map : direct_map_list_) {
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(direct_map.first));
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(direct_map.second.first));
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(direct_map.second.second));
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
  /**
   * The mapping of input columns to output columns is stored in two parts:
   * 1) A target_list stores non-trivial projections that can be calculated from
   *    expressions.
   * 2) A direct_map_list stores projections that is simply reorder of attributes
   *    in the input.
   *
   * We separate it in this way for two reasons:
   * i)  Postgres does the same thing;
   * ii) It makes it possible to use a more efficient executor to handle pure
   *     direct map projections.
   *
   * The input columns can be either:
   * 1) Part of the OutputSchema of a child plan node.
   * 2) Columns provided in INSERT, UPDATE, DELETE statements.
   *
   * NB: in case of a constant-valued projection, it is still under the umbrella
   * of target_list, though it sounds simple enough.
   */
  const std::vector<Column> columns_;
  const std::vector<DerivedTarget> targets_;
  const std::vector<DirectMap> direct_map_list_;
};
}  // namespace terrier::plan_node
