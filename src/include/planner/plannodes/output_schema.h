#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.h"
#include "common/hash_util.h"
#include "common/json_header.h"
#include "common/macros.h"
#include "common/strong_typedef.h"
#include "parser/expression/abstract_expression.h"
#include "planner/plannodes/plan_node_defs.h"
#include "planner/plannodes/plan_visitor.h"
#include "type/type_id.h"
#include "type/type_util.h"

namespace noisepage::planner {

/**
 * Internal object for representing output columns of a plan node. This object is to be differentiated from
 * catalog::Schema, which contains all columns of a table. This object can contain a subset of columns of a table.
 * This class is also meant to provide mapping information from a set of input columns to a set of output columns.
 */
class OutputSchema {
 public:
  /**
   * Describes a column output by a plan
   */
  class Column {
   public:
    /**
     * Instantiates a Column object, primary to be used for building a Schema object
     * @param name column name
     * @param type SQL type for this column
     * @param expr Expression
     */
    Column(std::string name, const type::TypeId type, std::unique_ptr<parser::AbstractExpression> expr)
        : name_(std::move(name)), type_(type), expr_(std::move(expr)) {
      NOISEPAGE_ASSERT(type_ != type::TypeId::INVALID, "Attribute type cannot be INVALID.");
    }

    /**
     * Default constructor used for deserialization
     */
    Column() = default;

    /**
     * Creates a copy of this column.
     */
    Column Copy() const { return Column(GetName(), GetType(), expr_->Copy()); }

    /**
     * @return column name
     */
    const std::string &GetName() const { return name_; }

    /**
     * @return SQL type for this column
     */
    type::TypeId GetType() const { return type_; }

    /**
     * @return expr
     */
    common::ManagedPointer<parser::AbstractExpression> GetExpr() const { return common::ManagedPointer(expr_.get()); }

    /**
     * @return the hashed value for this column based on name and OID
     */
    common::hash_t Hash() const {
      common::hash_t hash = common::HashUtil::Hash(name_);
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(type_));
      hash = common::HashUtil::CombineHashes(hash, expr_->Hash());
      return hash;
    }

    /**
     * @return whether the two columns are equal
     */
    bool operator==(const Column &rhs) const {
      // Name
      if (name_ != rhs.name_) return false;

      // Type
      if (type_ != rhs.type_) return false;

      return *expr_ == *rhs.expr_;
    }

    /**
     * Inequality check
     * @param rhs other
     * @return true if the two columns are not equal
     */
    bool operator!=(const Column &rhs) const { return !operator==(rhs); }

    /**
     * @return column serialized to json
     */
    nlohmann::json ToJson() const;

    /**
     * @param j json to deserialize
     */
    std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j);

   private:
    std::string name_;
    type::TypeId type_;
    std::unique_ptr<parser::AbstractExpression> expr_;
  };

  /**
   * Instantiates a OutputSchema object from a vector of previously-defined Columns
   * @param columns collection of columns
   */
  explicit OutputSchema(std::vector<Column> &&columns) : columns_(std::move(columns)) {}

  /**
   * Default constructor for deserialization
   */
  OutputSchema() = default;

  /**
   * @param col_id offset into the schema specifying which Column to access
   * @return description of the schema for a specific column
   */
  const Column &GetColumn(size_t col_id) const {
    NOISEPAGE_ASSERT(col_id < columns_.size(), "column id is out of bounds for this Schema");
    return columns_[col_id];
  }

  /**
   * @return the vector of columns that are part of this schema
   */
  const std::vector<Column> &GetColumns() const { return columns_; }

  /**
   * @return The number of output columns.
   */
  std::size_t NumColumns() const { return columns_.size(); }

  /**
   * Make a copy of this OutputSchema
   * @return unique pointer to the copy
   */
  std::unique_ptr<OutputSchema> Copy() const {
    std::vector<Column> columns;
    for (const auto &col : GetColumns()) {
      columns.emplace_back(col.Copy());
    }
    return std::make_unique<OutputSchema>(std::move(columns));
  }

  /**
   * Hash the current OutputSchema.
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
  bool operator==(const OutputSchema &rhs) const { return columns_ == rhs.columns_; }

  /**
   * Inequality check
   * @param rhs other
   * @return true if the two OutputSchema are not equal
   */
  bool operator!=(const OutputSchema &rhs) const { return !operator==(rhs); }

  /**
   * @return derived column serialized to json
   */
  nlohmann::json ToJson() const;

  /**
   * @param j json to deserialize
   */
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j);

 private:
  std::vector<Column> columns_;
};

DEFINE_JSON_HEADER_DECLARATIONS(OutputSchema::Column);
DEFINE_JSON_HEADER_DECLARATIONS(OutputSchema);

}  // namespace noisepage::planner
