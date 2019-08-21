#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "common/constants.h"
#include "common/hash_util.h"
#include "common/json.h"
#include "common/macros.h"
#include "common/strong_typedef.h"
#include "parser/expression/abstract_expression.h"
#include "planner/plannodes/plan_node_defs.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"
#include "type/type_util.h"

namespace terrier::planner {

/**
 * TODO(Amadou): I modified this object to make more convenient for the execution engine.
 * Technically, all we need to execute a query is a list of columns type along with the expressions used to
 * generate them.
 * The col_oids, col_names are not that useful, and may not even exist for intermediate columns.
 * Also this makes it easier to make columns that are functions of multiple other columns.
 * The optimizer might need to change it though.
 */
class OutputSchema {
 public:
  /**
   * This object contains output columns of a plan node which a type along with an expression to generate the column.
   */
  class Column {
   public:
    /**
     * Instantiates a Column object.
     * TODO(Amadou): Given that the expressions already have a return type, passing in a type may be redundant.
     * @param type SQL type for this column
     * @param nullable is column nullable
     * @param expr the expression used to generate this column
     */
    Column(const type::TypeId type, const bool nullable, std::shared_ptr<parser::AbstractExpression> expr)
        : type_(type), nullable_(nullable), expr_(std::move(expr)) {
      TERRIER_ASSERT(type_ != type::TypeId::INVALID, "Attribute type cannot be INVALID.");
    }

    /**
     * Default constructor used for deserialization
     */
    Column() = default;

    /**
     * @return SQL type for this column
     */
    type::TypeId GetType() const { return type_; }
    /**
     * @return true if the column is nullable, false otherwise
     */
    bool GetNullable() const { return nullable_; }
    /**
     * @return the hashed value for this column.
     */
    common::hash_t Hash() const {
      common::hash_t hash = common::HashUtil::Hash(type_);
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(nullable_));
      if (expr_ != nullptr) {
        hash = common::HashUtil::CombineHashes(hash, expr_->Hash());
      }
      return hash;
    }

    const parser::AbstractExpression *GetExpr() const { return expr_.get(); }

    /**
     * @return whether the two columns are equal
     */
    bool operator==(const Column &rhs) const {
      if ((expr_ == nullptr && rhs.expr_ != nullptr) || (expr_ != nullptr && rhs.expr_ == nullptr)) {
        return false;
      }
      if (expr_ != nullptr && *expr_ != *rhs.expr_) {
        return false;
      }
      return type_ == rhs.type_ && nullable_ == rhs.nullable_;
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
    nlohmann::json ToJson() const {
      nlohmann::json j;
      j["type"] = type_;
      j["nullable"] = nullable_;
      j["expr"] = expr_;
      return j;
    }

    /**
     * @param j json to deserialize
     */
    void FromJson(const nlohmann::json &j) {
      type_ = j.at("type").get<type::TypeId>();
      nullable_ = j.at("nullable").get<bool>();
      if (!j.at("expr").is_null()) {
        expr_ = parser::DeserializeExpression(j.at("expr"));
      }
    }

   private:
    type::TypeId type_;
    bool nullable_;
    std::shared_ptr<parser::AbstractExpression> expr_;
  };

  /**
   * Instantiates a OutputSchema.
   * @param columns collection of columns
   */
  explicit OutputSchema(std::vector<Column> columns) : columns_(std::move(columns)) {
    TERRIER_ASSERT(!columns_.empty() && columns_.size() <= common::Constants::MAX_COL,
                   "Number of columns must be between 1 and MAX_COL.");
  }

  /**
   * Copy constructs an OutputSchema.
   * @param other the OutputSchema to be copied
   */
  OutputSchema(const OutputSchema &other) = default;

  /**
   * Default constructor for deserialization
   */
  OutputSchema() = default;

  /**
   * @param col_idx offset into the schema specifying which Column to access
   * @return description of the schema for a specific column
   */
  Column GetColumn(const uint32_t col_idx) const {
    TERRIER_ASSERT(col_idx < columns_.size(), "column id is out of bounds for this Schema");
    return columns_[col_idx];
  }
  /**
   * @return the vector of columns that are part of this schema
   */
  const std::vector<Column> &GetColumns() const { return columns_; }

  /**
   * Make a copy of this OutputSchema
   * @return shared pointer to the copy
   */
  std::shared_ptr<OutputSchema> Copy() const { return std::make_shared<OutputSchema>(*this); }

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
  bool operator==(const OutputSchema &rhs) const { return (columns_ == rhs.columns_); }

  /**
   * Inequality check
   * @param rhs other
   * @return true if the two OutputSchema are not equal
   */
  bool operator!=(const OutputSchema &rhs) const { return !operator==(rhs); }

  /**
   * @return derived column serialized to json
   */
  nlohmann::json ToJson() const {
    nlohmann::json j;
    j["columns"] = columns_;
    return j;
  }

  /**
   * @param j json to deserialize
   */
  void FromJson(const nlohmann::json &j) { columns_ = j.at("columns").get<std::vector<Column>>(); }

 private:
  std::vector<Column> columns_;
};

DEFINE_JSON_DECLARATIONS(OutputSchema::Column);
DEFINE_JSON_DECLARATIONS(OutputSchema);

}  // namespace terrier::planner
