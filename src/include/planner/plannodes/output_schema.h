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
 * Internal object for representing output columns of a plan node. This object is to be differentiated from
 * catalog::Schema, which contains all columns of a table. This object can contain a subset of columns of a table.
 * This class is also meant to provide mapping information from a set of input columns to a set of output columns.
 */
class OutputSchema {
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
     */
    Column(std::string name, const type::TypeId type) : name_(std::move(name)), type_(type) {
      TERRIER_ASSERT(type_ != type::TypeId::INVALID, "Attribute type cannot be INVALID.");
    }

    /**
     * Default constructor used for deserialization
     */
    Column() = default;

    /**
     * @return column name
     */
    const std::string &GetName() const { return name_; }

    /**
     * @return SQL type for this column
     */
    type::TypeId GetType() const { return type_; }

    /**
     * @return the hashed value for this column based on name and OID
     */
    common::hash_t Hash() const {
      common::hash_t hash = common::HashUtil::Hash(name_);
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(type_));
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

      return true;
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
      j["name"] = name_;
      j["type"] = type_;
      return j;
    }

    /**
     * @param j json to deserialize
     */
    void FromJson(const nlohmann::json &j) {
      name_ = j.at("name").get<std::string>();
      type_ = j.at("type").get<type::TypeId>();
    }

   private:
    std::string name_;
    type::TypeId type_;
  };

  /**
   * An intermediate column produced by plan nodes
   */
  class DerivedColumn {
   public:
    /**
     * Instantiate a derived column
     * @param column an intermediate column
     * @param expr the expression used to derive the intermediate column
     */
    DerivedColumn(Column column, const parser::AbstractExpression *expr) : column_(std::move(column)), expr_(expr) {}

    /**
     * Default constructor used for deserialization
     */
    DerivedColumn() = default;

    /**
     * Copies the DerivedColumn
     * @returns copy
     */
    DerivedColumn *Copy() const { return new DerivedColumn(column_, expr_->Copy()); }

    ~DerivedColumn() { delete expr_; }

    /**
     * @return the intermediate column definition
     */
    const Column &GetColumn() const { return column_; }

    /**
     * @return the expression used to derive the intermediate column
     */
    const parser::AbstractExpression *GetExpression() const { return expr_; }

    /**
     * Hash the current DerivedColumn.
     */
    common::hash_t Hash() const {
      common::hash_t hash = column_.Hash();
      hash = common::HashUtil::CombineHashes(hash, expr_->Hash());
      return hash;
    }

    /**
     * Equality check.
     * @param rhs other
     * @return true if the two derived columns are the same
     */
    bool operator==(const DerivedColumn &rhs) const {
      // Derived Column
      if (column_ != rhs.column_) return false;

      // Expression
      if ((expr_ == nullptr && rhs.expr_ != nullptr) || (expr_ != nullptr && rhs.expr_ == nullptr)) {
        return false;
      }
      if (expr_ != nullptr && *expr_ != *rhs.expr_) {
        return false;
      }

      return true;
    }

    /**
     * Inequality check
     * @param rhs other
     * @return true if the two DerivedColumn are not equal
     */
    bool operator!=(const DerivedColumn &rhs) const { return !operator==(rhs); }

    /**
     * @return derived column serialized to json
     */
    nlohmann::json ToJson() const {
      nlohmann::json j;
      j["column"] = column_;
      j["expr"] = expr_;
      return j;
    }

    /**
     * @param j json to deserialize
     */
    void FromJson(const nlohmann::json &j) {
      column_ = j.at("column").get<Column>();
      if (!j.at("expr").is_null()) {
        expr_ = parser::DeserializeExpression(j.at("expr"));
      }
    }

   private:
    /**
     * Intermediate column
     */
    Column column_;
    /**
     * The expression used to derive the intermediate column
     */
    const parser::AbstractExpression *expr_;
  };

  /**
   * Define a mapping of an offset into a vector of columns of an OutputSchema to an intermediate column produced by a
   * plan node
   */
  using DerivedTarget = std::pair<uint32_t, DerivedColumn *>;

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
                   "Number of columns must be between 1 and MAX_COL.");
  }

  /**
   * Default constructor for deserialization
   */
  OutputSchema() = default;

  /**
   * Copies the OutputSchema
   * @returns copy
   */
  std::shared_ptr<OutputSchema> Copy() const {
    std::vector<DerivedTarget> targets;
    for (auto &target : targets_) {
      targets.emplace_back(target.first, target.second->Copy());
    }

    return std::make_shared<OutputSchema>(columns_, std::move(targets), direct_map_list_);
  }

  ~OutputSchema() {
    for (const auto &pair : targets_) {
      delete pair.second;
    }
  }

  /**
   * @param col_id offset into the schema specifying which Column to access
   * @return description of the schema for a specific column
   */
  Column GetColumn(size_t col_id) const {
    TERRIER_ASSERT(col_id < columns_.size(), "column id is out of bounds for this Schema");
    return columns_[col_id];
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
      hash = common::HashUtil::CombineHashes(hash, target.second->Hash());
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
    // Columns
    if (columns_ != rhs.columns_) return false;

    // Targets
    std::function<bool(const DerivedTarget &, const DerivedTarget &)> cmp = [](const DerivedTarget &left,
                                                                               const DerivedTarget &right) {
      if (left.first != right.first) return false;
      if (left.second == nullptr && right.second == nullptr) return true;
      if (left.second == nullptr || right.second == nullptr) return false;
      return (*left.second == *right.second);
    };
    if (!std::equal(targets_.begin(), targets_.end(), rhs.targets_.begin(), cmp)) return false;

    // Direct Map List
    if (direct_map_list_ != rhs.direct_map_list_) return false;

    return true;
  }

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
    j["targets"] = targets_;
    j["direct_map_list"] = direct_map_list_;
    return j;
  }

  /**
   * @param j json to deserialize
   */
  void FromJson(const nlohmann::json &j) {
    columns_ = j.at("columns").get<std::vector<Column>>();
    // targets_ = j.at("targets").get<std::vector<DerivedTarget>>();
    // Deserialize children
    auto targets_json = j.at("targets").get<std::vector<std::pair<nlohmann::json, nlohmann::json>>>();
    for (const auto &pair_json : targets_json) {
      auto *derived_col = new DerivedColumn();
      derived_col->FromJson(pair_json.second);
      targets_.emplace_back(pair_json.first.get<uint32_t>(), derived_col);
    }
    direct_map_list_ = j.at("direct_map_list").get<std::vector<DirectMap>>();
  }

  /**
   * @returns DerivedTargets
   */
  const std::vector<DerivedTarget> &GetDerivedTargets() const { return targets_; }

  /**
   * @returns DirectMapList
   */
  const std::vector<DirectMap> &GetDirectMapList() const { return direct_map_list_; }

 private:
  std::vector<Column> columns_;
  std::vector<DerivedTarget> targets_;
  std::vector<DirectMap> direct_map_list_;
};

DEFINE_JSON_DECLARATIONS(OutputSchema::Column);
DEFINE_JSON_DECLARATIONS(OutputSchema::DerivedColumn);
DEFINE_JSON_DECLARATIONS(OutputSchema);

}  // namespace terrier::planner
