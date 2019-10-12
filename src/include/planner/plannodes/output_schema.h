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
     * @param nullable is column nullable
     * @param oid internal unique identifier for this column
     */
    Column(std::string name, const type::TypeId type, const bool nullable, const catalog::col_oid_t oid)
        : name_(std::move(name)), type_(type), nullable_(nullable), oid_(oid) {
      TERRIER_ASSERT(type_ != type::TypeId::INVALID, "Attribute type cannot be INVALID.");
    }

    /**
     * Default constructor used for deserialization
     */
    Column() = default;

    /**
     * Creates a copy of this column.
     */
    Column Copy() const { return Column(GetName(), GetType(), GetNullable(), GetOid()); }

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
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(type_));
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(oid_));
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(nullable_));
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

      // Nullable
      if (nullable_ != rhs.nullable_) return false;

      // Oid
      if (oid_ != rhs.oid_) return false;

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
      j["nullable"] = nullable_;
      j["oid"] = oid_;
      return j;
    }

    /**
     * @param j json to deserialize
     */
    void FromJson(const nlohmann::json &j) {
      name_ = j.at("name").get<std::string>();
      type_ = j.at("type").get<type::TypeId>();
      nullable_ = j.at("nullable").get<bool>();
      oid_ = j.at("oid").get<catalog::col_oid_t>();
    }

   private:
    std::string name_;
    type::TypeId type_;
    bool nullable_;
    catalog::col_oid_t oid_;
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
    DerivedColumn(Column column, common::ManagedPointer<parser::AbstractExpression> expr)
        : column_(std::move(column)), expr_(expr) {}

    /**
     * Default constructor used for deserialization
     */
    DerivedColumn() = default;

    /**
     * Creates a copy of this derived column.
     */
    DerivedColumn Copy() const { return DerivedColumn(GetColumn().Copy(), GetExpression()); }

    /**
     * @return the intermediate column definition
     */
    const Column &GetColumn() const { return column_; }

    /**
     * @return the expression used to derive the intermediate column
     */
    common::ManagedPointer<parser::AbstractExpression> GetExpression() const { return common::ManagedPointer(expr_); }

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
     * @return derived column serialized to json
     */
    nlohmann::json ToJson() const {
      nlohmann::json j;
      j["column"] = column_;
      j["expr"] = expr_->ToJson();
      return j;
    }

    /**
     * @param j json to deserialize
     */
    std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) {
      std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
      column_ = j.at("column").get<Column>();
      if (!j.at("expr").is_null()) {
        auto deserialized = parser::DeserializeExpression(j.at("expr"));
        expr_ = common::ManagedPointer(deserialized.result_);
        exprs.emplace_back(std::move(deserialized.result_));
        exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                     std::make_move_iterator(deserialized.non_owned_exprs_.end()));
      }
      return exprs;
    }

   private:
    /**
     * Intermediate column
     */
    Column column_;
    /**
     * The expression used to derive the intermediate column
     */
    common::ManagedPointer<parser::AbstractExpression> expr_;
  };

  /**
   * DerivedTarget defines a mapping of an offset into a vector of columns of an OutputSchema, to an intermediate
   * column produced by a plan node.
   */
  class DerivedTarget {
   public:
    /**
     * Creates a new DerivedTarget.
     * @param offset the offset into the OutputSchema columns
     * @param col the intermediate column produced by a plan node
     */
    DerivedTarget(uint32_t offset, DerivedColumn col) : offset_(offset), col_(std::move(col)) {}

    /**
     * Default constructor used for deserialization.
     */
    DerivedTarget() = default;

    /**
     * @return a copy of this DerivedTarget
     */
    DerivedTarget Copy() const { return DerivedTarget(GetOffset(), GetColumn().Copy()); }

    /**
     * @return offset into the OutputSchema columns
     */
    uint32_t GetOffset() const { return offset_; }

    /**
     * @return intermediate column produced by a plan node
     */
    const DerivedColumn &GetColumn() const { return col_; }

    /**
     * Equality check.
     * @param rhs other
     * @return true if the two derived targets are the same
     */
    bool operator==(const DerivedTarget &rhs) const {
      if (offset_ != rhs.offset_) return false;
      if (!(col_ == rhs.col_)) return false;
      return true;
    }

    /**
     * @return derived target serialized to json
     */
    nlohmann::json ToJson() const {
      nlohmann::json j;
      j["offset"] = offset_;
      j["col"] = col_.ToJson();
      return j;
    }

    /**
     * @param j json to deserialize
     */
    std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) {
      std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
      offset_ = j.at("offset").get<uint32_t>();
      auto col_exprs = col_.FromJson(j.at("col"));
      exprs.insert(exprs.end(), std::make_move_iterator(col_exprs.begin()), std::make_move_iterator(col_exprs.end()));
      return exprs;
    }

   private:
    uint32_t offset_;
    DerivedColumn col_;
  };

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
      // TODO(WAN): didn't we ban default arguments?
      : columns_(std::move(columns)), targets_(std::move(targets)), direct_map_list_(std::move(direct_map_list)) {
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
   * Make a copy of this OutputSchema
   * @return unique pointer to the copy
   */
  std::unique_ptr<OutputSchema> Copy() const {
    std::vector<Column> columns;
    for (const auto &col : GetColumns()) {
      columns.emplace_back(col.Copy());
    }

    // TODO(WAN): there are no getters for these members? why?
    std::vector<DerivedTarget> targets;
    for (const auto &target : targets_) {
      targets.emplace_back(target.Copy());
    }

    std::vector<DirectMap> direct_map_list;
    for (const auto &map : direct_map_list_) {
      direct_map_list.emplace_back(map.first, map.second);
    }
    return std::make_unique<OutputSchema>(std::move(columns), std::move(targets), std::move(direct_map_list));
  }

  /**
   * Hash the current OutputSchema.
   */
  common::hash_t Hash() const {
    common::hash_t hash = common::HashUtil::Hash(columns_.size());
    for (auto const &column : columns_) {
      hash = common::HashUtil::CombineHashes(hash, column.Hash());
    }
    for (auto const &target : targets_) {
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(target.GetOffset()));
      hash = common::HashUtil::CombineHashes(hash, target.GetColumn().Hash());
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
    if (targets_ != rhs.targets_) return false;

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
    std::vector<nlohmann::json> columns;
    for (const auto &col : columns_) {
      columns.emplace_back(col.ToJson());
    }
    j["columns"] = columns;
    std::vector<nlohmann::json> targets;
    for (const auto &target : targets_) {
      targets.emplace_back(target.ToJson());
    }
    j["targets"] = targets;
    j["direct_map_list"] = direct_map_list_;
    return j;
  }

  /**
   * @param j json to deserialize
   */
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) {
    std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;

    std::vector<nlohmann::json> columns_json = j.at("columns");
    for (const auto &j : columns_json) {
      Column c;
      c.FromJson(j);
      columns_.emplace_back(std::move(c));
    }

    std::vector<nlohmann::json> targets_json = j.at("targets");
    for (const auto &j : targets_json) {
      DerivedTarget target;
      auto target_exprs = target.FromJson(j);
      targets_.emplace_back(std::move(target));
      exprs.insert(exprs.end(), std::make_move_iterator(target_exprs.begin()),
                   std::make_move_iterator(target_exprs.end()));
    }

    direct_map_list_ = j.at("direct_map_list").get<std::vector<DirectMap>>();
    return exprs;
  }

 private:
  std::vector<Column> columns_;
  std::vector<DerivedTarget> targets_;
  std::vector<DirectMap> direct_map_list_;
};

DEFINE_JSON_DECLARATIONS(OutputSchema::Column);
DEFINE_JSON_DECLARATIONS(OutputSchema::DerivedColumn);
DEFINE_JSON_DECLARATIONS(OutputSchema::DerivedTarget);
DEFINE_JSON_DECLARATIONS(OutputSchema);

}  // namespace terrier::planner
