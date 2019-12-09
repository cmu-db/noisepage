#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "common/hash_util.h"
#include "parser/expression/abstract_expression.h"
#include "planner/plannodes/abstract_scan_plan_node.h"
#include "type/transient_value.h"

// TODO(Gus,Wen): plan node contained info on whether the scan was left or right open. This should be computed at
// exection time

namespace terrier::planner {

/**
 * Index Scan Predicate Description
 */
class IndexScanDescription {
 public:
  /**
   * Default constructor for JSON
   */
  IndexScanDescription() = default;

  /**
   * Constructor
   * @param tuple_oids Tuple Column OIDs with predicates
   * @param expr_list ExpressionType list of comparisons
   * @param val_list Value list of comparisons (bound + parameters)
   */
  IndexScanDescription(std::vector<catalog::col_oid_t> &&tuple_oids, std::vector<parser::ExpressionType> &&expr_list,
                       std::vector<type::TransientValue> &&val_list)
      : tuple_column_oid_list_(tuple_oids), expr_list_(expr_list), value_list_(std::move(val_list)) {}

  /**
   * Move constructor
   * @param other Other to move from
   */
  IndexScanDescription(IndexScanDescription &&other) noexcept
      : tuple_column_oid_list_(std::move(other.tuple_column_oid_list_)),
        expr_list_(std::move(other.expr_list_)),
        value_list_(std::move(other.value_list_)) {}

  /**
   * Move assignment
   * @param other Other to move from
   */
  IndexScanDescription &operator=(IndexScanDescription &&other) noexcept {
    tuple_column_oid_list_ = std::move(other.tuple_column_oid_list_);
    expr_list_ = std::move(other.expr_list_);
    value_list_ = std::move(other.value_list_);
    return *this;
  }

  /**
   * Checks equality against other IndexScanDescription
   * @param other IndexScanDescription to check against
   * @returns true if equal
   */
  bool operator==(const IndexScanDescription &other) const {
    if (tuple_column_oid_list_ != other.tuple_column_oid_list_) return false;
    if (expr_list_ != other.expr_list_) return false;
    return value_list_ == other.value_list_;
  }

  /**
   * Checks in-equality against other IndexScanDescription
   * @param other IndexScanDescription to check against
   * @returns true if not equal
   */
  bool operator!=(const IndexScanDescription &other) const { return !(*this == other); }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const {
    common::hash_t hash = common::HashUtil::Hash(tuple_column_oid_list_);
    hash = common::HashUtil::CombineHashInRange(hash, expr_list_.begin(), expr_list_.end());
    hash = common::HashUtil::CombineHashInRange(hash, value_list_.begin(), value_list_.end());
    return hash;
  }

  /**
   * Serializes this as json
   */
  nlohmann::json ToJson() const {
    nlohmann::json j;
    j["tuple_column_oid_list"] = tuple_column_oid_list_;
    j["expr_list"] = expr_list_;
    j["value_list"] = value_list_;
    return j;
  }

  /**
   * Deserializes from JSON
   * @param j json
   */
  void FromJson(const nlohmann::json &j) {
    tuple_column_oid_list_ = j.at("tuple_column_oid_list").get<std::vector<catalog::col_oid_t>>();
    expr_list_ = j.at("expr_list").get<std::vector<parser::ExpressionType>>();
    value_list_ = j.at("value_list").get<std::vector<type::TransientValue>>();
  }

  /**
   * @returns column oids in base table that have scan predicate
   */
  const std::vector<catalog::col_oid_t> &GetTupleColumnIdList() const { return tuple_column_oid_list_; }

  /**
   * @returns comparison expression type list
   */
  const std::vector<parser::ExpressionType> &GetExpressionTypeList() const { return expr_list_; }

  /**
   * @returns comparison values
   */
  const std::vector<type::TransientValue> &GetValueList() const { return value_list_; }

 private:
  /**
   * Subset of column oids referenced in the scan predicate that should be used
   * for probing into the relevant index.
   */
  std::vector<catalog::col_oid_t> tuple_column_oid_list_;

  /**
   * List of comparison expression types
   */
  std::vector<parser::ExpressionType> expr_list_;

  /**
   * List of values
   */
  std::vector<type::TransientValue> value_list_;
};

/**
 * Plan node for an index scan
 */
class IndexScanPlanNode : public AbstractScanPlanNode {
 public:
  /**
   * Builder for an index scan plan node
   */
  class Builder : public AbstractScanPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param oid oid for index to use for scan
     * @return builder object
     */
    Builder &SetIndexOid(catalog::index_oid_t oid) {
      index_oid_ = oid;
      return *this;
    }

    /**
     * @param column_oids OIDs of columns to scan
     * @return builder object
     */
    Builder &SetColumnOids(std::vector<catalog::col_oid_t> &&column_oids) {
      column_oids_ = column_oids;
      return *this;
    }

    /**
     * @param desc IndexScanDescription for index scan
     * @return builder object
     */
    Builder &SetIndexScanDescription(IndexScanDescription &&desc) {
      index_scan_desc_ = std::move(desc);
      return *this;
    }

    /**
     * Build the Index scan plan node
     * @return plan node
     */
    std::unique_ptr<IndexScanPlanNode> Build() {
      return std::unique_ptr<IndexScanPlanNode>(new IndexScanPlanNode(
          std::move(children_), std::move(output_schema_), scan_predicate_, std::move(column_oids_),
          std::move(index_scan_desc_), is_for_update_, database_oid_, namespace_oid_, index_oid_));
    }

   protected:
    /**
     * index OID to be used for scan
     */
    catalog::index_oid_t index_oid_;

    /**
     * OIDS of columns to scan
     */
    std::vector<catalog::col_oid_t> column_oids_;

    /**
     * IndexScanDescription
     */
    IndexScanDescription index_scan_desc_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param predicate predicate used for performing scan
   * @param column_oids OIDs of columns to scan
   * @param scan_desc IndexScanDescription for the index scan
   * @param is_for_update scan is used for an update
   * @param database_oid database oid for scan
   * @param index_oid OID of index to be used in index scan
   */
  IndexScanPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                    std::unique_ptr<OutputSchema> output_schema,
                    common::ManagedPointer<parser::AbstractExpression> predicate,
                    std::vector<catalog::col_oid_t> &&column_oids, IndexScanDescription &&scan_desc, bool is_for_update,
                    catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                    catalog::index_oid_t index_oid)
      : AbstractScanPlanNode(std::move(children), std::move(output_schema), predicate, is_for_update, database_oid,
                             namespace_oid),
        index_oid_(index_oid),
        column_oids_(column_oids),
        index_scan_desc_(std::move(scan_desc)) {}

 public:
  /**
   * Default constructor used for deserialization
   */
  IndexScanPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(IndexScanPlanNode)

  /**
   * @return index OID to be used for scan
   */
  catalog::index_oid_t GetIndexOid() const { return index_oid_; }

  /**
   * @return OIDs of columns to scan
   */
  const std::vector<catalog::col_oid_t> &GetColumnIds() const { return column_oids_; }

  /**
   * @returns Index Scan Description
   */
  const IndexScanDescription &GetIndexScanDescription() const { return index_scan_desc_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::INDEXSCAN; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;
  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  /**
   * Index oid associated with index scan
   */
  catalog::index_oid_t index_oid_;

  /**
   * OIDs of columns to scan
   */
  std::vector<catalog::col_oid_t> column_oids_;

  /**
   * IndexScanDescription
   */
  IndexScanDescription index_scan_desc_;
};

DEFINE_JSON_DECLARATIONS(IndexScanDescription)
DEFINE_JSON_DECLARATIONS(IndexScanPlanNode)

}  // namespace terrier::planner
