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

// TODO(Gus,Wen): IndexScanDesc had a `p_runtime_key_list` that did not have a comment explaining its use. We should
// figure that out. IndexScanDesc also had an expression type list, i dont see why this can't just be taken from the
// predicate

// TODO(Gus,Wen): plan node contained info on whether the scan was left or right open. This should be computed at
// exection time

namespace terrier::planner {

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
     * Build the Index scan plan node
     * @return plan node
     */
    std::shared_ptr<IndexScanPlanNode> Build() {
      return std::shared_ptr<IndexScanPlanNode>(
          new IndexScanPlanNode(std::move(children_), std::move(output_schema_), std::move(scan_predicate_),
                                is_for_update_, is_parallel_, database_oid_, namespace_oid_, index_oid_));
    }

   protected:
    /**
     * index OID to be used for scan
     */
    catalog::index_oid_t index_oid_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param predicate predicate used for performing scan
   * @param is_for_update scan is used for an update
   * @param is_parallel parallel scan flag
   * @param database_oid database oid for scan
   * @param index_oid OID of index to be used in index scan
   */
  IndexScanPlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children,
                    std::shared_ptr<OutputSchema> output_schema, std::shared_ptr<parser::AbstractExpression> predicate,
                    bool is_for_update, bool is_parallel, catalog::db_oid_t database_oid,
                    catalog::namespace_oid_t namespace_oid, catalog::index_oid_t index_oid)
      : AbstractScanPlanNode(std::move(children), std::move(output_schema), std::move(predicate), is_for_update,
                             is_parallel, database_oid, namespace_oid),
        index_oid_(index_oid) {}

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
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::INDEXSCAN; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;
  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  void FromJson(const nlohmann::json &j) override;

 private:
  /**
   * Index oid associated with index scan
   */
  catalog::index_oid_t index_oid_;
};

DEFINE_JSON_DECLARATIONS(IndexScanPlanNode)

}  // namespace terrier::planner
