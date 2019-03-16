#pragma once

#include <memory>
#include <string>
#include <utility>
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "plan_node/abstract_plan_node.h"
#include "plan_node/abstract_scan_plan_node.h"

namespace terrier::plan_node {

class SeqScanPlanNode : public AbstractScanPlanNode {
 public:
  SeqScanPlanNode(std::shared_ptr<OutputSchema> output_schema, parser::AbstractExpression *predicate,
                  catalog::table_oid_t table_oid, bool is_for_update = false, bool parallel = false)
      : AbstractScanPlanNode(std::move(output_schema), predicate, is_for_update, parallel), table_oid_(table_oid) {}

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::SEQSCAN; }

  /**
   * @return debug info
   */
  const std::string GetInfo() const override { return "SeqScanPlanNode"; }

  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  std::unique_ptr<AbstractPlanNode> Copy() const override {
    auto *new_plan = new SeqScanPlanNode(GetOutputSchema(), GetPredicate()->Copy().get(), GetTableOid(), IsForUpdate(),
                                         IsParallel());
    return std::unique_ptr<AbstractPlanNode>(new_plan);
  }

  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  // OID for table being scanned
  catalog::table_oid_t table_oid_;

  DISALLOW_COPY_AND_MOVE(SeqScanPlanNode);
};

}  // namespace terrier::plan_node
