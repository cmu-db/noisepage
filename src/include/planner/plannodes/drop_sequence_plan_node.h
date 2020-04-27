#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "parser/drop_statement.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace terrier::planner {
/**
 *  The plan node for dropping sequences
 */
class DropSequencePlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a drop sequence plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param sequence_oid the OID of the sequence to drop
     * @return builder object
     */
    Builder &SetSequenceOid(catalog::sequence_oid_t sequence_oid) {
      sequence_oid_ = sequence_oid;
      return *this;
    }

    /**
     * Build the drop sequence plan node
     * @return plan node
     */
    std::unique_ptr<DropSequencePlanNode> Build() {
      return std::unique_ptr<DropSequencePlanNode>(
          new DropSequencePlanNode(std::move(children_), std::move(output_schema_), sequence_oid_));
    }

   protected:
    /**
     * OID of the sequence to drop
     */
    catalog::sequence_oid_t sequence_oid_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param sequence_oid OID of the sequence to drop
   */
  DropSequencePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                       std::unique_ptr<OutputSchema> output_schema, catalog::sequence_oid_t sequence_oid)
      : AbstractPlanNode(std::move(children), std::move(output_schema)), sequence_oid_(sequence_oid) {}

 public:
  /**
   * Default constructor for deserialization
   */
  DropSequencePlanNode() = default;

  DISALLOW_COPY_AND_MOVE(DropSequencePlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DROP_SEQUENCE; }

  /**
   * @return OID of the sequence to drop
   */
  catalog::sequence_oid_t GetSequenceOid() const { return sequence_oid_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  /**
   * OID of the sequence to drop
   */
  catalog::sequence_oid_t sequence_oid_;
};

DEFINE_JSON_DECLARATIONS(DropSequencePlanNode);

}  // namespace terrier::planner
