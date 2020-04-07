#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "parser/create_statement.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace terrier::planner {

/**
 * Plan node for creating sequences
 */
class CreateSequencePlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a create sequence plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param sequence_name name of the sequence
     * @return builder object
     */
    Builder &SetSequenceName(std::string sequence_name) {
      sequence_name_ = std::move(sequence_name);
      return *this;
    }

    /**
     * Build the create sequence plan node
     * @return plan node
     */
    std::unique_ptr<CreateSequencePlanNode> Build() {
      return std::unique_ptr<CreateSequencePlanNode>(new CreateSequencePlanNode(
          std::move(children_), std::move(output_schema_),
          std::move(sequence_name_)));
    }

   protected:
    /**
     * Name of the sequence
     */
    std::string sequence_name_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param sequence_name name of the sequence
   */
  CreateSequencePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                        std::unique_ptr<OutputSchema> output_schema, std::string sequence_name)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        sequence_name_(std::move(sequence_name)) {}

 public:
  /**
   * Default constructor for deserialization
   */
  CreateSequencePlanNode() = default;

  DISALLOW_COPY_AND_MOVE(CreateSequencePlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::CREATE_SEQUENCE; }

  /**
   * @return sequence name
   */
  std::string GetSequenceName() const { return sequence_name_; }

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
   * Name of the sequence
   */
  std::string sequence_name_;
};

DEFINE_JSON_DECLARATIONS(CreateSequencePlanNode);

}  // namespace terrier::planner
