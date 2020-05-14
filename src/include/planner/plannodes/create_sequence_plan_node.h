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
     * @param database_oid  OID of the database
     * @return builder object
     */
    Builder &SetDatabaseOid(catalog::db_oid_t database_oid) {
      database_oid_ = database_oid;
      return *this;
    }

    /**
     * @param namespace_oid OID of the namespace
     * @return builder object
     */
    Builder &SetNamespaceOid(catalog::namespace_oid_t namespace_oid) {
      namespace_oid_ = namespace_oid;
      return *this;
    }

    /**
     * @param sequence_name name of the sequence
     * @return builder object
     */
    Builder &SetSequenceName(std::string sequence_name) {
      sequence_name_ = std::move(sequence_name);
      return *this;
    }

    /**
     * @param sequence_start start value of the sequence
     * @return builder object
     */
    Builder &SetSequenceStart(int64_t sequence_start) {
      sequence_start_ = sequence_start;
      return *this;
    }

    /**
     * @param sequence_increment increment value of the sequence
     * @return builder object
     */
    Builder &SetSequenceIncrement(int64_t sequence_increment) {
      sequence_increment_ = sequence_increment;
      return *this;
    }

    /**
     * @param sequence_max maximum value of the sequence
     * @return builder object
     */
    Builder &SetSequenceMax(int64_t sequence_max) {
      sequence_max_ = sequence_max;
      return *this;
    }

    /**
     * @param sequence_min minimum value of the sequence
     * @return builder object
     */
    Builder &SetSequenceMin(int64_t sequence_min) {
      sequence_min_ = sequence_min;
      return *this;
    }

    /**
     * @param sequence_cycle whether the sequence cycles
     * @return builder object
     */
    Builder &SetSequenceCycle(bool sequence_cycle) {
      sequence_cycle_ = sequence_cycle;
      return *this;
    }

    /**
     * Build the create sequence plan node
     * @return plan node
     */
    std::unique_ptr<CreateSequencePlanNode> Build() {
      return std::unique_ptr<CreateSequencePlanNode>(new CreateSequencePlanNode(
          std::move(children_), std::move(output_schema_), database_oid_, namespace_oid_, std::move(sequence_name_),
          sequence_start_, sequence_increment_, sequence_max_, sequence_min_, sequence_cycle_));
    }

   protected:
    /**
     * OID of the database
     */
    catalog::db_oid_t database_oid_;

    /**
     * OID of namespace
     */
    catalog::namespace_oid_t namespace_oid_;

    /**
     * Name of the sequence
     */
    std::string sequence_name_;

    /**
     * Start value of the sequence
     */
    int64_t sequence_start_;

    /**
     * Increment value of the sequence
     */
    int64_t sequence_increment_;

    /**
     * Maximum value of the sequence
     */
    int64_t sequence_max_;

    /**
     * Minimum value of the sequence
     */
    int64_t sequence_min_;

    /**
     * Whether the sequence cycles
     */
    bool sequence_cycle_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param sequence_name name of the sequence
   */
  CreateSequencePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                         std::unique_ptr<OutputSchema> output_schema, catalog::db_oid_t database_oid,
                         catalog::namespace_oid_t namespace_oid, std::string sequence_name, int64_t sequence_start,
                         int64_t sequence_increment, int64_t sequence_max, int64_t sequence_min, bool sequence_cycle)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        database_oid_(database_oid),
        namespace_oid_(namespace_oid),
        sequence_name_(std::move(sequence_name)),
        sequence_start_(sequence_start),
        sequence_increment_(sequence_increment),
        sequence_max_(sequence_max),
        sequence_min_(sequence_min),
        sequence_cycle_(sequence_cycle) {}

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
   * @return OID of the database
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /**
   * @return OID of the namespace
   */
  catalog::namespace_oid_t GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return sequence name
   */
  std::string GetSequenceName() const { return sequence_name_; }

  /**
   * @return start value of the sequence
   */
  int64_t GetSequenceStart() const { return sequence_start_; }

  /**
   * @return increment value of the sequence
   */
  int64_t GetSequenceIncrement() const { return sequence_increment_; }

  /**
   * @return maximum value of the sequence
   */
  int64_t GetSequenceMax() const { return sequence_max_; }

  /**
   * @return minimum value of the sequence
   */
  int64_t GetSequenceMin() const { return sequence_min_; }

  /**
   * @return whether the sequence cycles
   */
  bool GetSequenceCycle() const { return sequence_cycle_; }

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
   * OID of the database
   */
  catalog::db_oid_t database_oid_;

  /**
   * OID of namespace
   */
  catalog::namespace_oid_t namespace_oid_;

  /**
   * Name of the sequence
   */
  std::string sequence_name_;

  /**
   * Start value of the sequence
   */
  int64_t sequence_start_;

  /**
   * Increment value of the sequence
   */
  int64_t sequence_increment_;

  /**
   * Maximum value of the sequence
   */
  int64_t sequence_max_;

  /**
   * Minimum value of the sequence
   */
  int64_t sequence_min_;

  /**
   * Whether the sequence cycles
   */
  bool sequence_cycle_;
};

DEFINE_JSON_DECLARATIONS(CreateSequencePlanNode);

}  // namespace terrier::planner
