#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "catalog/schema.h"
#include "common/hash_util.h"

namespace terrier::plan_node {

//===--------------------------------------------------------------------===//
// Plan Node Types
//===--------------------------------------------------------------------===//

enum class PlanNodeType {

  INVALID = 0,

  // Scan Nodes
  SEQSCAN,
  INDEXSCAN,
  CSVSCAN,

  // Join Nodes
  NESTLOOP,
  HASHJOIN,

  // Mutator Nodes
  UPDATE,
  INSERT,
  DELETE,

  // DDL Nodes
  DROP,
  CREATE,
  POPULATE_INDEX,
  ANALYZE,

  // Algebra Nodes
  AGGREGATE,
  ORDERBY,
  PROJECTION,
  LIMIT,
  DISTINCT,
  HASH,

  // Test
  MOCK
};

//===--------------------------------------------------------------------===//
// Abstract Plan Node
//===--------------------------------------------------------------------===//

class AbstractPlanNode {
 public:
  AbstractPlanNode(catalog::Schema output_schema);

  virtual ~AbstractPlanNode();

  //===--------------------------------------------------------------------===//
  // Children Helpers
  //===--------------------------------------------------------------------===//

  void AddChild(std::unique_ptr<AbstractPlanNode> &&child);

  const std::vector<std::unique_ptr<AbstractPlanNode>> &GetChildren() const;

  size_t GetChildrenSize() const { return children_.size(); }

  const AbstractPlanNode *GetChild(uint32_t child_index) const;

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  // Each sub-class will have to implement this function to return their type
  // This is better than having to store redundant types in all the objects
  virtual PlanNodeType GetPlanNodeType() const = 0;

  // Get the output schema for the plan node. The output schema contains information on columns of the output of
  // the plan node operator
  catalog::Schema GetOutputSchema() const { return output_schema_; }

  // Get the estimated cardinality of this plan
  int GetEstimatedCardinality() const { return estimated_cardinality_; }

  // FOR TESTING ONLY. This function should only be called during construction of plan (ConvertOpExpression) or
  // for tests.
  void SetEstimatedCardinality(int cardinality) { estimated_cardinality_ = cardinality; }

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//
  virtual std::unique_ptr<AbstractPlanNode> Copy() const = 0;

  virtual common::hash_t Hash() const;

  virtual bool operator==(const AbstractPlanNode &rhs) const;
  virtual bool operator!=(const AbstractPlanNode &rhs) const { return !(*this == rhs); }

 private:
  // A plan node can have multiple children
  std::vector<std::unique_ptr<AbstractPlanNode>> children_;

  int estimated_cardinality_ = 500000;

  catalog::Schema output_schema_;

 private:
  DISALLOW_COPY_AND_MOVE(AbstractPlanNode);
};

class Equal {
 public:
  bool operator()(const std::shared_ptr<plan_node::AbstractPlanNode> &a,
                  const std::shared_ptr<plan_node::AbstractPlanNode> &b) const {
    return *a.get() == *b.get();
  }
};

class Hash {
 public:
  size_t operator()(const std::shared_ptr<plan_node::AbstractPlanNode> &plan) const {
    return static_cast<size_t>(plan->Hash());
  }
};

}  // namespace terrier::plan_node
