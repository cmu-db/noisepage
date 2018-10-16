//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_join_plan.h
//
// Identification: src/include/planner/abstract_join_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <vector>
#include <map>
#include <vector>
#include <catalog/schema.h>

#include "abstract_plannode.h"
#include "expression/abstract_expression.h"
#include "attribute_info.h"
#include "planner/project_info.h"
#include "plannode_defs.h"

namespace terrier::sql::plannode {

//===--------------------------------------------------------------------===//
// Abstract Join Plan Node
//===--------------------------------------------------------------------===//

class AbstractJoinPlanNode : public AbstractPlanNode {
 public:
  AbstractJoinPlanNode(
      JoinType joinType,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      std::unique_ptr<const ProjectInfo> &&proj_info,
      std::shared_ptr<const catalog::Schema> &proj_schema)
      : AbstractPlanNode(),
        join_type_(joinType),
        predicate_(std::move(predicate)),
        proj_info_(std::move(proj_info)),
        proj_schema_(proj_schema) {
    // Fuck off!
  }

  // TODO: Is this a col_oid_t or offset
  void GetOutputColumns(std::vector<oid_t> &columns) const override;

  hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  JoinType GetJoinType() const { return join_type_; }

  const expression::AbstractExpression *GetPredicate() const {
    return predicate_.get();
  }

  const std::vector<const AttributeInfo *> &GetLeftAttributes() const {
    return left_attributes_;
  }

  const std::vector<const AttributeInfo *> &GetRightAttributes() const {
    return right_attributes_;
  }

  const ProjectInfo *GetProjInfo() const { return proj_info_.get(); }

  const catalog::Schema *GetSchema() const { return proj_schema_.get(); }

 protected:
  // For joins, attributes arrive from both the left and right side of the join
  // This method enables this merging by properly identifying each side's
  // attributes in separate contexts.
  virtual void HandleSubplanBinding(bool from_left,
                                    const BindingContext &input) = 0;

 private:
  /** @brief The type of join that we're going to perform */
  JoinType join_type_;

  /** @brief Join predicate. */
  const std::unique_ptr<const expression::AbstractExpression> predicate_;

  /** @brief Projection info */
  std::unique_ptr<const ProjectInfo> proj_info_;

  /** @brief Projection schema */
  std::shared_ptr<const catalog::Schema> proj_schema_;

  std::vector<const AttributeInfo *> left_attributes_;
  std::vector<const AttributeInfo *> right_attributes_;

 private:
  DISALLOW_COPY_AND_MOVE(AbstractJoinPlanNode);
};

}  // namespace terrier::sql::plannode
