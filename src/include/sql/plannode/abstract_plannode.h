//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_plan.h
//
// Identification: src/include/planner/abstract_plan.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "sql/plannode/plannode_defs.h"
#include "util/hash_util.h"

namespace terrier::sql::plannode {

/**
 * Base class for all SQL plan nodes
 */
class AbstractPlan {

 public:
  AbstractPlan();

  virtual ~AbstractPlan();

  //===--------------------------------------------------------------------===//
  // Children + Parent Helpers
  //===--------------------------------------------------------------------===//

  void AddChild(std::unique_ptr<AbstractPlan> &&child);

  const std::vector<std::unique_ptr<AbstractPlan>> &GetChildren() const;

  size_t GetChildrenSize() const { return children_.size(); }

  const AbstractPlan *GetChild(uint32_t child_index) const;

  const AbstractPlan *GetParent() const;

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  /**
   * Each sub-class will have to implement this function to return their type.
   * This is better than having to store redundant types in all the objects.
   * @return
   */
  virtual PlanNodeType GetPlanNodeType() const = 0;

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  virtual void GetOutputColumns(std::vector<oid_t> &columns UNUSED_ATTRIBUTE) const {}

  /**
   * Get a string representation for debugging
   * @return
   */
  const std::string GetInfo() const;

  virtual std::unique_ptr<AbstractPlan> Copy() const = 0;

  virtual hash_t Hash() const;

  virtual bool operator==(const AbstractPlan &rhs) const;
  virtual bool operator!=(const AbstractPlan &rhs) const { return !(*this == rhs); }

 protected:
  // only used by its derived classes (when deserialization)
  AbstractPlan *Parent() const { return parent_; }

 private:
  // A plan node can have multiple children
  std::vector<std::unique_ptr<AbstractPlan>> children_;

  AbstractPlan *parent_ = nullptr;

 private:
  DISALLOW_COPY_AND_MOVE(AbstractPlan);
};

class Equal {
 public:
  bool operator()(const std::shared_ptr<planner::AbstractPlan> &a,
                  const std::shared_ptr<planner::AbstractPlan> &b) const {
    return *a.get() == *b.get();
  }
};

class Hash {
 public:
  size_t operator()(const std::shared_ptr<planner::AbstractPlan> &plan) const {
    return static_cast<size_t>(plan->Hash());
  }
};

}  // namespace terrier::sql::plannode
