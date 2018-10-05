//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_plan.h
//
// Identification: src/include/planner/hash_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "abstract_plannode.h"
#include "common/typedefs.h"

namespace terrier::sql::plannode {

class HashPlanNode : public AbstractPlanNode {
 public:
  typedef const expression::AbstractExpression HashKeyType;
  typedef std::unique_ptr<HashKeyType> HashKeyPtrType;

  HashPlanNode(std::vector<HashKeyPtrType> &hashkeys)
      : hash_keys_(std::move(hashkeys)) {}
  
  void GetOutputColumns(std::vector<col_oid_t> &columns) const override;

  inline PlanNodeType GetPlanNodeType() const override { return PlanNodeType::HASH; }

  const std::string GetInfo() const override { return "HashPlan"; }

  inline const std::vector<HashKeyPtrType> &GetHashKeys() const {
    return this->hash_keys_;
  }

  std::unique_ptr<AbstractPlanNode> Copy() const override {
    std::vector<HashKeyPtrType> copied_hash_keys;
    for (const auto &key : hash_keys_) {
      copied_hash_keys.push_back(std::unique_ptr<HashKeyType>(key->Copy()));
    }
    return std::unique_ptr<AbstractPlanNode>(new HashPlanNode(copied_hash_keys));
  }

  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override {
    return !(*this == rhs);
  }

  virtual void VisitParameters(codegen::QueryParametersMap &map,
                               std::vector<type::Value> &values,
                               const std::vector<type::Value> &values_from_user) override;

 private:
  std::vector<HashKeyPtrType> hash_keys_;

 private:
  DISALLOW_COPY_AND_MOVE(HashPlanNode);
};

}  // namespace planner
}  // namespace peloton
