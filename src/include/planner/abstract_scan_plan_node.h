#pragma once
#include "catalog/catalog_defs.h"
#include "parser/expression/abstract_expression.h"
#include "planner/abstract_plan_node.h"
namespace terrier::planner {

class AbstractScanPlanNode : public AbstractPlanNode {
 public:
  const catalog::table_oid_t GetTableOid() const { return target_table_; }
  const std::vector<catalog::col_oid_t> &GetColumnIds() const { return column_ids_; }
  const std::unique_ptr<parser::AbstractExpression> &GetPredicate() const { return predicate_; }

  uint64_t Hash() const override {
    uint64_t result = AbstractPlanNode::Hash();
    result = common::HashUtil::CombineHashes(result, common::HashUtil::Hash(target_table_));
    result = common::HashUtil::CombineHashInRange(result, column_ids_.begin(), column_ids_.end());
    return result;
  }

  bool operator==(const AbstractPlanNode &rhs) const override {
    if (!AbstractPlanNode::operator==(rhs)) return false;
    auto &other = dynamic_cast<const AbstractScanPlanNode &>(rhs);
    return target_table_ == other.target_table_ && column_ids_ == other.column_ids_;
  }

 protected:
  AbstractScanPlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children,
                       std::shared_ptr<AbstractPlanNode> &&parent, catalog::table_oid_t table,
                       std::vector<catalog::col_oid_t> &&column_ids)
      : AbstractPlanNode(std::move(children), std::move(parent)),
        target_table_(table),
        column_ids_(std::move(column_ids)) {}

  template <class ConcreteType>
  class Builder : public AbstractPlanNode::Builder<ConcreteType> {
   public:
    ConcreteType &SetTableOid(catalog::table_oid_t oid) {
      target_table_ = oid;
      return *dynamic_cast<ConcreteType *>(this);
    }

    ConcreteType &AddColumnToOutput(catalog::col_oid_t column_id) {
      column_ids_.push_back(column_id);
      return *dynamic_cast<ConcreteType *>(this);
    }

    ConcreteType &SetPredicate(std::unique_ptr<parser::AbstractExpression> predicate) {
      predicate_ = std::move(predicate);
      return *dynamic_cast<ConcreteType *>(this);
    }

   protected:
    catalog::table_oid_t target_table_;
    std::vector<catalog::col_oid_t> column_ids_;
    std::unique_ptr<parser::AbstractExpression> predicate_;
  };

 private:
  const catalog::table_oid_t target_table_;
  const std::vector<catalog::col_oid_t> column_ids_;
  const std::unique_ptr<parser::AbstractExpression> predicate_;
};
}  // namespace terrier::planner