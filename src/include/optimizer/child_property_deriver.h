#pragma once
#include <memory>
#include "optimizer/operator_visitor.h"
#include "optimizer/property_set.h"

namespace terrier {

namespace optimizer {
class GroupExpression;
class Memo;
}

namespace optimizer {

// TODO(boweic): Currently we only represent sort as property, later we may want
// to add group, data compression and data distribution(if we go distributed) as
// property
/**
 * @brief Generate child property requirements for physical operators, return pairs of
 *  possible input output properties pairs.
 */
class ChildPropertyDeriver : public OperatorVisitor {
 public:
  std::vector<std::pair<PropertySet*, std::vector<PropertySet*>>>
  GetProperties(GroupExpression *gexpr, PropertySet* requirements, Memo *memo);

  void Visit(const TableFreeScan *op) override;
  void Visit(const SeqScan *op) override;
  void Visit(const IndexScan *op) override;
  void Visit(const ExternalFileScan *op) override;
  void Visit(const QueryDerivedScan *op) override;
  void Visit(const OrderBy *op) override;
  void Visit(const Limit *op) override;
  void Visit(const InnerNLJoin *op) override;
  void Visit(const LeftNLJoin *op) override;
  void Visit(const RightNLJoin *op) override;
  void Visit(const OuterNLJoin *op) override;
  void Visit(const InnerHashJoin *op) override;
  void Visit(const LeftHashJoin *op) override;
  void Visit(const RightHashJoin *op) override;
  void Visit(const OuterHashJoin *op) override;
  void Visit(const Insert *op) override;
  void Visit(const InsertSelect *op) override;
  void Visit(const Delete *op) override;
  void Visit(const Update *op) override;
  void Visit(const HashGroupBy *op) override;
  void Visit(const SortGroupBy *op) override;
  void Visit(const Distinct *op) override;
  void Visit(const Aggregate *op) override;
  void Visit(const ExportExternalFile *op) override;

 private:
  void DeriveForJoin();
  PropertySet* requirements_;

  /**
   * @brief The derived output property set and input property sets, note that a
   *  operator may have more than one children
   */
  std::vector<std::pair<PropertySet*, std::vector<PropertySet*>>> output_;
  /**
   * @brief We need the memo and gexpr because some property may depend on
   *  child's schema
   */
  Memo *memo_;
  GroupExpression *gexpr_;
};

}  // namespace optimizer
}  // namespace terrier
