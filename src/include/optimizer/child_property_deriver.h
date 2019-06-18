#pragma once
#include <memory>
#include "optimizer/operator_visitor.h"
#include "optimizer/property_set.h"

namespace terrier {

namespace catalog {
class CatalogAccessor;
};

namespace optimizer {
class GroupExpression;
class Memo;
}

namespace optimizer {

// TODO(boweic): Currently we only represent sort as property, later we may want
// to add group, data compression and data distribution(if we go distributed) as
// property
class ChildPropertyDeriver : public OperatorVisitor {
 public:
  /**
   * Generate child property requirements for physical operators, return pairs of
   * possible input output properties pairs. All PropertySet pointers returned
   * by this function need to be freed by the caller.
   *
   * @param gexpr GroupExpression to generate property requirements for
   * @param requirements Required properties
   * @param memo Memo
   * @param accessor CatalogAccessor for use
   * @returns pair of possible input output properties pairs
   */
  std::vector<std::pair<PropertySet*, std::vector<PropertySet*>>>
  GetProperties(GroupExpression *gexpr, PropertySet* requirements,
                Memo *memo, catalog::CatalogAccessor *accessor);

  /**
   * Visitor function for TableFreeScan
   * @param op TableFreeScan operator to visit
   */
  void Visit(const TableFreeScan *op) override;

  /**
   * Visitor function for SeqScan
   * @param op SeqScan operator to visit
   */
  void Visit(const SeqScan *op) override;

  /**
   * Visitor function for IndexScan
   * @param op IndexScan operator to visit
   */
  void Visit(const IndexScan *op) override;

  /**
   * Visitor function for ExternalFileScan
   * @param op ExternalFileScan operator to visit
   */
  void Visit(const ExternalFileScan *op) override;

  /**
   * Visitor function for QueryDerivedScan
   * @param op QueryDerivedScan operator to visit
   */
  void Visit(const QueryDerivedScan *op) override;

  /**
   * Visitor function for OrderBy
   * @param op OrderBy operator to visit
   */
  void Visit(const OrderBy *op) override;

  /**
   * Visitor function for Limit
   * @param op Limit operator to visit
   */
  void Visit(const Limit *op) override;

  /**
   * Visitor function for InnerNLJoin
   * @param op InnerNLJoin operator to visit
   */
  void Visit(const InnerNLJoin *op) override;

  /**
   * Visitor function for LeftNLJoin
   * @param op LeftNLJoin operator to visit
   */
  void Visit(const LeftNLJoin *op) override;

  /**
   * Visitor function for RightNLJoin
   * @param op RightNLJoin operator to visit
   */
  void Visit(const RightNLJoin *op) override;

  /**
   * Visitor function for OuterNLJoin
   * @param op OuterNLJoin operator to visit
   */
  void Visit(const OuterNLJoin *op) override;

  /**
   * Visitor function for InnerHashJoin
   * @param op InnerHashJoin operator to visit
   */
  void Visit(const InnerHashJoin *op) override;

  /**
   * Visitor function for LeftHashJoin
   * @param op LeftHashJoin operator to visit
   */
  void Visit(const LeftHashJoin *op) override;

  /**
   * Visitor function for RightHashJoin
   * @param op RightHashJoin operator to visit
   */
  void Visit(const RightHashJoin *op) override;

  /**
   * Visitor function for OuterHashJoin
   * @param op OuterHashJoin operator to visit
   */
  void Visit(const OuterHashJoin *op) override;

  /**
   * Visitor function for Insert
   * @param op Insert operator to visit
   */
  void Visit(const Insert *op) override;

  /**
   * Visitor function for InsertSelect
   * @param op InsertSelect operator to visit
   */
  void Visit(const InsertSelect *op) override;

  /**
   * Visitor function for Delete
   * @param op Delete operator to visit
   */
  void Visit(const Delete *op) override;

  /**
   * Visitor function for Update
   * @param op Update operator to visit
   */
  void Visit(const Update *op) override;

  /**
   * Visitor function for HashGroupBy
   * @param op HashGroupBy operator to visit
   */
  void Visit(const HashGroupBy *op) override;

  /**
   * Visitor function for SortGroupBy
   * @param op SortGroupBy operator to visit
   */
  void Visit(const SortGroupBy *op) override;

  /**
   * Visitor function for Distinct
   * @param op Distinct operator to visit
   */
  void Visit(const Distinct *op) override;

  /**
   * Visitor function for Aggregate
   * @param op Aggregate operator to visit
   */
  void Visit(const Aggregate *op) override;

  /**
   * Visitor function for ExportExternalFile
   * @param op ExportExternalFile operator to visit
   */
  void Visit(const ExportExternalFile *op) override;

 private:
  /**
   * Derives properties for a JOIN
   */
  void DeriveForJoin();
  PropertySet* requirements_;

  /**
   * The derived output property set and input property sets, note that a
   * operator may have more than one children
   */
  std::vector<std::pair<PropertySet*, std::vector<PropertySet*>>> output_;

  /**
   * We need the memo and gexpr because some property may depend on child's schema
   */
  Memo *memo_;
  GroupExpression *gexpr_;

  /**
   * Accessor to the catalog
   */
  catalog::CatalogAccessor *accessor_;
};

}  // namespace optimizer
}  // namespace terrier
