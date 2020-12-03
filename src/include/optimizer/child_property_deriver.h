#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "optimizer/operator_visitor.h"
#include "optimizer/property_set.h"

namespace noisepage {

namespace catalog {
class CatalogAccessor;
};

namespace optimizer {

class GroupExpression;
class Memo;

// TODO(boweic): Currently we only represent sort as property, later we may want
// to add group, data compression and data distribution(if we go distributed) as
// property

/**
 * ChildPropertyDeriver derives output and child input property requirements
 * for a given GroupExpression considering the overall properties that need
 * to be satisifed.
 */
class ChildPropertyDeriver : public OperatorVisitor {
 public:
  /**
   * Generate child property requirements for physical operators, return pairs of
   * possible input output properties pairs. All PropertySet pointers returned
   * by this function need to be freed by the caller.
   *
   * @param accessor CatalogAccessor for use
   * @param memo Memo
   * @param gexpr GroupExpression to generate property requirements for
   * @param requirements Required properties
   * @returns pair of possible input output properties pairs
   */
  std::vector<std::pair<PropertySet *, std::vector<PropertySet *>>> GetProperties(catalog::CatalogAccessor *accessor,
                                                                                  Memo *memo, PropertySet *requirements,
                                                                                  GroupExpression *gexpr);

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
   * Visitor function for InnerIndexJoin
   * @param op InnerIndexJoin operator to visit
   */
  void Visit(const InnerIndexJoin *op) override;

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
   * Visitor function for LeftSemiHashJoin
   * @param op LeftSemiHashJoin operator to visit
   */
  void Visit(const LeftSemiHashJoin *op) override;
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
   * Visitor function for Aggregate
   * @param op Aggregate operator to visit
   */
  void Visit(const Aggregate *op) override;

  /**
   * Visitor function for ExportExternalFile
   * @param op ExportExternalFile operator to visit
   */
  void Visit(const ExportExternalFile *op) override;

  /**
   * Visit a CreateDatabase operator
   * @param create_database operator
   */
  void Visit(const CreateDatabase *create_database) override;

  /**
   * Visit a CreateFunction operator
   * @param create_function operator
   */
  void Visit(const CreateFunction *create_function) override;

  /**
   * Visit a CreateIndex operator
   * @param create_index operator
   */
  void Visit(const CreateIndex *create_index) override;

  /**
   * Visit a CreateTable operator
   * @param create_table operator
   */
  void Visit(const CreateTable *create_table) override;

  /**
   * Visit a CreateNamespace operator
   * @param create_namespace operator
   */
  void Visit(const CreateNamespace *create_namespace) override;

  /**
   * Visit a CreateTrigger operator
   * @param create_trigger operator
   */
  void Visit(const CreateTrigger *create_trigger) override;

  /**
   * Visit a CreateView operator
   * @param create_view operator
   */
  void Visit(const CreateView *create_view) override;
  /**
   * Visit a DropDatabase operator
   * @param drop_database operator
   */
  void Visit(const DropDatabase *drop_database) override;

  /**
   * Visit a DropTable operator
   * @param drop_table operator
   */
  void Visit(const DropTable *drop_table) override;

  /**
   * Visit a DropIndex operator
   * @param drop_index operator
   */
  void Visit(const DropIndex *drop_index) override;

  /**
   * Visit a DropNamespace operator
   * @param drop_namespace operator
   */
  void Visit(const DropNamespace *drop_namespace) override;

  /**
   * Visit a DropTrigger operator
   * @param drop_trigger operator
   */
  void Visit(const DropTrigger *drop_trigger) override;

  /**
   * Visit a DropView operator
   * @param drop_view operator
   */
  void Visit(const DropView *drop_view) override;

 private:
  /**
   * Derives properties for a JOIN
   */
  void DeriveForJoin();

  /**
   * Any property requirements
   */
  PropertySet *requirements_;

  /**
   * The derived output property set and input property sets, note that a
   * operator may have more than one children
   */
  std::vector<std::pair<PropertySet *, std::vector<PropertySet *>>> output_;

  /**
   * We need the memo and gexpr because some property may depend on child's schema
   */
  Memo *memo_;

  /**
   * Group Expression to derive
   */
  GroupExpression *gexpr_;

  /**
   * Accessor to the catalog
   */
  catalog::CatalogAccessor *accessor_;
};

}  // namespace optimizer
}  // namespace noisepage
