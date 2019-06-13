#pragma once

#include "common/managed_pointer.h"
#include "optimizer/operator_visitor.h"

namespace terrier {

namespace optimizer {
class PropertySet;
class GroupExpression;
class OperatorExpression;
class Memo;
}

namespace optimizer {

/**
 * @brief Generate input and output columns based on the required columns,
 * required properties and the current group expression. We use the input/output
 * columns to eventually generate plans
 */
class InputColumnDeriver : public OperatorVisitor {
 public:
  InputColumnDeriver();

  /**
   * Derives the input and output columns for a physical operator
   * @param gexpr Group Expression to derive for
   * @param properties Relevant data properties
   * @param required_cols Vector of required output columns
   * @param memo Memo
   * @returns pair where first element is the output columns and the second
   *          element is a vector of inputs from each child.
   *
   * Pointers returned are not ManagedPointer. However, the returned pointers
   * should not be deleted or ever modified.
   */
  std::pair<std::vector<const parser::AbstractExpression*>,
            std::vector<std::vector<const parser::AbstractExpression*>>>
  DeriveInputColumns(GroupExpression *gexpr, PropertySet* properties,
                     std::vector<const parser::AbstractExpression*> required_cols,
                     Memo *memo);

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
  /**
   * @brief Provide all tuple value expressions needed in the expression
   */
  void ScanHelper();
  void AggregateHelper(const BaseOperatorNode *);
  void JoinHelper(const BaseOperatorNode *op);

  /**
   * @brief Some operators, for example limit, directly pass down column
   * property
   */
  void Passdown();
  GroupExpression *gexpr_;
  Memo *memo_;

  /**
   * @brief The derived output columns and input columns, note that the current
   *  operator may have more than one children
   */
  std::pair<std::vector<const parser::AbstractExpression*>,
            std::vector<std::vector<const parser::AbstractExpression*>>>
      output_input_cols_;

  /**
   * @brief The required columns
   */
  std::vector<const parser::AbstractExpression*> required_cols_;

  /**
   * @brief The required physical property
   */
  PropertySet* properties_;
};

}  // namespace optimizer
}  // namespace terrier
