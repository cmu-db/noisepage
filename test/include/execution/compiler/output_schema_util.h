#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/compiler/expression_maker.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::execution::compiler::test {
using OutputColumn = planner::OutputSchema::Column;

/**
 * This class does 2 things:
 * 1. Facilitate the creation of output schema.
 * 2. Facilitate accessing the output of a child operator.
 */
class OutputSchemaHelper {
 public:
  /**
   * Construction
   * @param child_idx idx of the corresponding operator
   * @param expr_maker expression maker
   */
  OutputSchemaHelper(uint32_t child_idx, ExpressionMaker *expr_maker)
      : child_idx_{child_idx}, expr_maker_{expr_maker} {}

  /**
   * @param attr_idx index within the output tuples.
   * @return The attribute at the given index.
   */
  ExpressionMaker::ManagedExpression GetOutput(uint32_t attr_idx) {
    return expr_maker_->DVE(cols_.at(attr_idx).GetType(), child_idx_, attr_idx);
  }

  /**
   * @param col_name name of the column within the output tuples.
   * @return The attribute with the given name.
   */
  ExpressionMaker::ManagedExpression GetOutput(const std::string &col_name) {
    return GetOutput(name_to_idx_.at(col_name));
  }

  /**
   * @param col_name name of the output column
   * @param expr expression of the output column
   */
  void AddOutput(const std::string &col_name, ExpressionMaker::ManagedExpression expr) {
    auto idx = uint32_t(cols_.size());
    name_to_idx_.emplace(col_name, idx);
    cols_.emplace_back(col_name, expr->GetReturnValueType(), expr->Copy());
  }

  /**
   * @param agg_name Name of the aggregate
   * @return The derived value expression that accesses the given aggregate term.
   */
  ExpressionMaker::ManagedExpression GetAggTermForOutput(const std::string &agg_name) {
    const auto &agg = aggs_.at(name_to_agg_.at(agg_name));
    auto ret_type = agg->GetChild(0)->GetReturnValueType();
    auto tve_idx = name_to_agg_.at(agg_name);
    return expr_maker_->DVE(ret_type, 1, tve_idx);
  }

  /**
   * @param gby_name Name of the group by column
   * @return The derived value expression that accesses the given group by term
   */
  ExpressionMaker::ManagedExpression GetGroupByTermForOutput(const std::string &gby_name) {
    const auto &gby = gbys_.at(name_to_gby_.at(gby_name));
    auto ret_type = gby->GetReturnValueType();
    auto tve_idx = name_to_gby_.at(gby_name);
    return expr_maker_->DVE(ret_type, 0, tve_idx);
  }

  /**
   * @param col_name Name of the group by term
   * @param expr The group by expression
   */
  void AddGroupByTerm(const std::string &col_name, ExpressionMaker::ManagedExpression expr) {
    auto idx = uint32_t(gbys_.size());
    name_to_gby_.emplace(col_name, idx);
    gbys_.emplace_back(expr);
  }

  /**
   * @param col_name Name of the column
   * @return The group by expression
   */
  ExpressionMaker::ManagedExpression GetGroupByTerm(const std::string &col_name) {
    return gbys_.at(name_to_gby_.at(col_name));
  }

  /**
   * @param agg_name Name of the aggregate term
   * @param expr The aggregate expression
   */
  void AddAggTerm(const std::string &agg_name, ExpressionMaker::ManagedAggExpression expr) {
    auto idx = uint32_t(aggs_.size());
    name_to_agg_.emplace(agg_name, idx);
    aggs_.emplace_back(expr);
  }

  /**
   * @param agg_name Name of the aggregate term
   * @return The aggregate term
   */
  ExpressionMaker::ManagedAggExpression GetAggTerm(const std::string &agg_name) {
    return aggs_.at(name_to_agg_.at(agg_name));
  }

  /**
   * @return The constructed output schema
   */
  std::unique_ptr<planner::OutputSchema> MakeSchema() {
    std::vector<OutputColumn> cols;
    for (const auto &col : cols_) {
      cols.emplace_back(col.Copy());
    }
    return std::make_unique<planner::OutputSchema>(std::move(cols));
  }

 private:
  std::unordered_map<std::string, uint32_t> name_to_idx_;
  std::vector<OutputColumn> cols_;
  uint32_t child_idx_;
  std::unordered_map<std::string, uint32_t> name_to_gby_;
  std::vector<ExpressionMaker::ManagedExpression> gbys_;
  std::unordered_map<std::string, uint32_t> name_to_agg_;
  std::vector<ExpressionMaker::ManagedAggExpression> aggs_;
  ExpressionMaker *expr_maker_;
};
}  // namespace noisepage::execution::compiler::test
