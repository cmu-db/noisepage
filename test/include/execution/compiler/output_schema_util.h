#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "execution/compiler/expression_util.h"
#include "planner/plannodes/output_schema.h"

namespace terrier::execution::compiler {
using OutputColumn = planner::OutputSchema::Column;

class OutputSchemaHelper {
 public:
  OutputSchemaHelper(uint32_t child_idx, ExpressionMaker *expr_maker)
      : child_idx_{child_idx}, expr_maker_{expr_maker} {}

  ExpressionMaker::ManagedExpression GetOutput(uint32_t attr_idx) {
    return expr_maker_->DVE(cols_[attr_idx].GetType(), child_idx_, attr_idx);
  }

  ExpressionMaker::ManagedExpression GetOutput(const std::string &col_name) {
    return GetOutput(name_to_idx_[col_name]);
  }

  void AddOutput(const std::string &col_name, ExpressionMaker::ManagedExpression expr) {
    auto idx = uint32_t(cols_.size());
    name_to_idx_.emplace(col_name, idx);
    cols_.emplace_back(col_name, expr->GetReturnValueType(), expr->Copy());
  }

  void AddOutput(const std::string &col_name, ExpressionMaker::ManagedExpression expr, type::TypeId col_type) {
    auto idx = uint32_t(cols_.size());
    name_to_idx_.emplace(col_name, idx);
    cols_.emplace_back(col_name, col_type, expr->Copy());
  }

  ExpressionMaker::ManagedExpression GetAggTermForOutput(const std::string &agg_name) {
    const auto &agg = aggs_[name_to_agg_[agg_name]];
    auto ret_type = agg->GetChild(0)->GetReturnValueType();
    auto tve_idx = name_to_agg_[agg_name] + uint32_t(gbys_.size());
    return expr_maker_->DVE(ret_type, 0, tve_idx);
  }

  ExpressionMaker::ManagedExpression GetGroupByTermForOutput(const std::string &gby_name) {
    const auto &gby = gbys_[name_to_gby_[gby_name]];
    auto ret_type = gby->GetReturnValueType();
    auto tve_idx = name_to_gby_[gby_name];
    return expr_maker_->DVE(ret_type, 0, tve_idx);
  }

  void AddGroupByTerm(const std::string &col_name, ExpressionMaker::ManagedExpression expr) {
    auto idx = uint32_t(gbys_.size());
    name_to_gby_.emplace(col_name, idx);
    gbys_.emplace_back(expr);
  }

  ExpressionMaker::ManagedExpression GetGroupByTerm(const std::string &col_name) {
    return gbys_[name_to_gby_[col_name]];
  }

  void AddAggTerm(const std::string &agg_name, ExpressionMaker::ManagedAggExpression expr) {
    auto idx = uint32_t(aggs_.size());
    name_to_agg_.emplace(agg_name, idx);
    aggs_.emplace_back(expr);
  }

  ExpressionMaker::ManagedAggExpression GetAggTerm(const std::string &agg_name) {
    return aggs_[name_to_agg_[agg_name]];
  }

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
}  // namespace terrier::execution::compiler
