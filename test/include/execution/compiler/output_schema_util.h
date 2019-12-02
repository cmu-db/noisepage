#pragma once
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
    cols_.emplace_back(expr->GetReturnValueType(), true, expr);
  }

  void AddOutput(const std::string &col_name, ExpressionMaker::ManagedExpression expr, type::TypeId col_type) {
    auto idx = uint32_t(cols_.size());
    name_to_idx_.emplace(col_name, idx);
    cols_.emplace_back(col_type, true, expr);
  }

  ExpressionMaker::ManagedExpression GetAggTermForOutput(const std::string &agg_name) {
    const auto &agg = aggs[name_to_agg[agg_name]];
    auto ret_type = agg->GetChild(0)->GetReturnValueType();
    auto tve_idx = name_to_agg[agg_name] + uint32_t(gbys.size());
    return expr_maker_->DVE(ret_type, 0, tve_idx);
  }

  ExpressionMaker::ManagedExpression GetGroupByTermForOutput(const std::string &gby_name) {
    const auto &gby = gbys[name_to_gby[gby_name]];
    auto ret_type = gby->GetReturnValueType();
    auto tve_idx = name_to_gby[gby_name];
    return expr_maker_->DVE(ret_type, 0, tve_idx);
  }

  void AddGroupByTerm(const std::string &col_name, ExpressionMaker::ManagedExpression expr) {
    auto idx = uint32_t(gbys.size());
    name_to_gby.emplace(col_name, idx);
    gbys.emplace_back(expr);
  }

  const ExpressionMaker::ManagedExpression GetGroupByTerm(const std::string &col_name) {
    return gbys[name_to_gby[col_name]];
  }

  void AddAggTerm(const std::string &agg_name, ExpressionMaker::ManagedAggExpression expr) {
    auto idx = uint32_t(aggs.size());
    name_to_agg.emplace(agg_name, idx);
    aggs.emplace_back(expr);
  }

  const ExpressionMaker::ManagedAggExpression GetAggTerm(const std::string &agg_name) {
    return aggs[name_to_agg[agg_name]];
  }

  std::unique_ptr<planner::OutputSchema> MakeSchema() { return std::make_unique<planner::OutputSchema>(cols_); }

 private:
  std::unordered_map<std::string, uint32_t> name_to_idx_;
  std::vector<OutputColumn> cols_;
  uint32_t child_idx_;
  std::unordered_map<std::string, uint32_t> name_to_gby;
  std::vector<ExpressionMaker::ManagedExpression> gbys;
  std::unordered_map<std::string, uint32_t> name_to_agg;
  std::vector<ExpressionMaker::ManagedAggExpression> aggs;
  ExpressionMaker *expr_maker_;
};
}  // namespace terrier::execution::compiler
