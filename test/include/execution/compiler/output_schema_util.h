#pragma once

#include "execution/compiler/expression_maker.h"
#include "planner/plannodes/output_schema.h"

namespace terrier::execution::compiler {

using OutputColumn = planner::OutputSchema::Column;

class OutputSchemaHelper {
 public:
  OutputSchemaHelper(ExpressionMaker *expr_maker, uint32_t child_idx)
      : expr_maker_(expr_maker), child_idx_{child_idx} {}

  ExpressionMaker::Expression GetOutput(uint32_t attr_idx) {
    return expr_maker_->DVE(cols_[attr_idx].GetType(), child_idx_, attr_idx);
  }

  ExpressionMaker::Expression GetOutput(const std::string &col_name) {
    return GetOutput(name_to_idx_.at(col_name));
  }

  void AddOutput(const std::string &col_name, ExpressionMaker::Expression expr) {
    auto idx = uint32_t(cols_.size());
    name_to_idx_.emplace(col_name, idx);
    cols_.emplace_back(expr->GetReturnValueType(), true, expr);
  }

  void AddOutput(const std::string &col_name, ExpressionMaker::Expression expr,
                 sql::TypeId col_type) {
    auto idx = uint32_t(cols_.size());
    name_to_idx_.emplace(col_name, idx);
    cols_.emplace_back(col_type, true, expr);
  }

  ExpressionMaker::Expression GetAggTermForOutput(const std::string &agg_name) {
    auto agg = aggs[name_to_agg[agg_name]];
    auto ret_type = agg->GetChild(0)->GetReturnValueType();
    auto tve_idx = name_to_agg[agg_name];
    return expr_maker_->DVE(ret_type, 1, tve_idx);
  }

  ExpressionMaker::Expression GetGroupByTermForOutput(const std::string &gby_name) {
    auto gby = gbys[name_to_gby[gby_name]];
    auto ret_type = gby->GetReturnValueType();
    auto tve_idx = name_to_gby[gby_name];
    return expr_maker_->DVE(ret_type, 0, tve_idx);
  }

  void AddGroupByTerm(const std::string &col_name, ExpressionMaker::Expression expr) {
    auto idx = uint32_t(gbys.size());
    name_to_gby.emplace(col_name, idx);
    gbys.emplace_back(expr);
  }

  ExpressionMaker::Expression GetGroupByTerm(const std::string &col_name) {
    return gbys[name_to_gby[col_name]];
  }

  void AddAggTerm(const std::string &agg_name, ExpressionMaker::AggExpression expr) {
    auto idx = uint32_t(aggs.size());
    name_to_agg.emplace(agg_name, idx);
    aggs.emplace_back(expr);
  }

  ExpressionMaker::AggExpression GetAggTerm(const std::string &agg_name) {
    return aggs[name_to_agg[agg_name]];
  }

  std::unique_ptr<planner::OutputSchema> MakeSchema() {
    return std::make_unique<planner::OutputSchema>(cols_);
  }

 private:
  ExpressionMaker *expr_maker_;
  std::unordered_map<std::string, uint32_t> name_to_idx_;
  std::vector<OutputColumn> cols_;
  uint32_t child_idx_;
  std::unordered_map<std::string, uint32_t> name_to_gby;
  std::vector<ExpressionMaker::Expression> gbys;
  std::unordered_map<std::string, uint32_t> name_to_agg;
  std::vector<ExpressionMaker::AggExpression> aggs;
};
}  // namespace terrier::execution::compiler
