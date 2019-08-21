#pragma once
#include "execution/compiler/expression_util.h"
#include "planner/plannodes/output_schema.h"

namespace terrier::execution::compiler {
using OutputColumn = planner::OutputSchema::Column;

class OutputSchemaHelper {
 public:
  OutputSchemaHelper(uint32_t child_idx) : child_idx_{child_idx} {}

  ExpressionUtil::Expression GetOutput(uint32_t attr_idx) {
    return ExpressionUtil::DVE(cols_[attr_idx].GetType(), child_idx_, attr_idx);
  }

  ExpressionUtil::Expression GetOutput(std::string col_name) { return GetOutput(name_to_idx_[col_name]); }

  void AddOutput(const std::string &col_name, ExpressionUtil::Expression expr) {
    uint32_t idx = uint32_t(cols_.size());
    name_to_idx_.emplace(col_name, idx);
    cols_.emplace_back(expr->GetReturnValueType(), true, expr);
  }

  void AddOutput(const std::string &col_name, ExpressionUtil::Expression expr, type::TypeId col_type) {
    uint32_t idx = uint32_t(cols_.size());
    name_to_idx_.emplace(col_name, idx);
    cols_.emplace_back(col_type, true, expr);
  }

  ExpressionUtil::Expression GetAggTermForOutput(const std::string &agg_name) {
    auto agg = aggs[name_to_agg[agg_name]];
    auto ret_type = agg->GetChild(0)->GetReturnValueType();
    auto tve_idx = name_to_agg[agg_name] + uint32_t(gbys.size());
    return ExpressionUtil::DVE(ret_type, 0, tve_idx);
  }

  ExpressionUtil::Expression GetGroupByTermForOutput(const std::string &gby_name) {
    auto gby = gbys[name_to_gby[gby_name]];
    auto ret_type = gby->GetReturnValueType();
    auto tve_idx = name_to_gby[gby_name];
    return ExpressionUtil::DVE(ret_type, 0, tve_idx);
  }

  void AddGroupByTerm(const std::string &col_name, ExpressionUtil::Expression expr) {
    uint32_t idx = uint32_t(gbys.size());
    name_to_gby.emplace(col_name, idx);
    gbys.emplace_back(expr);
  }

  ExpressionUtil::Expression GetGroupByTerm(const std::string &col_name) { return gbys[name_to_gby[col_name]]; }

  void AddAggTerm(const std::string &agg_name, ExpressionUtil::AggExpression expr) {
    uint32_t idx = uint32_t(aggs.size());
    name_to_agg.emplace(agg_name, idx);
    aggs.emplace_back(expr);
  }

  ExpressionUtil::AggExpression GetAggTerm(const std::string &agg_name) { return aggs[name_to_agg[agg_name]]; }

  std::shared_ptr<planner::OutputSchema> MakeSchema() { return std::make_shared<planner::OutputSchema>(cols_); }

 private:
  std::unordered_map<std::string, uint32_t> name_to_idx_;
  std::vector<OutputColumn> cols_;
  uint32_t child_idx_;
  std::unordered_map<std::string, uint32_t> name_to_gby;
  std::vector<ExpressionUtil::Expression> gbys;
  std::unordered_map<std::string, uint32_t> name_to_agg;
  std::vector<ExpressionUtil::AggExpression> aggs;
};
}  // namespace terrier::execution::compiler
