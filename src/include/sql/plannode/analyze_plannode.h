//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// analyze_plan.h
//
// Identification: src/include/planner/analyze_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/*
 * TODO:
 * 1 How to handle analyze with new storage layer
 * 2 What datatable object to use
 */

#pragma once

#include "abstract_plannode.h"
#include <vector>

namespace terrier::sql::plannode {

class AnalyzePlanNode : public AbstractPlanNode {
 public:
  AnalyzePlanNode(const AnalyzePlanNode &) = delete;
  AnalyzePlanNode &operator=(const AnalyzePlanNode &) = delete;
  AnalyzePlanNode(AnalyzePlanNode &&) = delete;
  AnalyzePlanNode &operator=(AnalyzePlanNode &&) = delete;

  // TODO: Decide which of these to keep
//  explicit AnalyzePlan(storage::DataTable *table);
//
//  explicit AnalyzePlan(std::string table_name, std::string schema_name,
//                       std::string database_name,
//                       concurrency::TransactionContext *txn);
//
//  explicit AnalyzePlan(std::string table_name, std::string schema_name,
//                       std::string database_name,
//                       std::vector<char *> column_names,
//                       concurrency::TransactionContext *txn);
//
//  explicit AnalyzePlan(parser::AnalyzeStatement *parse_tree,
//                       concurrency::TransactionContext *txn);

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::ANALYZE; }

//  inline storage::DataTable *GetTable() const { return target_table_; }
//
//  inline std::string GetTableName() const { return table_name_; }

  inline std::vector<char *> GetColumnNames() const { return column_names_; }

  const std::string GetInfo() const { return "Analyze table Plan"; }

  inline std::unique_ptr<AbstractPlanNode> Copy() const {
    return std::unique_ptr<AbstractPlanNode>(new AnalyzePlanNode(target_table_));
  }

 private:
  storage::DataTable *target_table_ = nullptr;
  std::string table_name_;
  std::vector<char *> column_names_;
};

}  // namespace planner
