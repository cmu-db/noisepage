#pragma once

#include <memory>
#include <queue>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_accessor.h"
#include "main/db_main.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/optimizer.h"
#include "optimizer/properties.h"
#include "optimizer/property_set.h"
#include "optimizer/statistics/stats_storage.h"
#include "parser/postgresparser.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/insert_plan_node.h"
#include "settings/settings_manager.h"
#include "storage/garbage_collector.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "util/tpcc/builder.h"

#define __SETTING_GFLAGS_DEFINE__      // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_GFLAGS_DEFINE__       // NOLINT

namespace terrier {

class TpccPlanTest : public TerrierTest {
 public:
  static void CheckIndexScan(TpccPlanTest *test, parser::SelectStatement *sel_stmt, catalog::table_oid_t tbl_oid,
                             std::shared_ptr<planner::AbstractPlanNode> plan) {
    const planner::AbstractPlanNode *node = plan.get();
    while (node != nullptr) {
      if (node->GetPlanNodeType() == planner::PlanNodeType::INDEXSCAN) {
        EXPECT_EQ(node->GetChildrenSize(), 0);
        break;
      }

      EXPECT_LE(node->GetChildrenSize(), 1);
      if (node->GetChildrenSize() == 0) {
        node = nullptr;
        EXPECT_TRUE(false);
      } else {
        node = node->GetChild(0);
      }
    }
  }

  void SetUp() override {
    std::unordered_map<settings::Param, settings::ParamInfo> param_map;
    terrier::settings::SettingsManager::ConstructParamMap(param_map);
    db_main_ = new DBMain(std::move(param_map));
    settings_manager_ = db_main_->settings_manager_;
    stats_storage_ = new optimizer::StatsStorage();

    timestamp_manager_ = new transaction::TimestampManager;
    deferred_action_manager_ = new transaction::DeferredActionManager(timestamp_manager_);
    txn_manager_ = new transaction::TransactionManager(timestamp_manager_, deferred_action_manager_, &buffer_pool_,
                                                       true, DISABLED);
    gc_ = new storage::GarbageCollector(timestamp_manager_, deferred_action_manager_, txn_manager_, DISABLED);

    catalog_ = new catalog::Catalog(txn_manager_, &block_store_);
    tpcc::Builder tpcc_builder(&block_store_, catalog_, txn_manager_);
    auto *const tpcc_db = tpcc_builder.Build(storage::index::IndexType::BWTREE);

    db_ = tpcc_db->db_oid_;
    tbl_item_ = tpcc_db->item_table_oid_;
    tbl_warehouse_ = tpcc_db->warehouse_table_oid_;
    tbl_stock_ = tpcc_db->stock_table_oid_;
    tbl_district_ = tpcc_db->district_table_oid_;
    tbl_customer_ = tpcc_db->customer_table_oid_;
    tbl_history_ = tpcc_db->history_table_oid_;
    tbl_new_order_ = tpcc_db->new_order_table_oid_;
    tbl_order_ = tpcc_db->order_table_oid_;
    tbl_order_line_ = tpcc_db->order_line_table_oid_;

    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
  }

  void TearDown() override {
    // Cleanup the catalog
    catalog_->TearDown();

    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();

    delete catalog_;
    delete gc_;
    delete txn_manager_;
    delete db_main_;
    delete deferred_action_manager_;
    delete timestamp_manager_;
    delete stats_storage_;
  }

  void BeginTransaction() {
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(txn_, db_).release();
  }

  void EndTransaction(bool commit) {
    delete accessor_;
    if (commit)
      txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    else
      txn_manager_->Abort(txn_);
  }

  // Get predicates...
  void GenerateTableAliasSet(const parser::AbstractExpression *expr, std::unordered_set<std::string> *table_alias_set) {
    if (expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
      table_alias_set->insert(reinterpret_cast<const parser::ColumnValueExpression *>(expr)->GetTableName());
    } else {
      for (size_t i = 0; i < expr->GetChildrenSize(); i++)
        GenerateTableAliasSet(expr->GetChild(i).Get(), table_alias_set);
    }
  }

  std::vector<optimizer::AnnotatedExpression> ExtractPredicates(const parser::AbstractExpression *expr) {
    std::vector<const parser::AbstractExpression *> preds;
    SplitPredicates(expr, &preds);

    std::vector<optimizer::AnnotatedExpression> annotated;
    for (auto &pred : preds) {
      std::unordered_set<std::string> table_alias;
      GenerateTableAliasSet(pred, &table_alias);
      annotated.emplace_back(common::ManagedPointer<const parser::AbstractExpression>(pred), std::move(table_alias));
    }

    return annotated;
  }

  void SplitPredicates(const parser::AbstractExpression *expr, std::vector<const parser::AbstractExpression *> *preds) {
    if (expr->GetExpressionType() == parser::ExpressionType::CONJUNCTION_AND) {
      for (size_t idx = 0; idx < expr->GetChildrenSize(); idx++) {
        SplitPredicates(expr->GetChild(idx).Get(), preds);
      }
    } else {
      preds->push_back(expr);
    }
  }

  // Binding ColumnValueExpressions and "anti-duplicate" binding protection
  void BindColumnValues(const parser::AbstractExpression *expr, catalog::table_oid_t tbl_oid, std::string tbl_alias) {
    std::set<std::string> seen_names;
    std::queue<const parser::AbstractExpression *> frontier;
    frontier.push(expr);
    while (!frontier.empty()) {
      auto front = frontier.front();
      frontier.pop();

      for (size_t idx = 0; idx < front->GetChildrenSize(); idx++) {
        frontier.push(front->GetChild(idx).Get());
      }

      if (front->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
        auto *cve = reinterpret_cast<const parser::ColumnValueExpression *>(front);
        EXPECT_TRUE(seen_names.find(cve->GetColumnName()) == seen_names.end());
        auto *ccve = const_cast<parser::ColumnValueExpression *>(cve);

        seen_names.insert(cve->GetColumnName());

        ccve->SetDatabaseOID(db_);
        ccve->SetTableOID(tbl_oid);
        ccve->table_name_ = tbl_alias;

        auto &schema = accessor_->GetSchema(tbl_oid);
        auto col_oid = schema.GetColumn(ccve->GetColumnName()).Oid();
        EXPECT_TRUE(col_oid != catalog::INVALID_COLUMN_OID);

        ccve->SetColumnOID(col_oid);
        ccve->SetReturnValueType(schema.GetColumn(ccve->GetColumnName()).Type());
      }

      // Unfortunate binder hack!
      const_cast<parser::AbstractExpression *>(front)->DeriveExpressionName();
      const_cast<parser::AbstractExpression *>(front)->DeriveReturnValueType();
    }
  }

  // Optimizer on Insert
  void OptimizeInsert(std::string query, catalog::table_oid_t tbl_oid) {
    parser::PostgresParser pgparser;
    auto stmt_list = pgparser.BuildParseTree(query);
    auto ins_stmt = stmt_list.GetStatement(0).CastManagedPointerTo<parser::InsertStatement>();

    BeginTransaction();

    auto &schema = accessor_->GetSchema(tbl_oid);
    std::vector<catalog::col_oid_t> col_oids;
    for (auto &col : *ins_stmt->GetInsertColumns()) {
      col_oids.push_back(schema.GetColumn(col).Oid());
    }

    auto property_set = new optimizer::PropertySet();
    optimizer::OperatorExpression *plan = nullptr;
    {
      std::vector<catalog::col_oid_t> oids = col_oids;
      std::vector<std::vector<common::ManagedPointer<const parser::AbstractExpression>>> ins = {row};
      plan = new optimizer::OperatorExpression(optimizer::LogicalInsert::Make(db_, accessor_->GetDefaultNamespace(),
                                                                              tbl_oid, std::move(oids), std::move(ins)),
                                               {});
    }

    auto query_info = optimizer::QueryInfo(parser::StatementType::INSERT, {}, property_set);
    auto optimizer = new optimizer::Optimizer(new optimizer::TrivialCostModel());
    auto out_plan = optimizer->BuildPlanTree(plan, query_info, txn_, settings_manager_, accessor_, stats_storage_);

    EXPECT_EQ(out_plan->GetPlanNodeType(), planner::PlanNodeType::INSERT);
    auto insert = std::dynamic_pointer_cast<planner::InsertPlanNode>(out_plan);
    EXPECT_EQ(insert->GetDatabaseOid(), db_);
    EXPECT_EQ(insert->GetNamespaceOid(), accessor_->GetDefaultNamespace());
    EXPECT_EQ(insert->GetTableOid(), tbl_oid);
    EXPECT_EQ(insert->GetParameterInfo(), col_oids);
    EXPECT_EQ(insert->GetBulkInsertCount(), 1);
    EXPECT_EQ(insert->GetValues(0).size(), row.size());
    for (size_t idx = 0; idx < row.size(); idx++) {
      EXPECT_EQ(row[idx].Get(), insert->GetValues(0)[idx]);
    }

    delete plan;
    delete property_set;
    delete optimizer;
    EndTransaction(true);
  }

  // Optimize an Update
  void OptimizeUpdate(std::string query, std::string tbl_alias, catalog::table_oid_t tbl_oid) {
    parser::PostgresParser pgparser;
    auto stmt_list = pgparser.BuildParseTree(query);
    auto upd_stmt = reinterpret_cast<parser::UpdateStatement *>(stmt_list[0].get());

    BeginTransaction();
    std::vector<optimizer::OperatorExpression *> child;
    if (upd_stmt->GetUpdateCondition()) {
      auto *upd_cond = upd_stmt->GetUpdateCondition()->Copy();
      txn_->RegisterCommitAction([=]() { delete upd_cond; });
      txn_->RegisterAbortAction([=]() { delete upd_cond; });
      BindColumnValues(upd_cond, tbl_oid, tbl_alias);

      auto predicates = ExtractPredicates(upd_cond);
      auto *get = new optimizer::OperatorExpression(
          optimizer::LogicalGet::Make(db_, accessor_->GetDefaultNamespace(), tbl_oid, predicates, tbl_alias, false),
          {});
      child.push_back(get);
    }

    for (auto &upd : upd_stmt->GetUpdateClauses()) {
      BindColumnValues(upd->GetUpdateValue().Get(), tbl_oid, tbl_alias);
    }

    std::vector<common::ManagedPointer<const parser::UpdateClause>> clauses;
    for (auto &upd : upd_stmt->GetUpdateClauses()) {
      clauses.emplace_back(upd.get());
    }

    auto plan = new optimizer::OperatorExpression(
        optimizer::LogicalUpdate::Make(db_, accessor_->GetDefaultNamespace(), tbl_alias, tbl_oid, std::move(clauses)),
        std::move(child));

    auto property_set = new optimizer::PropertySet();
    auto query_info = optimizer::QueryInfo(parser::StatementType::UPDATE, {}, property_set);
    auto optimizer = new optimizer::Optimizer(new optimizer::TrivialCostModel());
    auto out_plan = optimizer->BuildPlanTree(plan, query_info, txn_, settings_manager_, accessor_, stats_storage_);

    EXPECT_EQ(out_plan->GetPlanNodeType(), planner::PlanNodeType::UPDATE);
    EXPECT_EQ(out_plan->GetChildrenSize(), 1);
    EXPECT_EQ(out_plan->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);

    delete plan;
    delete property_set;
    delete optimizer;
    EndTransaction(true);
  }

  // Optimize a Query
  void OptimizeQuery(std::string query, std::string tbl_alias, catalog::table_oid_t tbl_oid,
                     void (*Check)(TpccPlanTest *test, parser::SelectStatement *sel_stmt, catalog::table_oid_t tbl_oid,
                                   std::shared_ptr<planner::AbstractPlanNode> plan)) {
    parser::PostgresParser pgparser;
    auto stmt_list = pgparser.BuildParseTree(query);
    auto sel_stmt = reinterpret_cast<parser::SelectStatement *>(stmt_list[0].get());

    BeginTransaction();

    // Bind Select and Create Output
    auto property_set = new optimizer::PropertySet();
    std::vector<common::ManagedPointer<const parser::AbstractExpression>> output;
    for (auto idx = 0u; idx < sel_stmt->GetSelectColumnsSize(); idx++) {
      BindColumnValues(sel_stmt->GetSelectColumn(idx).Get(), tbl_oid, tbl_alias);
      output.emplace_back(sel_stmt->GetSelectColumn(idx).Get());
    }

    // Build Get Plan
    auto plan = new optimizer::OperatorExpression(
        optimizer::LogicalGet::Make(db_, accessor_->GetDefaultNamespace(), tbl_oid, {}, tbl_alias, false), {});

    // Build Filter if exists
    if (sel_stmt->GetSelectCondition()) {
      auto predicate = sel_stmt->GetSelectCondition()->Copy();
      txn_->RegisterCommitAction([=]() { delete predicate; });
      txn_->RegisterAbortAction([=]() { delete predicate; });
      BindColumnValues(predicate, tbl_oid, tbl_alias);

      auto predicates = ExtractPredicates(predicate);
      auto children = {plan};
      plan =
          new optimizer::OperatorExpression(optimizer::LogicalFilter::Make(std::move(predicates)), std::move(children));
    }

    std::vector<planner::OrderByOrderingType> sort_dirs;
    std::vector<common::ManagedPointer<const parser::AbstractExpression>> sort_exprs;
    if (sel_stmt->GetSelectOrderBy()) {
      auto order_by = sel_stmt->GetSelectOrderBy();
      for (size_t idx = 0; idx < order_by->GetOrderByExpressionsSize(); idx++) {
        auto ob_type = order_by->GetOrderByTypes()[idx];
        auto ob_expr = order_by->GetOrderByExpression(idx).Get();
        BindColumnValues(ob_expr, tbl_oid, tbl_alias);

        sort_exprs.emplace_back(ob_expr);
        sort_dirs.push_back(ob_type == parser::OrderType::kOrderAsc ? planner::OrderByOrderingType::ASC
                                                                    : planner::OrderByOrderingType::DESC);
      }

      auto sort_prop = new optimizer::PropertySort(sort_exprs, sort_dirs);
      property_set->AddProperty(sort_prop);
    }

    // Limit
    auto sel_limit = sel_stmt->GetSelectLimit();
    if (sel_stmt && sel_limit->GetLimit() != -1) {
      auto lim_child = {plan};
      auto offset = sel_stmt->GetSelectLimit()->GetOffset();
      auto limit = sel_stmt->GetSelectLimit()->GetLimit();
      plan = new optimizer::OperatorExpression(
          optimizer::LogicalLimit::Make(offset, limit, std::move(sort_exprs), std::move(sort_dirs)),
          std::move(lim_child));
    }

    auto query_info = optimizer::QueryInfo(parser::StatementType::SELECT, std::move(output), property_set);
    auto optimizer = new optimizer::Optimizer(new optimizer::TrivialCostModel());
    auto out_plan = optimizer->BuildPlanTree(plan, query_info, txn_, settings_manager_, accessor_, stats_storage_);

    // Check Plan for correctness
    Check(this, sel_stmt, tbl_oid, out_plan);

    delete plan;
    delete property_set;
    delete optimizer;
    EndTransaction(true);
  }

  catalog::Catalog *catalog_;
  transaction::TransactionManager *txn_manager_;

  storage::RecordBufferSegmentPool buffer_pool_{100, 100};
  storage::BlockStore block_store_{100, 100};
  storage::GarbageCollector *gc_;

  DBMain *db_main_;
  settings::SettingsManager *settings_manager_;
  optimizer::StatsStorage *stats_storage_;

  // Optimizer transaction
  transaction::TransactionContext *txn_;
  transaction::DeferredActionManager *deferred_action_manager_;
  transaction::TimestampManager *timestamp_manager_;
  catalog::CatalogAccessor *accessor_;

  catalog::db_oid_t db_;

  // OIDs
  catalog::table_oid_t tbl_item_;
  catalog::table_oid_t tbl_warehouse_;
  catalog::table_oid_t tbl_stock_;
  catalog::table_oid_t tbl_district_;
  catalog::table_oid_t tbl_customer_;
  catalog::table_oid_t tbl_history_;
  catalog::table_oid_t tbl_new_order_;
  catalog::table_oid_t tbl_order_;
  catalog::table_oid_t tbl_order_line_;
};

}  // namespace terrier
