#include <memory>
#include <queue>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "catalog/catalog_accessor.h"
#include "main/db_main.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/optimizer.h"
#include "optimizer/properties.h"
#include "optimizer/property_set.h"
#include "optimizer/query_to_operator_transformer.h"
#include "optimizer/statistics/stats_storage.h"
#include "parser/postgresparser.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/insert_plan_node.h"
#include "settings/settings_manager.h"
#include "storage/garbage_collector.h"
#include "test_util/test_harness.h"
#include "test_util/tpcc/builder.h"
#include "transaction/transaction_manager.h"

#define __SETTING_GFLAGS_DEFINE__      // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_GFLAGS_DEFINE__       // NOLINT
#include "test_util/tpcc/tpcc_plan_test.h"

namespace terrier {

void TpccPlanTest::CheckIndexScan(TpccPlanTest *test, parser::SelectStatement *sel_stmt, catalog::table_oid_t tbl_oid,
                                  std::unique_ptr<planner::AbstractPlanNode> plan) {
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

void TpccPlanTest::SetUp() {
  std::unordered_map<settings::Param, settings::ParamInfo> param_map;
  terrier::settings::SettingsManager::ConstructParamMap(param_map);
  db_main_ = new DBMain(std::move(param_map));
  settings_manager_ = db_main_->settings_manager_;
  stats_storage_ = new optimizer::StatsStorage();

  timestamp_manager_ = new transaction::TimestampManager;
  deferred_action_manager_ = new transaction::DeferredActionManager(timestamp_manager_);
  txn_manager_ =
      new transaction::TransactionManager(timestamp_manager_, deferred_action_manager_, &buffer_pool_, true, DISABLED);
  gc_ = new storage::GarbageCollector(timestamp_manager_, deferred_action_manager_, txn_manager_, DISABLED);

  catalog_ = new catalog::Catalog(txn_manager_, &block_store_);
  tpcc::Builder tpcc_builder(&block_store_, catalog_, txn_manager_);
  tpcc_db_ = tpcc_builder.Build(storage::index::IndexType::BWTREE);

  db_ = tpcc_db_->db_oid_;
  tbl_item_ = tpcc_db_->item_table_oid_;
  tbl_warehouse_ = tpcc_db_->warehouse_table_oid_;
  tbl_stock_ = tpcc_db_->stock_table_oid_;
  tbl_district_ = tpcc_db_->district_table_oid_;
  tbl_customer_ = tpcc_db_->customer_table_oid_;
  tbl_history_ = tpcc_db_->history_table_oid_;
  tbl_new_order_ = tpcc_db_->new_order_table_oid_;
  tbl_order_ = tpcc_db_->order_table_oid_;
  tbl_order_line_ = tpcc_db_->order_line_table_oid_;
  pk_new_order_ = tpcc_db_->new_order_primary_index_oid_;

  gc_->PerformGarbageCollection();
  gc_->PerformGarbageCollection();
}

void TpccPlanTest::TearDown() {
  // Cleanup the catalog
  catalog_->TearDown();

  gc_->PerformGarbageCollection();
  gc_->PerformGarbageCollection();
  gc_->PerformGarbageCollection();

  delete tpcc_db_;
  delete catalog_;
  delete gc_;
  delete txn_manager_;
  delete db_main_;
  delete deferred_action_manager_;
  delete timestamp_manager_;
  delete stats_storage_;
}

void TpccPlanTest::BeginTransaction() {
  txn_ = txn_manager_->BeginTransaction();
  accessor_ = catalog_->GetAccessor(txn_, db_).release();
}

void TpccPlanTest::EndTransaction(bool commit) {
  delete accessor_;
  if (commit)
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  else
    txn_manager_->Abort(txn_);
}

std::unique_ptr<planner::AbstractPlanNode> TpccPlanTest::Optimize(const std::string &query,
                                                                  catalog::table_oid_t tbl_oid,
                                                                  parser::StatementType stmt_type) {
  parser::PostgresParser pgparser;
  auto stmt_list = pgparser.BuildParseTree(query);

  // Bind + Transform
  auto accessor = catalog_->GetAccessor(txn_, db_);
  auto *binder = new binder::BindNodeVisitor(std::move(accessor), "tpcc");
  binder->BindNameToNode(stmt_list.GetStatement(0), &stmt_list);
  accessor = binder->GetCatalogAccessor();
  auto *transformer = new optimizer::QueryToOperatorTransformer(std::move(accessor));
  auto plan = transformer->ConvertToOpExpression(stmt_list.GetStatement(0), &stmt_list);
  delete binder;
  delete transformer;

  auto optimizer = new optimizer::Optimizer(new optimizer::TrivialCostModel());
  std::unique_ptr<planner::AbstractPlanNode> out_plan;
  if (stmt_type == parser::StatementType::SELECT) {
    auto sel_stmt = stmt_list.GetStatement(0).CastManagedPointerTo<parser::SelectStatement>();

    // Output
    auto output = sel_stmt->GetSelectColumns();

    // Property Sort
    auto property_set = new optimizer::PropertySet();
    std::vector<optimizer::OrderByOrderingType> sort_dirs;
    std::vector<common::ManagedPointer<parser::AbstractExpression>> sort_exprs;
    if (sel_stmt->GetSelectOrderBy()) {
      auto order_by = sel_stmt->GetSelectOrderBy();
      auto types = order_by->GetOrderByTypes();
      auto exprs = order_by->GetOrderByExpressions();
      for (size_t idx = 0; idx < order_by->GetOrderByExpressionsSize(); idx++) {
        sort_exprs.emplace_back(exprs[idx]);
        sort_dirs.push_back(types[idx] == parser::OrderType::kOrderAsc ? optimizer::OrderByOrderingType::ASC
                                                                       : optimizer::OrderByOrderingType::DESC);
      }

      auto sort_prop = new optimizer::PropertySort(sort_exprs, sort_dirs);
      property_set->AddProperty(sort_prop);
    }

    auto query_info = optimizer::QueryInfo(parser::StatementType::SELECT, std::move(output), property_set);
    out_plan =
        optimizer->BuildPlanTree(txn_, accessor_, settings_manager_, stats_storage_, query_info, std::move(plan));
    delete property_set;
  } else if (stmt_type == parser::StatementType::INSERT) {
    auto ins_stmt = stmt_list.GetStatement(0).CastManagedPointerTo<parser::InsertStatement>();

    auto &schema = accessor_->GetSchema(tbl_oid);
    std::vector<catalog::col_oid_t> col_oids;
    for (auto &col : *ins_stmt->GetInsertColumns()) {
      col_oids.push_back(schema.GetColumn(col).Oid());
    }

    auto property_set = new optimizer::PropertySet();
    auto query_info = optimizer::QueryInfo(stmt_type, {}, property_set);
    out_plan =
        optimizer->BuildPlanTree(txn_, accessor_, settings_manager_, stats_storage_, query_info, std::move(plan));
    delete property_set;

    EXPECT_EQ(out_plan->GetPlanNodeType(), planner::PlanNodeType::INSERT);
    auto insert = reinterpret_cast<planner::InsertPlanNode *>(out_plan.get());
    EXPECT_EQ(insert->GetDatabaseOid(), db_);
    EXPECT_EQ(insert->GetNamespaceOid(), accessor_->GetDefaultNamespace());
    EXPECT_EQ(insert->GetTableOid(), tbl_oid);
    EXPECT_EQ(insert->GetParameterInfo(), col_oids);
    EXPECT_EQ(insert->GetBulkInsertCount(), 1);

    auto values = *(ins_stmt->GetValues());
    EXPECT_EQ(values.size(), 1);
    EXPECT_EQ(insert->GetValues(0).size(), values[0].size());
    for (size_t idx = 0; idx < ins_stmt->GetValues()->size(); idx++) {
      EXPECT_EQ(*(values[0][idx].Get()), *(insert->GetValues(0)[idx].Get()));
    }
  } else {
    auto property_set = new optimizer::PropertySet();
    auto query_info = optimizer::QueryInfo(stmt_type, {}, property_set);
    out_plan =
        optimizer->BuildPlanTree(txn_, accessor_, settings_manager_, stats_storage_, query_info, std::move(plan));
    delete property_set;
  }

  delete optimizer;
  return out_plan;
}

void TpccPlanTest::OptimizeUpdate(const std::string &query, catalog::table_oid_t tbl_oid,
                                  void (*Check)(TpccPlanTest *test, catalog::table_oid_t tbl_oid,
                                                std::unique_ptr<planner::AbstractPlanNode> plan)) {
  BeginTransaction();
  auto plan = Optimize(query, tbl_oid, parser::StatementType::UPDATE);
  EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::UPDATE);
  EXPECT_EQ(plan->GetChildrenSize(), 1);
  EXPECT_EQ(plan->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);
  Check(this, tbl_oid, std::move(plan));
  EndTransaction(true);
}

void TpccPlanTest::OptimizeDelete(const std::string &query, catalog::table_oid_t tbl_oid,
                                  void (*Check)(TpccPlanTest *test, catalog::table_oid_t tbl_oid,
                                                std::unique_ptr<planner::AbstractPlanNode> plan)) {
  BeginTransaction();
  auto plan = Optimize(query, tbl_oid, parser::StatementType::DELETE);
  Check(this, tbl_oid, std::move(plan));
  EndTransaction(true);
}

void TpccPlanTest::OptimizeInsert(const std::string &query, catalog::table_oid_t tbl_oid) {
  BeginTransaction();
  Optimize(query, tbl_oid, parser::StatementType::INSERT);
  EndTransaction(true);
}

void TpccPlanTest::OptimizeQuery(const std::string &query, catalog::table_oid_t tbl_oid,
                                 void (*Check)(TpccPlanTest *test, parser::SelectStatement *sel_stmt,
                                               catalog::table_oid_t tbl_oid,
                                               std::unique_ptr<planner::AbstractPlanNode> plan)) {
  BeginTransaction();
  parser::PostgresParser pgparser;
  auto stmt_list = pgparser.BuildParseTree(query);
  auto sel_stmt = stmt_list.GetStatement(0).CastManagedPointerTo<parser::SelectStatement>();
  auto plan = Optimize(query, tbl_oid, parser::StatementType::SELECT);
  Check(this, sel_stmt.Get(), tbl_oid, std::move(plan));
  EndTransaction(true);
}

}  // namespace terrier
