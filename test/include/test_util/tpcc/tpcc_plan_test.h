#pragma once

#include <memory>
#include <queue>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "optimizer/optimizer.h"
#include "parser/postgresparser.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "test_util/test_harness.h"

namespace terrier {

namespace tpcc {
class Database;
};

class TpccPlanTest : public TerrierTest {
 public:
  static void CheckIndexScan(TpccPlanTest *test, parser::SelectStatement *sel_stmt, catalog::table_oid_t tbl_oid,
                             std::unique_ptr<planner::AbstractPlanNode> plan);

  void SetUp() override;
  void TearDown() override;
  void BeginTransaction();
  void EndTransaction(bool commit);

  std::unique_ptr<planner::AbstractPlanNode> Optimize(const std::string &query, catalog::table_oid_t tbl_oid,
                                                      parser::StatementType stmt_type);

  // Optimizer on Insert
  void OptimizeInsert(const std::string &query, catalog::table_oid_t tbl_oid);

  // Optimize an Update
  void OptimizeUpdate(const std::string &query, catalog::table_oid_t tbl_oid,
                      void (*Check)(TpccPlanTest *test, catalog::table_oid_t tbl_oid,
                                    std::unique_ptr<planner::AbstractPlanNode> plan));

  // Optimize a Delete
  void OptimizeDelete(const std::string &query, catalog::table_oid_t tbl_oid,
                      void (*Check)(TpccPlanTest *test, catalog::table_oid_t tbl_oid,
                                    std::unique_ptr<planner::AbstractPlanNode> plan));

  // Optimize a Query
  void OptimizeQuery(const std::string &query, catalog::table_oid_t tbl_oid,
                     void (*Check)(TpccPlanTest *test, parser::SelectStatement *sel_stmt, catalog::table_oid_t tbl_oid,
                                   std::unique_ptr<planner::AbstractPlanNode> plan));

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

  // Database
  tpcc::Database *tpcc_db_;

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

  catalog::index_oid_t pk_new_order_;
};

}  // namespace terrier
