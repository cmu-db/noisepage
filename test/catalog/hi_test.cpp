#include "catalog/catalog.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <assert.h>

#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "catalog/postgres/pg_namespace.h"
#include "main/db_main.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "storage/index/index_builder.h"
#include "storage/sql_table.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"
#include "type/transient_value_factory.h"
#include "catalog/postgres/pg_constraint.h"
#include "common/macros.h"
#include "execution/exec/execution_context.h"
#include "planner/plannodes/create_database_plan_node.h"
#include "planner/plannodes/create_index_plan_node.h"
#include "planner/plannodes/create_namespace_plan_node.h"
#include "planner/plannodes/create_table_plan_node.h"
#include "planner/plannodes/drop_database_plan_node.h"
#include "planner/plannodes/drop_index_plan_node.h"
#include "planner/plannodes/drop_namespace_plan_node.h"
#include "planner/plannodes/drop_table_plan_node.h"
#include "benchmark_util/data_table_benchmark_util.h"
#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "catalog/postgres/pg_proc.h"
#include "loggers/binder_logger.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/postgresparser.h"
#include "storage/garbage_collector.h"
#include "transaction/deferred_action_manager.h"

namespace terrier {

// NOLINTNEXTLINE
/**
 * Verify that the constraint table sand their related inidies are present and valid within 
 * the catalog and catalog accessor
 */
int main() {
  auto db_main_ = terrier::DBMain::Builder().SetUseGC(true).SetUseCatalog(true).Build();
  auto txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
  auto catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
  auto *txn = txn_manager_->BeginTransaction();
  auto db_ = catalog_->GetDatabaseOid(common::ManagedPointer(txn), catalog::DEFAULT_DATABASE);
  
  auto accessor = *(catalog_->GetAccessor(common::ManagedPointer(txn), db_));
  auto ns_oid = accessor.GetNamespaceOid("pg_catalog");
  assert(ns_oid != catalog::INVALID_NAMESPACE_OID);
  assert(ns_oid == catalog::postgres::NAMESPACE_CATALOG_NAMESPACE_OID);

  auto con_table = accessor.GetTableOid(ns_oid, "pg_constraint");
  assert(con_table == catalog::postgres::CONSTRAINT_TABLE_OID);
  std::unordered_set<catalog::index_oid_t> con_table_index_set;
  con_table_index_set.insert(catalog::postgres::CONSTRAINT_OID_INDEX_OID);
  con_table_index_set.insert(catalog::postgres::CONSTRAINT_NAME_INDEX_OID);
  con_table_index_set.insert(catalog::postgres::CONSTRAINT_NAMESPACE_INDEX_OID);
  con_table_index_set.insert(catalog::postgres::CONSTRAINT_TABLE_INDEX_OID);
  con_table_index_set.insert(catalog::postgres::CONSTRAINT_INDEX_INDEX_OID);

  std::vector<catalog::index_oid_t> idx_table = accessor.GetIndexOids(con_table);
  std::unordered_set<catalog::index_oid_t> con_visited;
  assert(con_table_index_set.size() == idx_table.size());
  for (catalog::index_oid_t idx : idx_table) {
    assert(con_table_index_set.count(idx) == 1);
    assert(con_visited.count(idx) == 0);
    con_visited.insert(idx);
  }

  auto fk_table = accessor.GetTableOid(ns_oid, "fk_constraint");
  assert(fk_table == catalog::postgres::FK_TABLE_OID);
  std::unordered_set<catalog::index_oid_t> fk_table_index_set;
  fk_table_index_set.insert(catalog::postgres::FK_OID_INDEX_OID);
  fk_table_index_set.insert(catalog::postgres::FK_CON_OID_INDEX_OID);
  fk_table_index_set.insert(catalog::postgres::FK_SRC_TABLE_OID_INDEX_OID);
  fk_table_index_set.insert(catalog::postgres::FK_REF_TABLE_OID_INDEX_OID);

  std::vector<catalog::index_oid_t> fk_index = accessor.GetIndexOids(fk_table);
  std::unordered_set<catalog::index_oid_t> fk_visited;
  assert(fk_table_index_set.size() == fk_index.size());
  for (catalog::index_oid_t idx : fk_index) {
    assert(fk_table_index_set.count(idx) == 1);
    assert (fk_visited.count(idx) == 0);
    fk_visited.insert(idx);
  }

  auto check_table = accessor.GetTableOid(ns_oid, "check_constraint");
  assert(check_table != catalog::postgres::CHECK_TABLE_OID);
  std::unordered_set<catalog::index_oid_t> check_table_index_set;
  check_table_index_set.insert(catalog::postgres::CHECK_OID_INDEX_OID);

  std::vector<catalog::index_oid_t> check_index = accessor.GetIndexOids(check_table);
  std::unordered_set<catalog::index_oid_t> check_visited;
  assert(check_table_index_set.size() == check_index.size());
  for (catalog::index_oid_t idx : check_index) {
    assert(check_table_index_set.count(idx) == 1);
    assert(check_visited.count(idx) == 0);
    check_visited.insert(idx);
  }

  auto exclusion_table = accessor.GetTableOid(ns_oid, "exclusion_constraint");
  assert(exclusion_table != catalog::postgres::EXCLUSION_TABLE_OID);
  std::unordered_set<catalog::index_oid_t> exclusion_table_index_set;
  exclusion_table_index_set.insert(catalog::postgres::EXCLUSION_OID_INDEX_OID);

  std::vector<catalog::index_oid_t> cexclusion_index = accessor.GetIndexOids(exclusion_table);
  std::unordered_set<catalog::index_oid_t> exclusion_visited;
  assert(exclusion_table_index_set.size() == cexclusion_index.size());
  for (catalog::index_oid_t idx : cexclusion_index) {
    assert(exclusion_table_index_set.count(idx) == 1);
    assert(exclusion_visited.count(idx) == 0);
    exclusion_visited.insert(idx);
  }
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  return 0;
}
}