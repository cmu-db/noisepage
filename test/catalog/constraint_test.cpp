#include "catalog/catalog.h"
#include <stdio.h>
#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "gtest/gtest.h"
#include "test_util/test_harness.h"
#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "catalog/postgres/pg_namespace.h"
#include "main/db_main.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "storage/index/index_builder.h"
#include "storage/sql_table.h"
#include "test_util/test_harness.h"
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

struct ConstraintTests : public TerrierTest {
  void SetUp() override {
    db_main_ = terrier::DBMain::Builder().SetUseGC(true).SetUseCatalog(true).Build();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
    auto *txn = txn_manager_->BeginTransaction();
    db_ = catalog_->GetDatabaseOid(common::ManagedPointer(txn), catalog::DEFAULT_DATABASE);
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  void VerifyCatalogTables(const catalog::CatalogAccessor &accessor) {
    auto ns_oid = accessor.GetNamespaceOid("pg_catalog");
    EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);
    EXPECT_EQ(ns_oid, catalog::postgres::NAMESPACE_CATALOG_NAMESPACE_OID);

    VerifyTablePresent(accessor, ns_oid, "pg_attribute");
    VerifyTablePresent(accessor, ns_oid, "pg_class");
    VerifyTablePresent(accessor, ns_oid, "pg_constraint");
    VerifyTablePresent(accessor, ns_oid, "fk_constraint");
    VerifyTablePresent(accessor, ns_oid, "check_constraint");
    VerifyTablePresent(accessor, ns_oid, "exclusion_constraint");
    VerifyTablePresent(accessor, ns_oid, "pg_index");
    VerifyTablePresent(accessor, ns_oid, "pg_namespace");
    VerifyTablePresent(accessor, ns_oid, "pg_type");
    VerifyTablePresent(accessor, ns_oid, "pg_language");
    VerifyTablePresent(accessor, ns_oid, "pg_proc");
  }

  void VerifyTablePresent(const catalog::CatalogAccessor &accessor, catalog::namespace_oid_t ns_oid,
                          const std::string &table_name) {
    auto table_oid = accessor.GetTableOid(ns_oid, table_name);
    EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  }

  void VerifyTableAbsent(const catalog::CatalogAccessor &accessor, catalog::namespace_oid_t ns_oid,
                         const std::string &table_name) {
    auto table_oid = accessor.GetTableOid(ns_oid, table_name);
    EXPECT_EQ(table_oid, catalog::INVALID_TABLE_OID);
  }

  void VerifyIndexPresent(const catalog::CatalogAccessor &accessor, catalog::namespace_oid_t ns_oid,
                          const std::string &table_name) {
    auto table_oid = accessor.GetTableOid(ns_oid, table_name);
    EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  }

  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  catalog::db_oid_t db_;
};

// NOLINTNEXTLINE
/**
 * Verify that the constraint table sand their related inidies are present and valid within 
 * the catalog and catalog accessor
 */
TEST_F(ConstraintTests, ConstraintTableAndIndexPresentTest) {
  std::cerr << "sart\n";
  db_main_ = terrier::DBMain::Builder().SetUseGC(true).SetUseCatalog(true).Build();
  txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
  std::cerr << "sart 2\n";
  catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
  std::cerr << "sart 3\n";
  auto *txn = txn_manager_->BeginTransaction();
  std::cerr << "sart 4\n";
  db_ = catalog_->GetDatabaseOid(common::ManagedPointer(txn), catalog::DEFAULT_DATABASE);
  std::cerr << "pass creation\n";
  auto accessor = *(catalog_->GetAccessor(common::ManagedPointer(txn), db_));
  auto ns_oid = accessor.GetNamespaceOid("pg_catalog");
  EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);
  EXPECT_EQ(ns_oid, catalog::postgres::NAMESPACE_CATALOG_NAMESPACE_OID);

  auto con_table = accessor.GetTableOid(ns_oid, "pg_constraint");
  EXPECT_EQ(con_table, catalog::postgres::CONSTRAINT_TABLE_OID);
  std::unordered_set<catalog::index_oid_t> con_table_index_set;
  con_table_index_set.insert(catalog::postgres::CONSTRAINT_OID_INDEX_OID);
  con_table_index_set.insert(catalog::postgres::CONSTRAINT_NAME_INDEX_OID);
  con_table_index_set.insert(catalog::postgres::CONSTRAINT_NAMESPACE_INDEX_OID);
  con_table_index_set.insert(catalog::postgres::CONSTRAINT_TABLE_INDEX_OID);
  con_table_index_set.insert(catalog::postgres::CONSTRAINT_INDEX_INDEX_OID);

  std::vector<catalog::index_oid_t> idx_table = accessor.GetIndexOids(con_table);
  std::unordered_set<catalog::index_oid_t> con_visited;
  EXPECT_EQ(con_table_index_set.size(), idx_table.size());
  for (catalog::index_oid_t idx : idx_table) {
    EXPECT_TRUE(con_table_index_set.count(idx) == 1);
    EXPECT_TRUE(con_visited.count(idx) == 0);
    con_visited.insert(idx);
  }
  std::cerr << "pass con \n";
  auto fk_table = accessor.GetTableOid(ns_oid, "fk_constraint");
  EXPECT_EQ(fk_table, catalog::postgres::FK_TABLE_OID);
  std::unordered_set<catalog::index_oid_t> fk_table_index_set;
  fk_table_index_set.insert(catalog::postgres::FK_OID_INDEX_OID);
  fk_table_index_set.insert(catalog::postgres::FK_CON_OID_INDEX_OID);
  fk_table_index_set.insert(catalog::postgres::FK_SRC_TABLE_OID_INDEX_OID);
  fk_table_index_set.insert(catalog::postgres::FK_REF_TABLE_OID_INDEX_OID);

  std::vector<catalog::index_oid_t> fk_index = accessor.GetIndexOids(fk_table);
  std::unordered_set<catalog::index_oid_t> fk_visited;
  EXPECT_EQ(fk_table_index_set.size(), fk_index.size());
  for (catalog::index_oid_t idx : fk_index) {
    EXPECT_TRUE(fk_table_index_set.count(idx) == 1);
    EXPECT_TRUE(fk_visited.count(idx) == 0);
    fk_visited.insert(idx);
  }
  std::cerr << "pass fk\n";
  auto check_table = accessor.GetTableOid(ns_oid, "check_constraint");
  EXPECT_EQ(check_table, catalog::postgres::CHECK_TABLE_OID);
  std::unordered_set<catalog::index_oid_t> check_table_index_set;
  check_table_index_set.insert(catalog::postgres::CHECK_OID_INDEX_OID);

  std::vector<catalog::index_oid_t> check_index = accessor.GetIndexOids(check_table);
  std::unordered_set<catalog::index_oid_t> check_visited;
  EXPECT_EQ(check_table_index_set.size(), check_index.size());
  for (catalog::index_oid_t idx : check_index) {
    EXPECT_TRUE(check_table_index_set.count(idx) == 1);
    EXPECT_TRUE(check_visited.count(idx) == 0);
    check_visited.insert(idx);
  }
  std::cerr << "pass check\n";
  auto exclusion_table = accessor.GetTableOid(ns_oid, "exclusion_constraint");
  EXPECT_EQ(exclusion_table, catalog::postgres::EXCLUSION_TABLE_OID);
  std::unordered_set<catalog::index_oid_t> exclusion_table_index_set;
  exclusion_table_index_set.insert(catalog::postgres::EXCLUSION_OID_INDEX_OID);

  std::vector<catalog::index_oid_t> cexclusion_index = accessor.GetIndexOids(exclusion_table);
  std::unordered_set<catalog::index_oid_t> exclusion_visited;
  EXPECT_EQ(exclusion_table_index_set.size(), cexclusion_index.size());
  for (catalog::index_oid_t idx : cexclusion_index) {
    EXPECT_TRUE(exclusion_table_index_set.count(idx) == 1);
    EXPECT_TRUE(exclusion_visited.count(idx) == 0);
    exclusion_visited.insert(idx);
  }
  std::cerr << "pass exclusion\n";
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
/**
 * Follow the same procedure to create things and verify that they present 
 * This include constraint info, table and related indicies
 */
TEST_F(ConstraintTests, DDLCreateTableProcedureTest) {
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);

  // Create the column definition (no OIDs)
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false,
                    parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
  cols.emplace_back("user_col_1", type::TypeId::INTEGER, false,
                    parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
  auto tmp_schema = catalog::Schema(cols);

  auto table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "test_table", tmp_schema);
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  VerifyTablePresent(*accessor, accessor->GetDefaultNamespace(), "test_table");
  // Check lookup via search path
  EXPECT_EQ(table_oid, accessor->GetTableOid("test_table"));
  EXPECT_EQ(accessor->GetTable(table_oid), nullptr);  // Check that allocation has not happened
  auto schema = accessor->GetSchema(table_oid);

  // Verify our columns exist
  EXPECT_NE(schema.GetColumn("id").Oid(), catalog::INVALID_COLUMN_OID);
  EXPECT_NE(schema.GetColumn("user_col_1").Oid(), catalog::INVALID_COLUMN_OID);

  // Verify we can instantiate a storage object with the generated schema
  auto table = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema);

  EXPECT_TRUE(accessor->SetTablePointer(table_oid, table));
  EXPECT_EQ(common::ManagedPointer(table), accessor->GetTable(table_oid));

  std::vector<catalog::col_oid_t> pk_cols;
  pk_cols.push_back(schema.GetColumn("id").Oid());

  std::vector<catalog::IndexSchema::Column> key_cols;
  key_cols.emplace_back("id", type::TypeId::INTEGER, false,
                    parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
  catalog::IndexSchema index_schema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);
  const auto index_oid = accessor->CreateIndex(accessor->GetDefaultNamespace(), table_oid, "pk", index_schema);
  EXPECT_NE (index_oid, catalog::INVALID_INDEX_OID);
  // Get the canonical IndexSchema from the Catalog now that column oids have been assigned
  const auto &schemaa = accessor->GetIndexSchema(index_oid);
  // Instantiate an Index and update the pointer in the Catalog
  storage::index::IndexBuilder index_builder;
  index_builder.SetKeySchema(schemaa);
  auto *const index = index_builder.Build();
  bool result UNUSED_ATTRIBUTE = accessor->SetIndexPointer(index_oid, index);

  accessor->CreatePKConstraint(accessor->GetDefaultNamespace(), table_oid, "test_pk", index_oid, pk_cols);
  EXPECT_NE(accessor->GetConstraints(table_oid)[0], catalog::INVALID_CONSTRAINT_OID);
  std::cerr << "pre commit\n";
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  std::cerr << "post commit\n";
  // Get an accessor into the database and validate the catalog tables exist
  // then delete it and verify an invalid OID is now returned for the lookup
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);
  EXPECT_NE(accessor, nullptr);
  std::cerr << "pre check table\n";
  VerifyTablePresent(*accessor, accessor->GetDefaultNamespace(), "test_table");
  std::cerr << "post check table\n";
  EXPECT_TRUE(accessor->DropTable(table_oid));
  std::cerr << "post drop table\n";
  VerifyTableAbsent(*accessor, accessor->GetDefaultNamespace(), "test_table");
  std::cerr << "post cerify drop table\n";
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
/**
 * 
 */
TEST_F(ConstraintTests, DDLCreateUniqueConstraintProcedureTest) {

}


// NOLINTNEXTLINE
/**
 * Verify that the constraint table sand their related inidies are present and valid within 
 * the catalog and catalog accessor
 */
TEST_F(ConstraintTests, ConstraintDeletionTest) {

}


// NOLINTNEXTLINE
/**
 * Verify that the constraint table sand their related inidies are present and valid within 
 * the catalog and catalog accessor
 */
TEST_F(ConstraintTests, ConstraintDeletionCascadeTest) {

}

// NOLINTNEXTLINE
/**
 * Verify that the constraint table sand their related inidies are present and valid within 
 * the catalog and catalog accessor
 */
TEST_F(ConstraintTests, GetConstraintTest) {

}


// NOLINTNEXTLINE
/**
 * Verify that the constraint table sand their related inidies are present and valid within 
 * the catalog and catalog accessor
 */
TEST_F(ConstraintTests, TableDeletionConstaintCascadeTest) {

}

// NOLINTNEXTLINE
/**
 * Verify that the constraint table sand their related inidies are present and valid within 
 * the catalog and catalog accessor
 */
TEST_F(ConstraintTests, EnforceUniqueTest) {

}

/*
 * Create and read UNIQUE constraint
 * test_table
 * id(PK), col_1 (UNIQUE)
 */
// NOLINTNEXTLINE
// TEST_F(CatalogTests, SingleUniqueConstraintTest) {
//   auto txn = txn_manager_->BeginTransaction();
//   auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);

//   // Create the column definition (no OIDs)
//   planner::CreateTablePlanNode 



//   // Create the index
//   std::vector<catalog::IndexSchema::Column> key_cols{catalog::IndexSchema::Column{
//       "id", type::TypeId::INTEGER, false, parser::ColumnValueExpression(db_, table_oid, schema.GetColumn("id").Oid())}};
//   auto index_schema = catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);
//   auto idx_oid = accessor->CreateIndex(accessor->GetDefaultNamespace(), table_oid,
//                                        "test_table_index_mabobberwithareallylongnamethatstillneedsmore", index_schema);
//   EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
//   auto true_schema = accessor->GetIndexSchema(idx_oid);

//   storage::index::IndexBuilder index_builder;
//   index_builder.SetKeySchema(true_schema);
//   auto index = index_builder.Build();

//   EXPECT_TRUE(accessor->SetIndexPointer(idx_oid, index));
//   EXPECT_EQ(common::ManagedPointer(index), accessor->GetIndex(idx_oid));
//   txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

//   // Get an accessor into the database and validate the catalog tables exist
//   // then delete it and verify an invalid OID is now returned for the lookup
//   txn = txn_manager_->BeginTransaction();
//   accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);
//   EXPECT_NE(accessor, nullptr);
//   idx_oid = accessor->GetIndexOid("test_table_index_mabobberwithareallylongnamethatstillneedsmore");
//   EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
//   EXPECT_TRUE(accessor->DropIndex(idx_oid));
//   idx_oid = accessor->GetIndexOid("test_table_index_mabobberwithareallylongnamethatstillneedsmore");
//   EXPECT_EQ(idx_oid, catalog::INVALID_INDEX_OID);
//   txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
// }

/*
 * Create a user table and index. Drop them both by dropping the table using cascading drop logic.
 */
// NOLINTNEXTLINE
// TEST_F(CatalogTests, CascadingDropTableTest) {
//   auto txn = txn_manager_->BeginTransaction();
//   auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);
//   EXPECT_NE(accessor, nullptr);

//   // Create the column definition (no OIDs)
//   std::vector<catalog::Schema::Column> cols;
//   cols.emplace_back("id", type::TypeId::INTEGER, false,
//                     parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
//   cols.emplace_back("user_col_1", type::TypeId::INTEGER, false,
//                     parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
//   auto tmp_schema = catalog::Schema(cols);

//   auto table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "test_table", tmp_schema);
//   auto schema = accessor->GetSchema(table_oid);
//   auto table = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema);

//   EXPECT_TRUE(accessor->SetTablePointer(table_oid, table));
//   txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

//   // Create the index
//   txn = txn_manager_->BeginTransaction();
//   accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);
//   EXPECT_NE(accessor, nullptr);
//   std::vector<catalog::IndexSchema::Column> key_cols{catalog::IndexSchema::Column{
//       "id", type::TypeId::INTEGER, false, parser::ColumnValueExpression(db_, table_oid, schema.GetColumn("id").Oid())}};
//   auto index_schema = catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);
//   auto idx_oid = accessor->CreateIndex(accessor->GetDefaultNamespace(), table_oid, "test_index", index_schema);
//   EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
//   auto true_schema = accessor->GetIndexSchema(idx_oid);

//   storage::index::IndexBuilder index_builder;
//   index_builder.SetKeySchema(true_schema);
//   auto index = index_builder.Build();

//   EXPECT_TRUE(accessor->SetIndexPointer(idx_oid, index));
//   EXPECT_EQ(common::ManagedPointer(index), accessor->GetIndex(idx_oid));
//   txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

//   // Get an accessor into the database and validate the catalog tables exist
//   // then delete it and verify an invalid OID is now returned for the lookup
//   txn = txn_manager_->BeginTransaction();
//   accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);
//   EXPECT_NE(accessor, nullptr);

//   VerifyTablePresent(*accessor, accessor->GetDefaultNamespace(), "test_table");
//   idx_oid = accessor->GetIndexOid("test_index");
//   EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
//   EXPECT_TRUE(accessor->DropTable(table_oid));
//   VerifyTableAbsent(*accessor, accessor->GetDefaultNamespace(), "test_table");
//   idx_oid = accessor->GetIndexOid("test_index");
//   EXPECT_EQ(idx_oid, catalog::INVALID_INDEX_OID);
//   txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
// }

// /*
//  * Create a user table and index. Drop them all by dropping the namespace using cascading drop logic.
//  */
// // NOLINTNEXTLINE
// TEST_F(CatalogTests, CascadingDropNamespaceTest) {
//   auto txn = txn_manager_->BeginTransaction();
//   auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);
//   EXPECT_NE(accessor, nullptr);
//   auto ns_oid = accessor->CreateNamespace("test_namespace");
//   EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);
//   VerifyCatalogTables(*accessor);  // Check visibility to me
//   txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

//   // Create the column definition (no OIDs)
//   std::vector<catalog::Schema::Column> cols;
//   cols.emplace_back("id", type::TypeId::INTEGER, false,
//                     parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
//   cols.emplace_back("user_col_1", type::TypeId::INTEGER, false,
//                     parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
//   auto tmp_schema = catalog::Schema(cols);

//   txn = txn_manager_->BeginTransaction();
//   accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);
//   EXPECT_NE(accessor, nullptr);
//   auto table_oid = accessor->CreateTable(ns_oid, "test_table", tmp_schema);
//   auto schema = accessor->GetSchema(table_oid);
//   auto table = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema);

//   EXPECT_TRUE(accessor->SetTablePointer(table_oid, table));
//   txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

//   // Create the index
//   txn = txn_manager_->BeginTransaction();
//   accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);
//   EXPECT_NE(accessor, nullptr);
//   std::vector<catalog::IndexSchema::Column> key_cols{catalog::IndexSchema::Column{
//       "id", type::TypeId::INTEGER, false, parser::ColumnValueExpression(db_, table_oid, schema.GetColumn("id").Oid())}};
//   auto index_schema = catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);
//   auto idx_oid = accessor->CreateIndex(ns_oid, table_oid, "test_index", index_schema);
//   EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
//   auto true_schema = accessor->GetIndexSchema(idx_oid);

//   storage::index::IndexBuilder index_builder;
//   index_builder.SetKeySchema(true_schema);
//   auto index = index_builder.Build();

//   EXPECT_TRUE(accessor->SetIndexPointer(idx_oid, index));
//   EXPECT_EQ(common::ManagedPointer(index), accessor->GetIndex(idx_oid));
//   txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

//   // Get an accessor into the database and validate the catalog tables exist
//   // then delete it and verify an invalid OID is now returned for the lookup
//   txn = txn_manager_->BeginTransaction();
//   accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);
//   EXPECT_NE(accessor, nullptr);

//   VerifyTablePresent(*accessor, ns_oid, "test_table");
//   idx_oid = accessor->GetIndexOid(ns_oid, "test_index");
//   EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
//   EXPECT_TRUE(accessor->DropNamespace(ns_oid));
//   VerifyTableAbsent(*accessor, ns_oid, "test_table");
//   idx_oid = accessor->GetIndexOid(ns_oid, "test_index");
//   EXPECT_EQ(idx_oid, catalog::INVALID_INDEX_OID);
//   txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
// }

// // NOLINTNEXTLINE
// TEST_F(CatalogTests, GetIndexesTest) {
//   auto txn = txn_manager_->BeginTransaction();
//   auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);

//   // Create the column definition (no OIDs)
//   std::vector<catalog::Schema::Column> cols;
//   cols.emplace_back("id", type::TypeId::INTEGER, false,
//                     parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
//   auto tmp_schema = catalog::Schema(cols);

//   auto table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "test_table", tmp_schema);
//   auto schema = accessor->GetSchema(table_oid);
//   auto table = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema);
//   EXPECT_TRUE(accessor->SetTablePointer(table_oid, table));

//   // Create the index
//   std::vector<catalog::IndexSchema::Column> key_cols{catalog::IndexSchema::Column{
//       "id", type::TypeId::INTEGER, false, parser::ColumnValueExpression(db_, table_oid, schema.GetColumn("id").Oid())}};
//   auto index_schema = catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);
//   auto idx_oid = accessor->CreateIndex(accessor->GetDefaultNamespace(), table_oid, "test_table_idx", index_schema);
//   EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
//   auto true_schema = accessor->GetIndexSchema(idx_oid);

//   storage::index::IndexBuilder index_builder;
//   index_builder.SetKeySchema(true_schema);
//   auto index = index_builder.Build();

//   EXPECT_TRUE(accessor->SetIndexPointer(idx_oid, index));
//   EXPECT_EQ(common::ManagedPointer(index), accessor->GetIndex(idx_oid));
//   txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

//   // Get an accessor into the database
//   txn = txn_manager_->BeginTransaction();
//   accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);
//   EXPECT_NE(accessor, nullptr);

//   // Check that GetIndexes returns the indexes
//   auto idx_oids = accessor->GetIndexOids(table_oid);
//   EXPECT_EQ(idx_oids.size(), 1);
//   EXPECT_EQ(idx_oids[0], idx_oid);
//   txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
// }

// // NOLINTNEXTLINE
// TEST_F(CatalogTests, GetIndexObjectsTest) {
//   constexpr auto num_indexes = 3;
//   auto txn = txn_manager_->BeginTransaction();
//   auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);

//   // Create the column definition (no OIDs)
//   std::vector<catalog::Schema::Column> cols;
//   cols.emplace_back("id", type::TypeId::INTEGER, false,
//                     parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
//   auto tmp_schema = catalog::Schema(cols);

//   auto table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "test_table", tmp_schema);
//   auto schema = accessor->GetSchema(table_oid);
//   auto table = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema);
//   EXPECT_TRUE(accessor->SetTablePointer(table_oid, table));

//   // Create the a couple of index
//   std::vector<catalog::index_oid_t> index_oids;
//   for (auto i = 0; i < num_indexes; i++) {
//     std::vector<catalog::IndexSchema::Column> key_cols{
//         catalog::IndexSchema::Column{"id", type::TypeId::INTEGER, false,
//                                      parser::ColumnValueExpression(db_, table_oid, schema.GetColumn("id").Oid())}};
//     auto index_schema = catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);
//     auto idx_oid = accessor->CreateIndex(accessor->GetDefaultNamespace(), table_oid,
//                                          "test_table_idx" + std::to_string(i), index_schema);
//     EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
//     index_oids.push_back(idx_oid);
//     const auto &true_schema = accessor->GetIndexSchema(idx_oid);

//     storage::index::IndexBuilder index_builder;
//     index_builder.SetKeySchema(true_schema);
//     auto index = index_builder.Build();

//     EXPECT_TRUE(accessor->SetIndexPointer(idx_oid, index));
//     EXPECT_EQ(common::ManagedPointer(index), accessor->GetIndex(idx_oid));
//   }
//   txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

//   // Get an accessor into the database
//   txn = txn_manager_->BeginTransaction();
//   accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);
//   EXPECT_NE(accessor, nullptr);

//   // Check that GetIndexes returns the indexes correct number of indexes
//   auto idx_oids = accessor->GetIndexOids(table_oid);
//   EXPECT_EQ(num_indexes, idx_oids.size());

//   // Fetch all objects with a single call, check that sets are equal
//   auto index_objects = accessor->GetIndexes(table_oid);
//   EXPECT_EQ(num_indexes, index_objects.size());
//   for (const auto &object_pair : index_objects) {
//     EXPECT_TRUE(object_pair.first);
//     EXPECT_EQ(1, object_pair.second.GetColumns().size());
//     EXPECT_EQ("id", object_pair.second.GetColumn(0).Name());
//   }

//   txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
// }

}  // namespace terrier
