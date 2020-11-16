#include "catalog/catalog.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "catalog/database_catalog.h"
#include "catalog/postgres/pg_namespace.h"
#include "execution/functions/function_context.h"
#include "main/db_main.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "storage/index/index_builder.h"
#include "storage/sql_table.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace noisepage {

struct CatalogTests : public TerrierTest {
  void SetUp() override {
    db_main_ = noisepage::DBMain::Builder().SetUseGC(true).SetUseCatalog(true).Build();
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

  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  catalog::db_oid_t db_;
};

TEST_F(CatalogTests, LanguageTest) {
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // Check visibility to me
  VerifyCatalogTables(*accessor);

  // make sure "internal" is bootstrapped
  auto oid = accessor->CreateLanguage("internal");
  EXPECT_EQ(oid, catalog::INVALID_LANGUAGE_OID);

  txn_manager_->Abort(txn);
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // add custom language
  oid = accessor->CreateLanguage("test_language");
  EXPECT_NE(oid, catalog::INVALID_LANGUAGE_OID);

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto good_oid = oid;

  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // make sure we can't add the same language again
  oid = accessor->CreateLanguage("test_language");
  EXPECT_EQ(oid, catalog::INVALID_LANGUAGE_OID);
  txn_manager_->Abort(txn);

  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // make sure we get the same language oid back for the custom language we created
  oid = accessor->GetLanguageOid("test_language");
  EXPECT_EQ(oid, good_oid);
  auto result = accessor->DropLanguage(good_oid);
  EXPECT_TRUE(result);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // drop the language we made
  result = accessor->DropLanguage(good_oid);
  EXPECT_FALSE(result);
  txn_manager_->Abort(txn);
}

TEST_F(CatalogTests, ProcTest) {
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // Check visibility to me
  VerifyCatalogTables(*accessor);

  auto lan_oid = accessor->CreateLanguage("test_language");
  auto ns_oid = accessor->GetDefaultNamespace();

  EXPECT_NE(lan_oid, catalog::INVALID_LANGUAGE_OID);

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // create a sample proc
  auto procname = "sample";
  std::vector<std::string> args = {"arg1", "arg2", "arg3"};
  std::vector<catalog::type_oid_t> arg_types = {accessor->GetTypeOidFromTypeId(type::TypeId::INTEGER),
                                                accessor->GetTypeOidFromTypeId(type::TypeId::BOOLEAN),
                                                accessor->GetTypeOidFromTypeId(type::TypeId::SMALLINT)};
  std::vector<catalog::postgres::ProArgModes> arg_modes = {
      catalog::postgres::ProArgModes::IN, catalog::postgres::ProArgModes::IN, catalog::postgres::ProArgModes::IN};
  auto src = "int sample(arg1, arg2, arg3){return 2;}";

  auto proc_oid =
      accessor->CreateProcedure(procname, lan_oid, ns_oid, args, arg_types, arg_types, arg_modes,
                                catalog::type_oid_t(static_cast<uint8_t>(type::TypeId::INTEGER)), src, false);
  EXPECT_NE(proc_oid, catalog::INVALID_PROC_OID);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // make sure we didn't find this proc that we never added
  auto found_oid = accessor->GetProcOid("bad_proc", arg_types);
  EXPECT_EQ(found_oid, catalog::INVALID_PROC_OID);

  // look for proc that we actually added
  found_oid = accessor->GetProcOid(procname, arg_types);

  auto sin_oid = accessor->GetProcOid("sin", {accessor->GetTypeOidFromTypeId(type::TypeId::DECIMAL)});
  EXPECT_NE(sin_oid, catalog::INVALID_PROC_OID);

  auto sin_context = accessor->GetProcCtxPtr(sin_oid);
  EXPECT_TRUE(sin_context->IsBuiltin());
  EXPECT_EQ(sin_context->GetBuiltin(), execution::ast::Builtin::Sin);
  EXPECT_EQ(sin_context->GetFunctionReturnType(), type::TypeId::DECIMAL);
  auto sin_args = sin_context->GetFunctionArgsType();
  EXPECT_EQ(sin_args.size(), 1);
  EXPECT_EQ(sin_args.back(), type::TypeId::DECIMAL);
  EXPECT_EQ(sin_context->GetFunctionName(), "sin");

  EXPECT_EQ(found_oid, proc_oid);
  auto result = accessor->DropProcedure(found_oid);
  EXPECT_TRUE(result);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

/*
 * Create and delete a database
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, DatabaseTest) {
  // Create a database and check that it's immediately visible
  auto txn = txn_manager_->BeginTransaction();
  auto db_oid = catalog_->CreateDatabase(common::ManagedPointer(txn), "test_database", true);
  EXPECT_NE(db_oid, catalog::INVALID_DATABASE_OID);
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
  EXPECT_NE(accessor, nullptr);
  VerifyCatalogTables(*accessor);  // Check visibility to me
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Cannot add a database twice
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
  auto tmp_oid = accessor->CreateDatabase("test_database");
  EXPECT_EQ(tmp_oid, catalog::INVALID_DATABASE_OID);  // Should cause a name conflict
  txn_manager_->Abort(txn);

  // Get an accessor into the database and validate the catalog tables exist
  // then delete it and verify an invalid OID is now returned for the lookup
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
  EXPECT_NE(accessor, nullptr);
  VerifyCatalogTables(*accessor);  // Check visibility to me
  tmp_oid = accessor->GetDatabaseOid("test_database");
  EXPECT_TRUE(accessor->DropDatabase(tmp_oid));
  tmp_oid = accessor->GetDatabaseOid("test_database");
  EXPECT_EQ(tmp_oid, catalog::INVALID_DATABASE_OID);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Cannot get an accessor to a non-existent database
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
  EXPECT_EQ(accessor, nullptr);
  txn_manager_->Abort(txn);
}

/*
 * Create and delete a namespace
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, NamespaceTest) {
  // Create a database and check that it's immediately visible
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_NE(accessor, nullptr);
  auto ns_oid = accessor->CreateNamespace("test_namespace");
  EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);
  VerifyCatalogTables(*accessor);  // Check visibility to me
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  ns_oid = accessor->CreateNamespace("test_namespace");
  EXPECT_EQ(ns_oid, catalog::INVALID_NAMESPACE_OID);  // Should cause a name conflict
  txn_manager_->Abort(txn);

  // Get an accessor into the database and validate the catalog tables exist
  // then delete it and verify an invalid OID is now returned for the lookup
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_NE(accessor, nullptr);
  VerifyCatalogTables(*accessor);  // Check visibility to me
  ns_oid = accessor->GetNamespaceOid("test_namespace");
  EXPECT_TRUE(accessor->DropNamespace(ns_oid));
  ns_oid = accessor->GetNamespaceOid("test_namespace");
  EXPECT_EQ(ns_oid, catalog::INVALID_NAMESPACE_OID);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  ns_oid = accessor->GetNamespaceOid("test_namespace");
  EXPECT_EQ(ns_oid, catalog::INVALID_NAMESPACE_OID);
  txn_manager_->Abort(txn);
}

/*
 * Create and delete a user table.
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, UserTableTest) {
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // Create the column definition (no OIDs)
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  cols.emplace_back("user_col_1", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
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
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Get an accessor into the database and validate the catalog tables exist
  // then delete it and verify an invalid OID is now returned for the lookup
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_NE(accessor, nullptr);

  VerifyTablePresent(*accessor, accessor->GetDefaultNamespace(), "test_table");
  EXPECT_TRUE(accessor->DropTable(table_oid));
  VerifyTableAbsent(*accessor, accessor->GetDefaultNamespace(), "test_table");
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

/*
 * Create and delete a user index.
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, UserIndexTest) {
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // Create the column definition (no OIDs)
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  cols.emplace_back("user_col_1", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  auto tmp_schema = catalog::Schema(cols);

  auto table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "test_table", tmp_schema);
  auto schema = accessor->GetSchema(table_oid);
  auto table = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema);

  EXPECT_TRUE(accessor->SetTablePointer(table_oid, table));

  // Create the index
  std::vector<catalog::IndexSchema::Column> key_cols{catalog::IndexSchema::Column{
      "id", type::TypeId::INTEGER, false, parser::ColumnValueExpression(db_, table_oid, schema.GetColumn("id").Oid())}};
  auto index_schema = catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);
  auto idx_oid = accessor->CreateIndex(accessor->GetDefaultNamespace(), table_oid,
                                       "test_table_index_mabobberwithareallylongnamethatstillneedsmore", index_schema);
  EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
  auto true_schema = accessor->GetIndexSchema(idx_oid);

  storage::index::IndexBuilder index_builder;
  index_builder.SetKeySchema(true_schema);
  auto index = index_builder.Build();

  EXPECT_TRUE(accessor->SetIndexPointer(idx_oid, index));
  EXPECT_EQ(common::ManagedPointer(index), accessor->GetIndex(idx_oid));
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Get an accessor into the database and validate the catalog tables exist
  // then delete it and verify an invalid OID is now returned for the lookup
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_NE(accessor, nullptr);
  idx_oid = accessor->GetIndexOid("test_table_index_mabobberwithareallylongnamethatstillneedsmore");
  EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
  EXPECT_TRUE(accessor->DropIndex(idx_oid));
  idx_oid = accessor->GetIndexOid("test_table_index_mabobberwithareallylongnamethatstillneedsmore");
  EXPECT_EQ(idx_oid, catalog::INVALID_INDEX_OID);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

/*
 * Create a user table and index. Drop them both by dropping the table using cascading drop logic.
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, CascadingDropTableTest) {
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_NE(accessor, nullptr);

  // Create the column definition (no OIDs)
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  cols.emplace_back("user_col_1", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  auto tmp_schema = catalog::Schema(cols);

  auto table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "test_table", tmp_schema);
  auto schema = accessor->GetSchema(table_oid);
  auto table = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema);

  EXPECT_TRUE(accessor->SetTablePointer(table_oid, table));
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Create the index
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_NE(accessor, nullptr);
  std::vector<catalog::IndexSchema::Column> key_cols{catalog::IndexSchema::Column{
      "id", type::TypeId::INTEGER, false, parser::ColumnValueExpression(db_, table_oid, schema.GetColumn("id").Oid())}};
  auto index_schema = catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);
  auto idx_oid = accessor->CreateIndex(accessor->GetDefaultNamespace(), table_oid, "test_index", index_schema);
  EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
  auto true_schema = accessor->GetIndexSchema(idx_oid);

  storage::index::IndexBuilder index_builder;
  index_builder.SetKeySchema(true_schema);
  auto index = index_builder.Build();

  EXPECT_TRUE(accessor->SetIndexPointer(idx_oid, index));
  EXPECT_EQ(common::ManagedPointer(index), accessor->GetIndex(idx_oid));
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Get an accessor into the database and validate the catalog tables exist
  // then delete it and verify an invalid OID is now returned for the lookup
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_NE(accessor, nullptr);

  VerifyTablePresent(*accessor, accessor->GetDefaultNamespace(), "test_table");
  idx_oid = accessor->GetIndexOid("test_index");
  EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
  EXPECT_TRUE(accessor->DropTable(table_oid));
  VerifyTableAbsent(*accessor, accessor->GetDefaultNamespace(), "test_table");
  idx_oid = accessor->GetIndexOid("test_index");
  EXPECT_EQ(idx_oid, catalog::INVALID_INDEX_OID);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

/*
 * Create a user table and index. Drop them all by dropping the namespace using cascading drop logic.
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, CascadingDropNamespaceTest) {
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_NE(accessor, nullptr);
  auto ns_oid = accessor->CreateNamespace("test_namespace");
  EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);
  VerifyCatalogTables(*accessor);  // Check visibility to me
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Create the column definition (no OIDs)
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  cols.emplace_back("user_col_1", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  auto tmp_schema = catalog::Schema(cols);

  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_NE(accessor, nullptr);
  auto table_oid = accessor->CreateTable(ns_oid, "test_table", tmp_schema);
  auto schema = accessor->GetSchema(table_oid);
  auto table = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema);

  EXPECT_TRUE(accessor->SetTablePointer(table_oid, table));
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Create the index
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_NE(accessor, nullptr);
  std::vector<catalog::IndexSchema::Column> key_cols{catalog::IndexSchema::Column{
      "id", type::TypeId::INTEGER, false, parser::ColumnValueExpression(db_, table_oid, schema.GetColumn("id").Oid())}};
  auto index_schema = catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);
  auto idx_oid = accessor->CreateIndex(ns_oid, table_oid, "test_index", index_schema);
  EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
  auto true_schema = accessor->GetIndexSchema(idx_oid);

  storage::index::IndexBuilder index_builder;
  index_builder.SetKeySchema(true_schema);
  auto index = index_builder.Build();

  EXPECT_TRUE(accessor->SetIndexPointer(idx_oid, index));
  EXPECT_EQ(common::ManagedPointer(index), accessor->GetIndex(idx_oid));
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Get an accessor into the database and validate the catalog tables exist
  // then delete it and verify an invalid OID is now returned for the lookup
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_NE(accessor, nullptr);

  VerifyTablePresent(*accessor, ns_oid, "test_table");
  idx_oid = accessor->GetIndexOid(ns_oid, "test_index");
  EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
  EXPECT_TRUE(accessor->DropNamespace(ns_oid));
  VerifyTableAbsent(*accessor, ns_oid, "test_table");
  idx_oid = accessor->GetIndexOid(ns_oid, "test_index");
  EXPECT_EQ(idx_oid, catalog::INVALID_INDEX_OID);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

/*
 * Create a user table in default namespace and an index on that table in a user namespace. Verify that index gets
 * dropped correctly when dropping the user namespace, but the table remains.
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, CascadingDropNamespaceWithIndexOnOtherNamespaceTest) {
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_NE(accessor, nullptr);
  auto ns_oid = accessor->CreateNamespace("test_namespace");
  EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);
  VerifyCatalogTables(*accessor);  // Check visibility to me
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Create the column definition (no OIDs)
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  cols.emplace_back("user_col_1", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  auto tmp_schema = catalog::Schema(cols);

  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_NE(accessor, nullptr);
  auto table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "test_table", tmp_schema);
  auto schema = accessor->GetSchema(table_oid);
  auto table = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema);

  EXPECT_TRUE(accessor->SetTablePointer(table_oid, table));
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Create the index
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_NE(accessor, nullptr);
  std::vector<catalog::IndexSchema::Column> key_cols{catalog::IndexSchema::Column{
      "id", type::TypeId::INTEGER, false, parser::ColumnValueExpression(db_, table_oid, schema.GetColumn("id").Oid())}};
  auto index_schema = catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);
  auto idx_oid = accessor->CreateIndex(ns_oid, table_oid, "test_index", index_schema);
  EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
  auto true_schema = accessor->GetIndexSchema(idx_oid);

  storage::index::IndexBuilder index_builder;
  index_builder.SetKeySchema(true_schema);
  auto index = index_builder.Build();

  EXPECT_TRUE(accessor->SetIndexPointer(idx_oid, index));
  EXPECT_EQ(common::ManagedPointer(index), accessor->GetIndex(idx_oid));
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Get an accessor into the database and validate the catalog tables exist
  // then delete it and verify an invalid OID is now returned for the lookup
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_NE(accessor, nullptr);

  // Table is in default namespace, index is in user namespace
  VerifyTablePresent(*accessor, accessor->GetDefaultNamespace(), "test_table");
  idx_oid = accessor->GetIndexOid(ns_oid, "test_index");
  EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);

  // Table is not in user namespace (obvious), index is not in user namespace
  VerifyTableAbsent(*accessor, ns_oid, "test_table");
  idx_oid = accessor->GetIndexOid(accessor->GetDefaultNamespace(), "test_index");
  EXPECT_EQ(idx_oid, catalog::INVALID_INDEX_OID);

  EXPECT_TRUE(accessor->DropNamespace(ns_oid));

  // Table is in default namespace, index is not in user namespace
  VerifyTablePresent(*accessor, accessor->GetDefaultNamespace(), "test_table");
  idx_oid = accessor->GetIndexOid(ns_oid, "test_index");
  EXPECT_EQ(idx_oid, catalog::INVALID_INDEX_OID);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

/*
 * Check behavior of search path
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, UserSearchPathTest) {
  // Create a database and check that it's immediately visible
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  auto public_ns_oid = accessor->GetNamespaceOid("public");
  EXPECT_NE(public_ns_oid, catalog::INVALID_NAMESPACE_OID);
  EXPECT_EQ(public_ns_oid, catalog::postgres::NAMESPACE_DEFAULT_NAMESPACE_OID);
  auto test_ns_oid = accessor->CreateNamespace("test");
  EXPECT_NE(test_ns_oid, catalog::INVALID_NAMESPACE_OID);
  VerifyCatalogTables(*accessor);  // Check visibility to me

  // Create the column definition (no OIDs)
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  cols.emplace_back("user_col_1", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  auto tmp_schema = catalog::Schema(cols);

  // Insert a table into "public"
  auto public_table_oid = accessor->CreateTable(public_ns_oid, "test_table", tmp_schema);
  EXPECT_NE(public_table_oid, catalog::INVALID_TABLE_OID);
  auto schema = accessor->GetSchema(public_table_oid);
  auto table = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema);
  EXPECT_TRUE(accessor->SetTablePointer(public_table_oid, table));

  // Insert a table into "test"
  auto test_table_oid = accessor->CreateTable(test_ns_oid, "test_table", tmp_schema);
  EXPECT_NE(test_table_oid, catalog::INVALID_TABLE_OID);
  schema = accessor->GetSchema(test_table_oid);
  table = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema);
  EXPECT_TRUE(accessor->SetTablePointer(test_table_oid, table));

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Check that it matches the table in the first namespace in path
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  accessor->SetSearchPath({test_ns_oid, public_ns_oid});
  EXPECT_EQ(accessor->GetTableOid("test_table"), test_table_oid);

  accessor->SetSearchPath({public_ns_oid, test_ns_oid});
  EXPECT_EQ(accessor->GetTableOid("test_table"), public_table_oid);

  auto table_oid = accessor->CreateTable(test_ns_oid, "test_table", tmp_schema);
  EXPECT_EQ(table_oid, catalog::INVALID_TABLE_OID);
  table_oid = accessor->CreateTable(test_ns_oid, "test_table", tmp_schema);
  EXPECT_EQ(table_oid, catalog::INVALID_TABLE_OID);
  txn_manager_->Abort(txn);

  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  accessor->DropTable(test_table_oid);

  accessor->SetSearchPath({test_ns_oid, public_ns_oid});
  EXPECT_EQ(accessor->GetTableOid("test_table"), public_table_oid);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

/*
 * Checks specifically whether the implicit searching of pg_catalog works correctly
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, CatalogSearchPathTest) {
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_EQ(accessor->GetTableOid("pg_namespace"), catalog::postgres::NAMESPACE_TABLE_OID);

  // Create the column definition (no OIDs)
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  cols.emplace_back("user_col_1", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  auto tmp_schema = catalog::Schema(cols);

  // Check whether name conflict is inserted into the proper default (first in search path) and masked by implicit
  // addition of 'pg_catalog' at start of search path
  auto user_table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "pg_namespace", tmp_schema);
  EXPECT_EQ(accessor->GetTableOid(catalog::postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, "pg_namespace"), user_table_oid);
  EXPECT_EQ(accessor->GetTableOid("pg_namespace"), catalog::postgres::NAMESPACE_TABLE_OID);

  // Explicitly set 'pg_catalog' as second in the search path and check proper searching
  accessor->SetSearchPath(
      {catalog::postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, catalog::postgres::NAMESPACE_CATALOG_NAMESPACE_OID});
  EXPECT_EQ(accessor->GetTableOid("pg_namespace"), user_table_oid);
  EXPECT_EQ(accessor->GetTableOid(catalog::postgres::NAMESPACE_CATALOG_NAMESPACE_OID, "pg_namespace"),
            catalog::postgres::NAMESPACE_TABLE_OID);

  // Return to implicit declaration to ensure logic works correctly
  accessor->SetSearchPath({catalog::postgres::NAMESPACE_DEFAULT_NAMESPACE_OID});
  EXPECT_EQ(accessor->GetTableOid(catalog::postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, "pg_namespace"), user_table_oid);
  EXPECT_EQ(accessor->GetTableOid("pg_namespace"), catalog::postgres::NAMESPACE_TABLE_OID);

  // Close out
  txn_manager_->Abort(txn);
}

/*
 * Check that the normalize function in CatalogAccessor behaves correctly
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, NameNormalizationTest) {
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  auto ns_oid = accessor->CreateNamespace("TeSt_NaMeSpAcE");
  EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_EQ(catalog::INVALID_NAMESPACE_OID, accessor->CreateNamespace("TEST_NAMESPACE"));  // should conflict
  EXPECT_EQ(ns_oid, accessor->GetNamespaceOid("TEST_NAMESPACE"));                          // Should succeed
  auto dbc = catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), db_);
  EXPECT_EQ(ns_oid,
            dbc->GetNamespaceOid(common::ManagedPointer(txn), "test_namespace"));  // Should match (normalized form)
  EXPECT_EQ(catalog::INVALID_NAMESPACE_OID,
            dbc->GetNamespaceOid(common::ManagedPointer(txn), "TeSt_NaMeSpAcE"));  // Not normalized
  txn_manager_->Abort(txn);
}

// NOLINTNEXTLINE
TEST_F(CatalogTests, GetIndexesTest) {
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // Create the column definition (no OIDs)
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  auto tmp_schema = catalog::Schema(cols);

  auto table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "test_table", tmp_schema);
  auto schema = accessor->GetSchema(table_oid);
  auto table = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema);
  EXPECT_TRUE(accessor->SetTablePointer(table_oid, table));

  // Create the index
  std::vector<catalog::IndexSchema::Column> key_cols{catalog::IndexSchema::Column{
      "id", type::TypeId::INTEGER, false, parser::ColumnValueExpression(db_, table_oid, schema.GetColumn("id").Oid())}};
  auto index_schema = catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);
  auto idx_oid = accessor->CreateIndex(accessor->GetDefaultNamespace(), table_oid, "test_table_idx", index_schema);
  EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
  auto true_schema = accessor->GetIndexSchema(idx_oid);

  storage::index::IndexBuilder index_builder;
  index_builder.SetKeySchema(true_schema);
  auto index = index_builder.Build();

  EXPECT_TRUE(accessor->SetIndexPointer(idx_oid, index));
  EXPECT_EQ(common::ManagedPointer(index), accessor->GetIndex(idx_oid));
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Get an accessor into the database
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_NE(accessor, nullptr);

  // Check that GetIndexes returns the indexes
  auto idx_oids = accessor->GetIndexOids(table_oid);
  EXPECT_EQ(idx_oids.size(), 1);
  EXPECT_EQ(idx_oids[0], idx_oid);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(CatalogTests, GetIndexObjectsTest) {
  constexpr auto num_indexes = 3;
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // Create the column definition (no OIDs)
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  auto tmp_schema = catalog::Schema(cols);

  auto table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "test_table", tmp_schema);
  auto schema = accessor->GetSchema(table_oid);
  auto table = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema);
  EXPECT_TRUE(accessor->SetTablePointer(table_oid, table));

  // Create the a couple of index
  std::vector<catalog::index_oid_t> index_oids;
  for (auto i = 0; i < num_indexes; i++) {
    std::vector<catalog::IndexSchema::Column> key_cols{
        catalog::IndexSchema::Column{"id", type::TypeId::INTEGER, false,
                                     parser::ColumnValueExpression(db_, table_oid, schema.GetColumn("id").Oid())}};
    auto index_schema = catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);
    auto idx_oid = accessor->CreateIndex(accessor->GetDefaultNamespace(), table_oid,
                                         "test_table_idx" + std::to_string(i), index_schema);
    EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
    index_oids.push_back(idx_oid);
    const auto &true_schema = accessor->GetIndexSchema(idx_oid);

    storage::index::IndexBuilder index_builder;
    index_builder.SetKeySchema(true_schema);
    auto index = index_builder.Build();

    EXPECT_TRUE(accessor->SetIndexPointer(idx_oid, index));
    EXPECT_EQ(common::ManagedPointer(index), accessor->GetIndex(idx_oid));
  }
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Get an accessor into the database
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  EXPECT_NE(accessor, nullptr);

  // Check that GetIndexes returns the indexes correct number of indexes
  auto idx_oids = accessor->GetIndexOids(table_oid);
  EXPECT_EQ(num_indexes, idx_oids.size());

  // Fetch all objects with a single call, check that sets are equal
  auto index_objects = accessor->GetIndexes(table_oid);
  EXPECT_EQ(num_indexes, index_objects.size());
  for (const auto &object_pair : index_objects) {
    EXPECT_TRUE(object_pair.first);
    EXPECT_EQ(1, object_pair.second.GetColumns().size());
    EXPECT_EQ("id", object_pair.second.GetColumn(0).Name());
  }

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

/*
 * Exercise a bunch of scenarios of the DDL lock semantics
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, DDLLockTest) {
  // txn0 is used to verify that older concurrent txns can't acquire lock
  auto *txn0 = txn_manager_->BeginTransaction();
  auto accessor0 = catalog_->GetAccessor(common::ManagedPointer(txn0), db_, DISABLED);
  // txn1 is used to verify that older concurrent txns can't acquire the lock
  auto *txn1 = txn_manager_->BeginTransaction();
  auto accessor1 = catalog_->GetAccessor(common::ManagedPointer(txn1), db_, DISABLED);

  // txn2 is used to verify that commit releases the lock
  auto *txn2 = txn_manager_->BeginTransaction();
  auto accessor2 = catalog_->GetAccessor(common::ManagedPointer(txn2), db_, DISABLED);
  // txn3 is used to verify that newer txns (than holder)
  auto *txn3 = txn_manager_->BeginTransaction();
  auto accessor3 = catalog_->GetAccessor(common::ManagedPointer(txn3), db_, DISABLED);

  auto ns_oid = accessor2->CreateNamespace("txn2_ns");  // succeeds, txn2 acquires lock
  EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);

  ns_oid = accessor2->CreateNamespace("txn2_ns2");  // succeeds, txn2 already holds lock
  EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);

  // txn4 is used to verify that newer concurrent transactions can't acquire lock
  auto *txn4 = txn_manager_->BeginTransaction();
  auto accessor4 = catalog_->GetAccessor(common::ManagedPointer(txn4), db_, DISABLED);
  ns_oid = accessor4->CreateNamespace("txn4_ns");
  EXPECT_EQ(ns_oid,
            catalog::INVALID_NAMESPACE_OID);  // fails, txn2 holds lock (txn4 > txn2)
  txn_manager_->Abort(txn4);

  ns_oid = accessor0->CreateNamespace("txn0_ns");
  EXPECT_EQ(ns_oid,
            catalog::INVALID_NAMESPACE_OID);  // fails, txn2 holds lock (txn0 < txn2)
  txn_manager_->Abort(txn0);

  txn_manager_->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);  // txn2 releases the lock

  ns_oid = accessor1->CreateNamespace("txn1_ns");
  EXPECT_EQ(ns_oid,
            catalog::INVALID_NAMESPACE_OID);  // fails, txn2 committed changes (txn1 < txn2)
  txn_manager_->Abort(txn1);

  ns_oid = accessor3->CreateNamespace("txn3_ns");
  EXPECT_EQ(ns_oid,
            catalog::INVALID_NAMESPACE_OID);  // fails, txn2 committed changes (txn3 > txn2, but txn3 < txn2 commit)
  txn_manager_->Abort(txn3);

  // txn5 is used to verify that older concurrent transactions can acquire lock after abort
  auto *txn5 = txn_manager_->BeginTransaction();
  auto accessor5 = catalog_->GetAccessor(common::ManagedPointer(txn5), db_, DISABLED);

  // txn6 is used to verify that abort releases the lock
  auto *txn6 = txn_manager_->BeginTransaction();
  auto accessor6 = catalog_->GetAccessor(common::ManagedPointer(txn6), db_, DISABLED);
  ns_oid = accessor6->CreateNamespace("txn6_ns");
  EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);  // succeeds, txn6 acquires lock
  txn_manager_->Abort(txn6);                          // txn6 releases the lock

  ns_oid = accessor5->GetNamespaceOid("txn2_ns");  // succeeds, txn5 acquires lock (txn5 < txn6)
  EXPECT_TRUE(accessor5->DropNamespace(ns_oid));
  ns_oid = accessor5->GetNamespaceOid("txn2_ns");
  EXPECT_EQ(ns_oid, catalog::INVALID_NAMESPACE_OID);
  txn_manager_->Commit(txn5, transaction::TransactionUtil::EmptyCallback, nullptr);  // txn5 releases the lock
}

}  // namespace noisepage
