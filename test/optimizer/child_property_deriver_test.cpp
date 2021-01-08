#include <memory>
#include <utility>
#include <vector>

#include "loggers/optimizer_logger.h"
#include "main/db_main.h"
#include "optimizer/child_property_deriver.h"
#include "optimizer/memo.h"
#include "optimizer/physical_operators.h"
#include "optimizer/property_set.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/column_value_expression.h"
#include "storage/sql_table.h"
#include "test_util/test_harness.h"

namespace noisepage::optimizer {

struct ChildPropertyDeriverTest : public TerrierTest {
 protected:
  void SetUpTables() {
    // create database
    txn_ = txn_manager_->BeginTransaction();
    OPTIMIZER_LOG_DEBUG("Creating database %s", default_database_name_.c_str());
    db_oid_ = catalog_->CreateDatabase(common::ManagedPointer(txn_), default_database_name_, true);
    // commit the transactions
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    OPTIMIZER_LOG_DEBUG("database %s created!", default_database_name_.c_str());

    // get default values of the columns
    auto int_default = parser::ConstantValueExpression(type::TypeId::INTEGER);
    auto varchar_default = parser::ConstantValueExpression(type::TypeId::VARCHAR);

    // create table A
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_oid_, DISABLED);
    // Create the column definition (no OIDs) for CREATE TABLE A(A1 int, a2 varchar)
    std::vector<catalog::Schema::Column> cols_a;
    cols_a.emplace_back("a1", type::TypeId::INTEGER, true, int_default);
    cols_a.emplace_back("a2", type::TypeId::VARCHAR, 20, true, varchar_default);
    auto schema_a = catalog::Schema(cols_a);

    table_a_oid_ = accessor_->CreateTable(accessor_->GetDefaultNamespace(), "a", schema_a);
    auto table_a = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema_a);
    EXPECT_TRUE(accessor_->SetTablePointer(table_a_oid_, table_a));

    for (const auto &col : accessor_->GetSchema(table_a_oid_).GetColumns()) {
      table_a_col_oids_.emplace_back(col.Oid());
    }

    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    accessor_.reset(nullptr);
  }

  void SetUp() override {
    TerrierTest::SetUp();

    db_main_ = noisepage::DBMain::Builder().SetUseGC(true).SetUseCatalog(true).Build();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();

    SetUpTables();
    // prepare for testing
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_oid_, DISABLED);
  }

  void TearDown() override {
    TerrierTest::TearDown();
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    accessor_.reset(nullptr);
  }

  ChildPropertyDeriver child_property_deriver_;
  std::unique_ptr<catalog::CatalogAccessor> accessor_;
  transaction::TransactionContext *txn_;
  catalog::db_oid_t db_oid_;
  catalog::table_oid_t table_a_oid_;
  std::vector<catalog::col_oid_t> table_a_col_oids_;

 private:
  std::string default_database_name_ = "test_db";
  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<catalog::Catalog> catalog_;
};

// NOLINTNEXTLINE
TEST_F(ChildPropertyDeriverTest, AnalyzeTest) {
  Memo memo;
  PropertySet requirements;

  std::vector<catalog::col_oid_t> cols;
  for (auto &col : table_a_col_oids_) {
    cols.emplace_back(col);
  }
  auto analyze = Analyze::Make(db_oid_, table_a_oid_, std::move(cols));
  GroupExpression gexpr(analyze, {}, txn_);

  auto properties = child_property_deriver_.GetProperties(accessor_.get(), &memo, &requirements, &gexpr);
  EXPECT_EQ(properties.size(), 1);

  auto [output_props, input_props] = properties.at(0);
  EXPECT_EQ(output_props->Properties().size(), 0);
  EXPECT_EQ(input_props.size(), 1);
  EXPECT_EQ(input_props.at(0)->Properties().size(), 0);
}

}  // namespace noisepage::optimizer
