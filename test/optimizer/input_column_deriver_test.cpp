#include <memory>
#include <utility>
#include <vector>

#include "catalog/postgres/pg_statistic.h"
#include "loggers/optimizer_logger.h"
#include "main/db_main.h"
#include "optimizer/input_column_deriver.h"
#include "optimizer/memo.h"
#include "optimizer/physical_operators.h"
#include "optimizer/property_set.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/column_value_expression.h"
#include "storage/sql_table.h"
#include "test_util/test_harness.h"

namespace noisepage::optimizer {

struct InputColumnDeriverTest : public TerrierTest {
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
    input_column_deriver_ = std::make_unique<InputColumnDeriver>(txn_, accessor_.get());
  }

  void TearDown() override {
    TerrierTest::TearDown();
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    accessor_.reset(nullptr);
  }

  std::unique_ptr<InputColumnDeriver> input_column_deriver_;
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
TEST_F(InputColumnDeriverTest, AnalyzeTest) {
  PropertySet properties;
  std::vector<catalog::col_oid_t> cols;
  for (auto &col : table_a_col_oids_) {
    cols.emplace_back(col);
  }
  auto analyze = Analyze::Make(db_oid_, table_a_oid_, std::move(cols));
  GroupExpression gexpr(analyze, {}, txn_);
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols;
  Memo memo;

  auto [output_cols, input_cols] = input_column_deriver_->DeriveInputColumns(&gexpr, &properties, required_cols, &memo);

  EXPECT_EQ(output_cols.size(), 0);
  EXPECT_EQ(input_cols.size(), 1);
  auto agg_cols = input_cols[0];
  EXPECT_EQ(agg_cols.size(), catalog::postgres::PgStatistic::NUM_ANALYZE_AGGREGATES * table_a_col_oids_.size() + 1);
  EXPECT_EQ(agg_cols[0]->GetExpressionType(), parser::ExpressionType::AGGREGATE_COUNT);
  EXPECT_EQ(agg_cols[0]->GetChildrenSize(), 1);
  EXPECT_EQ(agg_cols[0]->GetChild(0)->GetExpressionType(), parser::ExpressionType::STAR);
  for (size_t i = 1; i < agg_cols.size(); i++) {
    auto agg_index = (i - 1) % catalog::postgres::PgStatistic::NUM_ANALYZE_AGGREGATES;
    const auto &col_info = catalog::postgres::PgStatistic::ANALYZE_AGGREGATES.at(agg_index);
    auto agg_col = agg_cols[i].CastManagedPointerTo<parser::AggregateExpression>();
    EXPECT_EQ(agg_col->GetExpressionType(), col_info.aggregate_type_);
    EXPECT_EQ(agg_col->GetChildrenSize(), 1);
    EXPECT_EQ(agg_col->IsDistinct(), col_info.distinct_);

    auto child_col = agg_col->GetChild(0);
    EXPECT_EQ(child_col->GetExpressionType(), parser::ExpressionType::COLUMN_VALUE);
    auto cve = child_col.CastManagedPointerTo<parser::ColumnValueExpression>();
    EXPECT_EQ(cve->GetDatabaseOid(), db_oid_);
    EXPECT_EQ(cve->GetTableOid(), table_a_oid_);
    auto col_index = (i - 1) / catalog::postgres::PgStatistic::NUM_ANALYZE_AGGREGATES;
    EXPECT_EQ(cve->GetColumnOid(), table_a_col_oids_[col_index]);
  }
}

}  // namespace noisepage::optimizer
