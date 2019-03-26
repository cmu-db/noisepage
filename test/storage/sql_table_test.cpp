#include "storage/sql_table.h"
#include <algorithm>
#include <cstring>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/catalog_sql_table.h"
#include "catalog/database_handle.h"
#include "common/exception.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"
namespace terrier {

struct SqlTableTests : public TerrierTest {
  void SetUp() override { TerrierTest::SetUp(); }

  void TearDown() override { TerrierTest::TearDown(); }

  void CheckRow(const std::vector<type::Value> &ref_row, const std::vector<type::Value> &row) {
    EXPECT_EQ(ref_row.size(), row.size());
    for (uint32_t i = 0; i < ref_row.size(); i++) {
      EXPECT_TRUE(ref_row[i] == row[i]);
    }
  }

  storage::RecordBufferSegmentPool buffer_pool_{100, 100};
  transaction::TransactionManager txn_manager_ = {&buffer_pool_, true, LOGGING_DISABLED};
};

// NOLINTNEXTLINE
TEST_F(SqlTableTests, SelectInsertTest) {
  std::vector<type::Value> found_row;
  catalog::SqlTableRW table(catalog::table_oid_t(2));

  auto txn = txn_manager_.BeginTransaction();
  table.DefineColumn("id", type::TypeId::INTEGER, false, catalog::col_oid_t(0));
  table.DefineColumn("datname", type::TypeId::INTEGER, false, catalog::col_oid_t(1));
  table.Create();

  std::vector<type::Value> row1;
  row1.emplace_back(type::ValueFactory::GetIntegerValue(100));
  row1.emplace_back(type::ValueFactory::GetIntegerValue(15721));
  table.InsertRow(txn, row1);

  std::vector<type::Value> row2;
  row2.emplace_back(type::ValueFactory::GetIntegerValue(200));
  row2.emplace_back(type::ValueFactory::GetIntegerValue(25721));
  table.InsertRow(txn, row2);

  // This operation is slow, due to how sequential scan is done for a datatable.
  auto num_rows = table.GetNumRows(txn);
  EXPECT_EQ(2, num_rows);

  std::vector<type::Value> search_vec;
  search_vec.emplace_back(type::ValueFactory::GetIntegerValue(100));
  found_row = table.FindRow(txn, search_vec);
  CheckRow(row1, found_row);

  search_vec.clear();
  search_vec.emplace_back(type::ValueFactory::GetIntegerValue(200));
  found_row = table.FindRow(txn, search_vec);
  CheckRow(row2, found_row);

  txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
  delete txn;
}

/**
 * Insertion test, with content verification using the Value vector calls
 */
// NOLINTNEXTLINE
TEST_F(SqlTableTests, SelectInsertTest1) {
  catalog::SqlTableRW table(catalog::table_oid_t(2));

  auto txn = txn_manager_.BeginTransaction();
  table.DefineColumn("id", type::TypeId::INTEGER, false, catalog::col_oid_t(0));
  table.DefineColumn("c1", type::TypeId::INTEGER, false, catalog::col_oid_t(1));
  table.DefineColumn("c2", type::TypeId::INTEGER, false, catalog::col_oid_t(2));
  table.Create();

  std::vector<type::Value> row1;
  row1.emplace_back(type::ValueFactory::GetIntegerValue(100));
  row1.emplace_back(type::ValueFactory::GetIntegerValue(15721));
  row1.emplace_back(type::ValueFactory::GetIntegerValue(17));
  table.InsertRow(txn, row1);

  std::vector<type::Value> row2;
  row2.emplace_back(type::ValueFactory::GetIntegerValue(200));
  row2.emplace_back(type::ValueFactory::GetIntegerValue(25721));
  row2.emplace_back(type::ValueFactory::GetIntegerValue(27));
  table.InsertRow(txn, row2);

  auto it = table.begin(txn);
  while (it != table.end(txn)) {
    // std::cout << *it << std::endl;
    auto layout = table.GetLayout();
    storage::ProjectedColumns::RowView row_view = it->InterpretAsRow(layout, 0);
    auto ret_vec = table.ColToValueVec(row_view);
    ++it;
  }

  // search for a single column
  std::vector<type::Value> search_vec;
  search_vec.emplace_back(type::ValueFactory::GetIntegerValue(100));

  // search for a value in column 0
  auto found_row = table.FindRow(txn, search_vec);
  CheckRow(row1, found_row);

  // add a value for column 1 and search again
  search_vec.emplace_back(type::ValueFactory::GetIntegerValue(15721));
  found_row = table.FindRow(txn, search_vec);
  CheckRow(row1, found_row);

  // now search for a non-existent value in column 2. This is slow.
  search_vec.clear();
  search_vec.emplace_back(type::ValueFactory::GetIntegerValue(19));
  found_row = table.FindRow(txn, search_vec);
  EXPECT_EQ(0, found_row.size());

  // search for second item
  search_vec.clear();
  search_vec.emplace_back(type::ValueFactory::GetIntegerValue(200));
  found_row = table.FindRow(txn, search_vec);
  CheckRow(row2, found_row);

  txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
  delete txn;
}

/**
 * Simple iterator test
 */
// NOLINTNEXTLINE
TEST_F(SqlTableTests, IteratorTest) {
  catalog::SqlTableRW table(catalog::table_oid_t(2));

  auto txn = txn_manager_.BeginTransaction();
  table.DefineColumn("id", type::TypeId::INTEGER, false, catalog::col_oid_t(0));
  table.DefineColumn("c1", type::TypeId::INTEGER, false, catalog::col_oid_t(1));
  table.DefineColumn("c2", type::TypeId::INTEGER, false, catalog::col_oid_t(2));
  table.Create();

  std::vector<type::Value> row1;
  row1.emplace_back(type::ValueFactory::GetIntegerValue(100));
  row1.emplace_back(type::ValueFactory::GetIntegerValue(15721));
  row1.emplace_back(type::ValueFactory::GetIntegerValue(17));
  table.InsertRow(txn, row1);

  std::vector<type::Value> row2;
  row2.emplace_back(type::ValueFactory::GetIntegerValue(200));
  row2.emplace_back(type::ValueFactory::GetIntegerValue(25721));
  row2.emplace_back(type::ValueFactory::GetIntegerValue(27));
  table.InsertRow(txn, row2);

  // for verification
  std::vector<int32_t> row_verify = {100, 200};
  int32_t row_verify_index = 0;

  auto it = table.begin(txn);
  while (it != table.end(txn)) {
    auto layout = table.GetLayout();
    storage::ProjectedColumns::RowView row_view = it->InterpretAsRow(layout, 0);
    auto ret_vec = table.ColToValueVec(row_view);
    EXPECT_EQ(row_verify[row_verify_index], ret_vec[0].GetIntValue());
    row_verify_index++;
    ++it;
  }
  EXPECT_EQ(row_verify.size(), row_verify_index);

  txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
  delete txn;
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, VarlenInsertTest) {
  catalog::SqlTableRW table(catalog::table_oid_t(2));
  auto txn = txn_manager_.BeginTransaction();

  table.DefineColumn("id", type::TypeId::INTEGER, false, catalog::col_oid_t(0));
  table.DefineColumn("datname", type::TypeId::VARCHAR, false, catalog::col_oid_t(1));
  table.Create();

  std::vector<type::Value> insert_vec;
  insert_vec.emplace_back(type::ValueFactory::GetIntegerValue(100));
  insert_vec.emplace_back(type::ValueFactory::GetVarcharValue("name"));
  table.InsertRow(txn, insert_vec);

  std::vector<type::Value> search_vec;
  search_vec.emplace_back(type::ValueFactory::GetIntegerValue(100));

  auto found_row = table.FindRow(txn, search_vec);
  CheckRow(insert_vec, found_row);

  txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
  delete txn;
}

}  // namespace terrier
