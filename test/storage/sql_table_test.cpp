#include "storage/sql_table.h"
#include <algorithm>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"
namespace terrier {

class SqlTableRW {
 public:
  explicit SqlTableRW(catalog::table_oid_t table_oid) : table_oid_(table_oid) {}

  void DefineColumn(std::string name, type::TypeId type, bool nullable, catalog::col_oid_t oid) {
    cols_.emplace_back(name, type, nullable, oid);
  }

  /**
   * Create the SQL table
   */
  void Create() {
    if (schema_ == nullptr) {
      schema_ = new catalog::Schema(cols_);
    }
    if (table_ == nullptr) {
      table_ = new storage::SqlTable(&block_store_, *schema_, table_oid_);
    }

    for (const auto &c : cols_) {
      col_oids_.emplace_back(c.GetOid());
    }
  }

  void StartRow() {
    auto row_pair = table_->InitializerForProjectedRow(col_oids_);
    insert_buffer_ = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
    insert_ = row_pair.first.InitializeRow(insert_buffer_);
  }

  storage::TupleSlot EndRowAndInsert() {
    auto txn = txn_manager_.BeginTransaction();
    auto slot = table_->Insert(txn, *insert_);
    insert_ = nullptr;
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);

    delete[] insert_buffer_;
    delete txn;
    return storage::TupleSlot(slot.GetBlock(), slot.GetOffset());
  }

  uint32_t GetIntColInRow(int32_t col_num, storage::TupleSlot slot) {
    auto txn = txn_manager_.BeginTransaction();
    auto row_pair = table_->InitializerForProjectedRow(col_oids_);
    auto read_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
    storage::ProjectedRow *read = row_pair.first.InitializeRow(read_buffer);
    table_->Select(txn, slot, read);
    byte *col_p = read->AccessForceNotNull(row_pair.second[col_oids_[col_num]]);
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    return *(reinterpret_cast<uint32_t *>(col_p));
  }

  void SetIntColInRow(int32_t col_num, int32_t value) {
    auto row_pair = table_->InitializerForProjectedRow(col_oids_);
    byte *col_p = insert_->AccessForceNotNull(row_pair.second[col_oids_[col_num]]);
    (*reinterpret_cast<uint32_t *>(col_p)) = value;
  }

  char *GetVarcharColInRow(int32_t col_num, storage::TupleSlot slot) {
    auto txn = txn_manager_.BeginTransaction();
    auto row_pair = table_->InitializerForProjectedRow(col_oids_);
    auto read_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
    storage::ProjectedRow *read = row_pair.first.InitializeRow(read_buffer);
    table_->Select(txn, slot, read);
    byte *col_p = read->AccessForceNotNull(row_pair.second[col_oids_[col_num]]);

    auto *entry = reinterpret_cast<storage::VarlenEntry *>(col_p);
    // stored string has no null terminator, add space for it
    uint32_t size = entry->Size() + 1;
    // allocate return string
    auto *ret_st = static_cast<char *>(malloc(size));
    memcpy(ret_st, entry->Content(), size);
    // add the null terminator
    *(ret_st + size - 1) = 0;
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    return ret_st;
  }

  void SetVarcharColInRow(int32_t col_num, const char *st) {
    auto row_pair = table_->InitializerForProjectedRow(col_oids_);
    byte *col_p = insert_->AccessForceNotNull(row_pair.second[col_oids_[col_num]]);
    // string size, without null terminator
    size_t size = strlen(st);
    byte *varlen = common::AllocationUtil::AllocateAligned(size);
    memcpy(varlen, st, size);
    *reinterpret_cast<storage::VarlenEntry *>(col_p) = {varlen, static_cast<uint32_t>(size), false};
  }

 private:
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};
  transaction::TransactionManager txn_manager_ = {&buffer_pool_, true, LOGGING_DISABLED};

  storage::BlockStore block_store_{100, 100};
  catalog::table_oid_t table_oid_;
  storage::SqlTable *table_ = nullptr;

  catalog::Schema *schema_ = nullptr;
  std::vector<catalog::Schema::Column> cols_;
  std::vector<catalog::col_oid_t> col_oids_;

  // std::pair<terrier::storage::ProjectedRowInitializer, terrier::storage::ProjectionMap> *row_pair_ = nullptr;

  byte *insert_buffer_ = nullptr;
  storage::ProjectedRow *insert_ = nullptr;
};

struct SqlTableTests : public TerrierTest {
  void SetUp() override {
    TerrierTest::SetUp();
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);
  }

  void TearDown() override {
    TerrierTest::TearDown();
    delete txn_manager_;
  }

  storage::RecordBufferSegmentPool buffer_pool_{100, 100};
  transaction::TransactionManager *txn_manager_;
};

// NOLINTNEXTLINE
TEST_F(SqlTableTests, DISABLED_SelectInsertTest) {
  auto txn1 = txn_manager_->BeginTransaction();

  // create a sql table
  storage::BlockStore block_store{100, 100};
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, catalog::col_oid_t(0));
  cols.emplace_back("datname", type::TypeId::INTEGER, false, catalog::col_oid_t(1));
  catalog::Schema schema(cols);
  storage::SqlTable test(&block_store, schema, catalog::table_oid_t(2));

  std::vector<catalog::col_oid_t> col_oids;
  col_oids.emplace_back(cols[0].GetOid());
  col_oids.emplace_back(cols[1].GetOid());
  auto row_pair = test.InitializerForProjectedRow(col_oids);

  // Insert a row
  byte *insert_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *insert = row_pair.first.InitializeRow(insert_buffer);
  // set the first value to be 100
  byte *first = insert->AccessForceNotNull(row_pair.second[col_oids[0]]);
  (*reinterpret_cast<uint32_t *>(first)) = 100;
  // set the second attribute to be 15271
  byte *second = insert->AccessForceNotNull(row_pair.second[col_oids[1]]);
  (*reinterpret_cast<uint32_t *>(second)) = 15721;
  auto slot = test.Insert(txn1, *insert);

  txn_manager_->Commit(txn1, TestCallbacks::EmptyCallback, nullptr);
  delete txn1;

  auto txn2 = txn_manager_->BeginTransaction();
  // read that row
  auto read_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *read = row_pair.first.InitializeRow(read_buffer);

  bool select_result = test.Select(txn2, slot, read);
  txn_manager_->Commit(txn2, TestCallbacks::EmptyCallback, nullptr);

  delete txn2;
  EXPECT_TRUE(select_result);
  delete[] insert_buffer;
  delete[] read_buffer;
  // TODO(yangjuns): check actual value
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, NewSelectInsertTest) {
  // SqlTableRW table = {catalog::table_oid_t(2)};
  SqlTableRW table(catalog::table_oid_t(2));

  table.DefineColumn("id", type::TypeId::INTEGER, false, catalog::col_oid_t(0));
  table.DefineColumn("datname", type::TypeId::INTEGER, false, catalog::col_oid_t(1));
  table.Create();
  table.StartRow();
  table.SetIntColInRow(0, 100);
  table.SetIntColInRow(1, 15721);
  storage::TupleSlot row1_slot = table.EndRowAndInsert();

  table.StartRow();
  table.SetIntColInRow(0, 200);
  table.SetIntColInRow(1, 25721);
  storage::TupleSlot row2_slot = table.EndRowAndInsert();

  uint32_t id = table.GetIntColInRow(0, row1_slot);
  EXPECT_EQ(100, id);
  uint32_t datname = table.GetIntColInRow(1, row1_slot);
  EXPECT_EQ(15721, datname);

  id = table.GetIntColInRow(0, row2_slot);
  EXPECT_EQ(200, id);
  datname = table.GetIntColInRow(1, row2_slot);
  EXPECT_EQ(25721, datname);
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, DISABLED_VarlenInsertTest) {
  auto txn1 = txn_manager_->BeginTransaction();

  // create a sql table
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, catalog::col_oid_t(0));
  cols.emplace_back("datname", type::TypeId::VARCHAR, false, catalog::col_oid_t(1));
  catalog::Schema schema(cols);

  storage::BlockStore block_store{100, 100};
  storage::SqlTable test(&block_store, schema, catalog::table_oid_t(2));

  std::vector<catalog::col_oid_t> col_oids;
  col_oids.emplace_back(cols[0].GetOid());
  col_oids.emplace_back(cols[1].GetOid());
  auto row_pair = test.InitializerForProjectedRow(col_oids);

  // Insert a row
  byte *insert_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());

  storage::ProjectedRow *insert = row_pair.first.InitializeRow(insert_buffer);
  // set the first value to be 100
  byte *first = insert->AccessForceNotNull(row_pair.second[col_oids[0]]);
  (*reinterpret_cast<uint32_t *>(first)) = 100;

  byte *second = insert->AccessForceNotNull(row_pair.second[col_oids[1]]);
  const char *db_name = "name";
  memcpy(second, db_name, strlen(db_name) - 1);
  auto slot = test.Insert(txn1, *insert);

  txn_manager_->Commit(txn1, TestCallbacks::EmptyCallback, nullptr);
  delete txn1;

  auto txn2 = txn_manager_->BeginTransaction();
  // read that row
  auto read_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *read = row_pair.first.InitializeRow(read_buffer);

  test.Select(txn2, slot, read);
  txn_manager_->Commit(txn2, TestCallbacks::EmptyCallback, nullptr);
  delete txn2;
  EXPECT_TRUE(true);
  delete[] insert_buffer;
  delete[] read_buffer;
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, NewVarlenInsertTest) {
  SqlTableRW table(catalog::table_oid_t(2));

  table.DefineColumn("id", type::TypeId::INTEGER, false, catalog::col_oid_t(0));
  table.DefineColumn("datname", type::TypeId::VARCHAR, false, catalog::col_oid_t(1));
  table.Create();

  table.StartRow();
  table.SetIntColInRow(0, 100);
  table.SetVarcharColInRow(1, "name");
  storage::TupleSlot row_slot = table.EndRowAndInsert();

  uint32_t id = table.GetIntColInRow(0, row_slot);
  EXPECT_EQ(100, id);
  char *table_name = table.GetVarcharColInRow(1, row_slot);
  EXPECT_STREQ("name", table_name);
  free(table_name);
}

}  // namespace terrier
