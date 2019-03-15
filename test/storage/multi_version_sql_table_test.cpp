#include <algorithm>
#include <cstring>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include "storage/sql_table.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"
namespace terrier {

/**
 * Help class to simplify operations on a SqlTable
 */
class SqlTableRW {
 public:
  explicit SqlTableRW(catalog::table_oid_t table_oid) : version_(0), table_oid_(table_oid) {}
  ~SqlTableRW() {
    delete pri_;
    delete pr_map_;
    delete schema_;
    delete table_;
  }

  /**
   * Append a column definition to the internal list. The list will be
   * used when creating the SqlTable.
   * @param name of the column
   * @param type of the column
   * @param nullable
   * @param oid for the column
   */
  void DefineColumn(std::string name, type::TypeId type, bool nullable, catalog::col_oid_t oid) {
    cols_.emplace_back(name, type, nullable, oid);
  }

  void AddColumn(transaction::TransactionContext *txn, std::string name, type::TypeId type, bool nullable,
                 catalog::col_oid_t oid) {
    cols_.emplace_back(name, type, nullable, oid);
    catalog::Schema new_schema(cols_);
    table_->ChangeSchema(txn, new_schema);

    col_oids_.clear();
    for (const auto &c : cols_) {
      col_oids_.emplace_back(c.GetOid());
      LOG_INFO("{}", !c.GetOid());
    }

    // save information needed for (later) reading and writing
    auto row_pair = table_->InitializerForProjectedRow(col_oids_, version_);
    pri_ = new storage::ProjectedRowInitializer(std::get<0>(row_pair));
    pr_map_ = new storage::ProjectionMap(std::get<1>(row_pair));
  }
  /**
   * Create the SQL table.
   */
  void Create() {
    schema_ = new catalog::Schema(cols_);
    table_ = new storage::SqlTable(&block_store_, *schema_, table_oid_);

    for (const auto &c : cols_) {
      col_oids_.emplace_back(c.GetOid());
    }

    // save information needed for (later) reading and writing
    auto row_pair = table_->InitializerForProjectedRow(col_oids_);
    pri_ = new storage::ProjectedRowInitializer(std::get<0>(row_pair));
    pr_map_ = new storage::ProjectionMap(std::get<1>(row_pair));
  }

  /**
   * First step in writing a row.
   */
  void StartRow() {
    insert_buffer_ = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    insert_ = pri_->InitializeRow(insert_buffer_);
  }

  /**
   * Insert the row into the table
   * @return slot where the row was created
   */
  storage::TupleSlot EndRowAndInsert(transaction::TransactionContext *txn) {
    auto slot = table_->Insert(txn, *insert_);
    insert_ = nullptr;
    delete[] insert_buffer_;
    return storage::TupleSlot(slot.GetBlock(), slot.GetOffset());
  }

  /**
   * Read an integer from a row
   * @param col_num column number in the schema
   * @param slot - tuple to read from
   * @return integer value
   */
  uint32_t GetIntColInRow(transaction::TransactionContext *txn, int32_t col_num, storage::TupleSlot slot) {
    auto read_buffer = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    storage::ProjectedRow *read = pri_->InitializeRow(read_buffer);
    table_->Select(txn, slot, read, version_);
    LOG_INFO("before force not null ...");
    byte *col_p = read->AccessWithNullCheck(pr_map_->at(col_oids_[col_num]));
    printf("address %p \n", col_p);
    uint32_t ret_val;
    if (col_p == nullptr) {
      ret_val = 12345;
    } else {
      LOG_INFO("right before dereferencing ... ");
      ret_val = *(reinterpret_cast<uint32_t *>(col_p));
    }
    delete[] read_buffer;
    return ret_val;
  }

  /**
   * Save an integer, for insertion by EndRowAndInsert
   * @param col_num column number in the schema
   * @param value to save
   */
  void SetIntColInRow(int32_t col_num, int32_t value) {
    byte *col_p = insert_->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));
    (*reinterpret_cast<uint32_t *>(col_p)) = value;
  }

  /**
   * Read a string from a row
   * @param col_num column number in the schema
   * @param slot - tuple to read from
   * @return malloc'ed C string (with null terminator). Caller must
   *   free.
   */
  char *GetVarcharColInRow(transaction::TransactionContext *txn, int32_t col_num, storage::TupleSlot slot) {
    auto read_buffer = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    storage::ProjectedRow *read = pri_->InitializeRow(read_buffer);
    table_->Select(txn, slot, read, version_);
    byte *col_p = read->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));

    auto *entry = reinterpret_cast<storage::VarlenEntry *>(col_p);
    // stored string has no null terminator, add space for it
    uint32_t size = entry->Size() + 1;
    // allocate return string
    auto *ret_st = static_cast<char *>(malloc(size));
    std::memcpy(ret_st, entry->Content(), size);
    // add the null terminator
    *(ret_st + size - 1) = 0;

    delete txn;
    delete[] read_buffer;
    return ret_st;
  }

  /**
   * Save a string, for insertion by EndRowAndInsert
   * @param col_num column number in the schema
   * @param st C string to save.
   */
  void SetVarcharColInRow(int32_t col_num, const char *st) {
    byte *col_p = insert_->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));
    // string size, without null terminator
    size_t size = strlen(st);
    byte *varlen = common::AllocationUtil::AllocateAligned(size);
    std::memcpy(varlen, st, size);
    *reinterpret_cast<storage::VarlenEntry *>(col_p) = {varlen, static_cast<uint32_t>(size), false};
  }

 public:
  // This is a public field that transactions can set and read.
  // The purpose is to record the version for each transaction. In reality this information should be retrieved from
  // catalog. We just make it for the test case.
  storage::layout_version_t version_;

 private:
  storage::BlockStore block_store_{100, 100};
  catalog::table_oid_t table_oid_;
  storage::SqlTable *table_ = nullptr;

  catalog::Schema *schema_ = nullptr;
  std::vector<catalog::Schema::Column> cols_;
  std::vector<catalog::col_oid_t> col_oids_;

  // this pri and pr_map are the initializer and map for the most recent version
  storage::ProjectedRowInitializer *pri_ = nullptr;
  storage::ProjectionMap *pr_map_ = nullptr;

  byte *insert_buffer_ = nullptr;
  storage::ProjectedRow *insert_ = nullptr;
};

struct SqlTableTests : public TerrierTest {
  void SetUp() override { TerrierTest::SetUp(); }

  void TearDown() override { TerrierTest::TearDown(); }

  storage::RecordBufferSegmentPool buffer_pool_{100, 100};
  transaction::TransactionManager txn_manager_ = {&buffer_pool_, true, LOGGING_DISABLED};
};

// NOLINTNEXTLINE
TEST_F(SqlTableTests, SelectInsertTest) {
  SqlTableRW table(catalog::table_oid_t(2));
  auto txn = txn_manager_.BeginTransaction();
  table.DefineColumn("id", type::TypeId::INTEGER, false, catalog::col_oid_t(0));
  table.DefineColumn("datname", type::TypeId::INTEGER, false, catalog::col_oid_t(1));
  table.Create();
  table.StartRow();
  table.SetIntColInRow(0, 100);
  table.SetIntColInRow(1, 10000);
  storage::TupleSlot row1_slot = table.EndRowAndInsert(txn);

  table.StartRow();
  table.SetIntColInRow(0, 200);
  table.SetIntColInRow(1, 10001);
  storage::TupleSlot row2_slot = table.EndRowAndInsert(txn);

  uint32_t id = table.GetIntColInRow(txn, 0, row1_slot);
  EXPECT_EQ(100, id);
  uint32_t datname = table.GetIntColInRow(txn, 1, row1_slot);
  EXPECT_EQ(10000, datname);

  // manually set the version of the transaction to be 1
  table.version_ = storage::layout_version_t(1);
  table.AddColumn(txn, "new_col", type::TypeId::INTEGER, true, catalog::col_oid_t(2));

  id = table.GetIntColInRow(txn, 0, row1_slot);
  EXPECT_EQ(100, id);
  datname = table.GetIntColInRow(txn, 1, row1_slot);
  EXPECT_EQ(10000, datname);

  id = table.GetIntColInRow(txn, 0, row2_slot);
  EXPECT_EQ(200, id);
  datname = table.GetIntColInRow(txn, 1, row2_slot);
  EXPECT_EQ(10001, datname);

  uint32_t new_col = table.GetIntColInRow(txn, 2, row1_slot);
  EXPECT_EQ(12345, new_col);

  txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
}

//// NOLINTNEXTLINE
// TEST_F(SqlTableTests, VarlenInsertTest) {
//  SqlTableRW table(catalog::table_oid_t(2));
//
//  table.DefineColumn("id", type::TypeId::INTEGER, false, catalog::col_oid_t(0));
//  table.DefineColumn("datname", type::TypeId::VARCHAR, false, catalog::col_oid_t(1));
//  table.Create();
//
//  table.StartRow();
//  table.SetIntColInRow(0, 100);
//  table.SetVarcharColInRow(1, "name");
//  storage::TupleSlot row_slot = table.EndRowAndInsert();
//
//  uint32_t id = table.GetIntColInRow(0, row_slot);
//  EXPECT_EQ(100, id);
//  char *table_name = table.GetVarcharColInRow(1, row_slot);
//  EXPECT_STREQ("name", table_name);
//  free(table_name);
//}

}  // namespace terrier
