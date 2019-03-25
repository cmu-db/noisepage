#include "storage/sql_table.h"
#include <algorithm>
#include <cstring>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"
namespace terrier {

/**
 * Help class to simplify operations on a SqlTable
 */
class SqlTableTestRW {
 public:
  explicit SqlTableTestRW(catalog::table_oid_t table_oid) : version_(0), table_oid_(table_oid) {}
  ~SqlTableTestRW() {
    delete pri_;
    delete pr_map_;
    delete schema_;
    delete table_;
    delete layout_;
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
    // update columns, schema and layout
    cols_.emplace_back(name, type, nullable, oid);
    delete schema_;
    delete layout_;
    schema_ = new catalog::Schema(cols_, next_version_++);
    layout_ = new storage::BlockLayout(storage::StorageUtil::BlockLayoutFromSchema(*schema_).first);

    table_->UpdateSchema(*schema_);

    col_oids_.clear();
    for (const auto &c : cols_) {
      col_oids_.emplace_back(c.GetOid());
      LOG_INFO("{}", !c.GetOid());
    }

    delete pri_;
    delete pr_map_;

    // save information needed for (later) reading and writing
    auto row_pair = table_->InitializerForProjectedRow(col_oids_, version_);
    pri_ = new storage::ProjectedRowInitializer(std::get<0>(row_pair));
    pr_map_ = new storage::ProjectionMap(std::get<1>(row_pair));
  }
  /**
   * Create the SQL table.
   */
  void Create() {
    schema_ = new catalog::Schema(cols_, next_version_++);
    table_ = new storage::SqlTable(&block_store_, *schema_, table_oid_);

    layout_ = new storage::BlockLayout(storage::StorageUtil::BlockLayoutFromSchema(*schema_).first);
    for (const auto &c : cols_) {
      col_oids_.emplace_back(c.GetOid());
    }

    // save information needed for (later) reading and writing
    auto row_pair = table_->InitializerForProjectedRow(col_oids_, version_);
    pri_ = new storage::ProjectedRowInitializer(std::get<0>(row_pair));
    pr_map_ = new storage::ProjectionMap(std::get<1>(row_pair));
  }

  /**
   * First step in inserting a row.
   */
  void StartInsertRow() { ResetProjectedRow(col_oids_); }

  /**
   * Insert the row into the table
   * @return slot where the row was created
   */
  storage::TupleSlot EndInsertRow(transaction::TransactionContext *txn) {
    auto slot = table_->Insert(txn, *pr_, version_);
    pr_ = nullptr;
    delete[] buffer_;
    return storage::TupleSlot(slot.GetBlock(), slot.GetOffset());
  }

  void StartUpdateRow(const std::vector<catalog::col_oid_t> &col_oids) {
    auto pr_pair = table_->InitializerForProjectedRow(col_oids, version_);
    buffer_ = common::AllocationUtil::AllocateAligned(pr_pair.first.ProjectedRowSize());
    pr_ = pr_pair.first.InitializeRow(buffer_);

    delete pri_;
    delete pr_map_;
    pri_ = new storage::ProjectedRowInitializer(std::get<0>(pr_pair));
    pr_map_ = new storage::ProjectionMap(std::get<1>(pr_pair));
  }

  std::pair<bool, storage::TupleSlot> EndUpdateRow(transaction::TransactionContext *txn, storage::TupleSlot slot) {
    auto result_pair = table_->Update(txn, slot, *pr_, *pr_map_, version_);
    pr_ = nullptr;
    delete[] buffer_;
    return result_pair;
  }

  /**
   * Check if a tuple is visible to you
   * @param txn
   * @param slot
   * @return
   */
  bool Visible(transaction::TransactionContext *txn, storage::TupleSlot slot) {
    buffer_ = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    pr_ = pri_->InitializeRow(buffer_);
    bool result = table_->Select(txn, slot, pr_, *pr_map_, version_);
    delete[] buffer_;
    return result;
  }

  /**
   * Read an integer from a row
   * @param col_num column number in the schema
   * @param slot - tuple to read from
   * @return integer value
   */
  uint32_t GetIntColInRow(transaction::TransactionContext *txn, catalog::col_oid_t col_oid, storage::TupleSlot slot) {
    auto read_buffer = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    storage::ProjectedRow *read = pri_->InitializeRow(read_buffer);
    table_->Select(txn, slot, read, *pr_map_, version_);
    byte *col_p = read->AccessWithNullCheck(pr_map_->at(col_oid));
    uint32_t ret_val;
    if (col_p == nullptr) {
      ret_val = 12345;
    } else {
      ret_val = *(reinterpret_cast<uint32_t *>(col_p));
    }
    delete[] read_buffer;
    return ret_val;
  }

  /**
   * Save an integer, for insertion by EndInsertRow
   * @param col_num column number in the schema
   * @param value to save
   */
  void SetIntColInRow(catalog::col_oid_t col_oid, int32_t value) {
    byte *col_p = pr_->AccessForceNotNull(pr_map_->at(col_oid));
    (*reinterpret_cast<uint32_t *>(col_p)) = value;
  }

  /**
   * Read a string from a row
   * @param col_num column number in the schema
   * @param slot - tuple to read from
   * @return malloc'ed C string (with null terminator). Caller must
   *   free.
   */
  char *GetVarcharColInRow(transaction::TransactionContext *txn, catalog::col_oid_t(col_oid), storage::TupleSlot slot) {
    auto read_buffer = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    storage::ProjectedRow *read = pri_->InitializeRow(read_buffer);
    table_->Select(txn, slot, read, *pr_map_, version_);
    byte *col_p = read->AccessForceNotNull(pr_map_->at(col_oid));

    auto *entry = reinterpret_cast<storage::VarlenEntry *>(col_p);
    // stored string has no null terminator, add space for it
    uint32_t size = entry->Size() + 1;
    // allocate return string
    auto *ret_st = reinterpret_cast<char *>(common::AllocationUtil::AllocateAligned(size));
    std::memcpy(ret_st, entry->Content(), size);
    // add the null terminator
    *(ret_st + size - 1) = 0;
    delete[] read_buffer;
    return ret_st;
  }

  storage::BlockLayout *GetLayout() { return layout_; }
  /**
   * Save a string, for insertion by EndRowAndInsert
   * @param col_num column number in the schema
   * @param st C string to save.
   */
  void SetVarcharColInRow(catalog::col_oid_t col_oid, const char *st) {
    byte *col_p = pr_->AccessForceNotNull(pr_map_->at(col_oid));
    // string size, without null terminator
    auto size = static_cast<uint32_t>(strlen(st));
    if (size <= storage::VarlenEntry::InlineThreshold()) {
      *reinterpret_cast<storage::VarlenEntry *>(col_p) =
          storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *>(st), size);
    } else {
      byte *varlen = common::AllocationUtil::AllocateAligned(size);
      std::memcpy(varlen, st, static_cast<uint32_t>(size));
      *reinterpret_cast<storage::VarlenEntry *>(col_p) =
          storage::VarlenEntry::Create(varlen, static_cast<uint32_t>(size), true);
    }
  }

 public:
  // This is a public field that transactions can set and read.
  // The purpose is to record the version for each transaction. In reality this information should be retrieved from
  // catalog. We just make it for the test case.
  storage::layout_version_t version_;
  storage::SqlTable *table_ = nullptr;

 private:
  storage::BlockStore block_store_{100, 100};
  catalog::table_oid_t table_oid_;

  // keep meta-data for most recent schema
  catalog::Schema *schema_ = nullptr;
  std::vector<catalog::Schema::Column> cols_;
  std::vector<catalog::col_oid_t> col_oids_;
  storage::BlockLayout *layout_ = nullptr;

  storage::ProjectedRowInitializer *pri_ = nullptr;
  storage::ProjectionMap *pr_map_ = nullptr;

  byte *buffer_ = nullptr;
  storage::ProjectedRow *pr_ = nullptr;

  storage::layout_version_t next_version_ = storage::layout_version_t(0);

  void ResetProjectedRow(const std::vector<catalog::col_oid_t> &col_oids) {
    auto pr_pair = table_->InitializerForProjectedRow(col_oids, version_);
    buffer_ = common::AllocationUtil::AllocateAligned(pr_pair.first.ProjectedRowSize());
    pr_ = pr_pair.first.InitializeRow(buffer_);

    delete pri_;
    delete pr_map_;
    pri_ = new storage::ProjectedRowInitializer(std::get<0>(pr_pair));
    pr_map_ = new storage::ProjectionMap(std::get<1>(pr_pair));
  }
};

struct SqlTableTests : public TerrierTest {
  void SetUp() override { TerrierTest::SetUp(); }

  void TearDown() override { TerrierTest::TearDown(); }

  storage::RecordBufferSegmentPool buffer_pool_{100, 100};
  transaction::TransactionManager txn_manager_ = {&buffer_pool_, true, LOGGING_DISABLED};
};

// NOLINTNEXTLINE
TEST_F(SqlTableTests, SelectTest) {
  SqlTableTestRW table(catalog::table_oid_t(2));
  auto txn = txn_manager_.BeginTransaction();
  table.DefineColumn("id", type::TypeId::INTEGER, false, catalog::col_oid_t(0));
  table.DefineColumn("datname", type::TypeId::INTEGER, false, catalog::col_oid_t(1));
  table.Create();
  table.StartInsertRow();
  table.SetIntColInRow(catalog::col_oid_t(0), 100);
  table.SetIntColInRow(catalog::col_oid_t(1), 10000);
  storage::TupleSlot row1_slot = table.EndInsertRow(txn);

  table.StartInsertRow();
  table.SetIntColInRow(catalog::col_oid_t(0), 200);
  table.SetIntColInRow(catalog::col_oid_t(1), 10001);
  storage::TupleSlot row2_slot = table.EndInsertRow(txn);

  uint32_t id = table.GetIntColInRow(txn, catalog::col_oid_t(0), row1_slot);
  EXPECT_EQ(100, id);
  uint32_t datname = table.GetIntColInRow(txn, catalog::col_oid_t(1), row1_slot);
  EXPECT_EQ(10000, datname);

  // manually set the version of the transaction to be 1
  table.version_ = storage::layout_version_t(1);
  table.AddColumn(txn, "new_col", type::TypeId::INTEGER, true, catalog::col_oid_t(2));

  id = table.GetIntColInRow(txn, catalog::col_oid_t(0), row1_slot);
  EXPECT_EQ(100, id);
  datname = table.GetIntColInRow(txn, catalog::col_oid_t(1), row1_slot);
  EXPECT_EQ(10000, datname);

  id = table.GetIntColInRow(txn, catalog::col_oid_t(0), row2_slot);
  EXPECT_EQ(200, id);
  datname = table.GetIntColInRow(txn, catalog::col_oid_t(1), row2_slot);
  EXPECT_EQ(10001, datname);

  uint32_t new_col = table.GetIntColInRow(txn, catalog::col_oid_t(2), row1_slot);
  EXPECT_EQ(12345, new_col);

  txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
  delete txn;
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, InsertTest) {
  SqlTableTestRW table(catalog::table_oid_t(2));
  auto txn = txn_manager_.BeginTransaction();
  table.DefineColumn("id", type::TypeId::INTEGER, false, catalog::col_oid_t(0));
  table.DefineColumn("datname", type::TypeId::INTEGER, false, catalog::col_oid_t(1));
  table.Create();
  table.StartInsertRow();
  table.SetIntColInRow(catalog::col_oid_t(0), 100);
  table.SetIntColInRow(catalog::col_oid_t(1), 10000);
  storage::TupleSlot row1_slot = table.EndInsertRow(txn);

  uint32_t id = table.GetIntColInRow(txn, catalog::col_oid_t(0), row1_slot);
  EXPECT_EQ(100, id);
  uint32_t datname = table.GetIntColInRow(txn, catalog::col_oid_t(1), row1_slot);
  EXPECT_EQ(10000, datname);

  // manually set the version of the transaction to be 1
  table.version_ = storage::layout_version_t(1);
  table.AddColumn(txn, "new_col", type::TypeId::INTEGER, true, catalog::col_oid_t(2));

  // insert (300, 10002, null)
  table.StartInsertRow();
  table.SetIntColInRow(catalog::col_oid_t(0), 300);
  table.SetIntColInRow(catalog::col_oid_t(1), 10002);
  storage::TupleSlot row3_slot = table.EndInsertRow(txn);

  // insert (400, 10003, 42)
  table.StartInsertRow();
  table.SetIntColInRow(catalog::col_oid_t(0), 400);
  table.SetIntColInRow(catalog::col_oid_t(1), 10003);
  table.SetIntColInRow(catalog::col_oid_t(2), 42);
  storage::TupleSlot row4_slot = table.EndInsertRow(txn);

  id = table.GetIntColInRow(txn, catalog::col_oid_t(0), row3_slot);
  EXPECT_EQ(300, id);
  datname = table.GetIntColInRow(txn, catalog::col_oid_t(1), row3_slot);
  EXPECT_EQ(10002, datname);
  uint32_t new_col = table.GetIntColInRow(txn, catalog::col_oid_t(2), row3_slot);
  EXPECT_EQ(12345, new_col);

  id = table.GetIntColInRow(txn, catalog::col_oid_t(0), row4_slot);
  EXPECT_EQ(400, id);
  datname = table.GetIntColInRow(txn, catalog::col_oid_t(1), row4_slot);
  EXPECT_EQ(10003, datname);
  datname = table.GetIntColInRow(txn, catalog::col_oid_t(2), row4_slot);
  EXPECT_EQ(42, datname);

  txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
  delete txn;
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, DeleteTest) {
  SqlTableTestRW table(catalog::table_oid_t(2));
  auto txn = txn_manager_.BeginTransaction();
  table.DefineColumn("id", type::TypeId::INTEGER, false, catalog::col_oid_t(0));
  table.DefineColumn("datname", type::TypeId::INTEGER, false, catalog::col_oid_t(1));
  table.Create();

  // insert (100, 10000)
  table.StartInsertRow();
  table.SetIntColInRow(catalog::col_oid_t(0), 100);
  table.SetIntColInRow(catalog::col_oid_t(1), 10000);
  storage::TupleSlot row1_slot = table.EndInsertRow(txn);

  // insert (200, 10001)
  table.StartInsertRow();
  table.SetIntColInRow(catalog::col_oid_t(0), 100);
  table.SetIntColInRow(catalog::col_oid_t(1), 10001);
  storage::TupleSlot row2_slot = table.EndInsertRow(txn);

  // manually set the version of the transaction to be 1
  table.version_ = storage::layout_version_t(1);
  table.AddColumn(txn, "new_col", type::TypeId::INTEGER, true, catalog::col_oid_t(2));

  // insert (300, 10002, null)
  table.StartInsertRow();
  table.SetIntColInRow(catalog::col_oid_t(0), 300);
  table.SetIntColInRow(catalog::col_oid_t(1), 10002);
  storage::TupleSlot row3_slot = table.EndInsertRow(txn);

  // insert (400, 10003, 42)
  table.StartInsertRow();
  table.SetIntColInRow(catalog::col_oid_t(0), 400);
  table.SetIntColInRow(catalog::col_oid_t(1), 10003);
  table.SetIntColInRow(catalog::col_oid_t(2), 42);
  storage::TupleSlot row4_slot = table.EndInsertRow(txn);

  // delete (100, 10000, null) and (300, 10002, null)
  EXPECT_TRUE(table.table_->Delete(txn, row1_slot, table.version_));
  EXPECT_TRUE(table.table_->Delete(txn, row3_slot, table.version_));

  EXPECT_TRUE(table.Visible(txn, row2_slot));
  EXPECT_TRUE(table.Visible(txn, row4_slot));

  EXPECT_FALSE(table.Visible(txn, row1_slot));
  EXPECT_FALSE(table.Visible(txn, row3_slot));
  txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
  delete txn;
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, UpdateTest) {
  SqlTableTestRW table(catalog::table_oid_t(2));
  auto txn = txn_manager_.BeginTransaction();
  table.DefineColumn("id", type::TypeId::INTEGER, false, catalog::col_oid_t(0));
  table.DefineColumn("datname", type::TypeId::INTEGER, false, catalog::col_oid_t(1));
  table.Create();

  // insert (100, 10000)
  table.StartInsertRow();
  table.SetIntColInRow(catalog::col_oid_t(0), 100);
  table.SetIntColInRow(catalog::col_oid_t(1), 10000);
  storage::TupleSlot row1_slot = table.EndInsertRow(txn);

  // insert (200, 10001)
  table.StartInsertRow();
  table.SetIntColInRow(catalog::col_oid_t(0), 200);
  table.SetIntColInRow(catalog::col_oid_t(1), 10001);
  storage::TupleSlot row2_slot = table.EndInsertRow(txn);

  // manually set the version of the transaction to be 1
  table.version_ = storage::layout_version_t(1);
  table.AddColumn(txn, "new_col", type::TypeId::INTEGER, true, catalog::col_oid_t(2));

  // insert (300, 10002, null)
  table.StartInsertRow();
  table.SetIntColInRow(catalog::col_oid_t(0), 300);
  table.SetIntColInRow(catalog::col_oid_t(1), 10002);
  table.EndInsertRow(txn);

  // insert (400, 10003, 42)
  table.StartInsertRow();
  table.SetIntColInRow(catalog::col_oid_t(0), 400);
  table.SetIntColInRow(catalog::col_oid_t(1), 10003);
  table.SetIntColInRow(catalog::col_oid_t(2), 42);
  storage::TupleSlot row4_slot = table.EndInsertRow(txn);

  // update (200, 10001, null) -> (200, 11001, null)
  std::vector<catalog::col_oid_t> update_oids;
  update_oids.emplace_back(catalog::col_oid_t(1));
  table.StartUpdateRow(update_oids);
  table.SetIntColInRow(catalog::col_oid_t(1), 11001);
  auto result = table.EndUpdateRow(txn, row2_slot);

  EXPECT_TRUE(result.first);
  EXPECT_EQ(result.second.GetBlock(), row2_slot.GetBlock());
  EXPECT_EQ(result.second.GetOffset(), row2_slot.GetOffset());
  uint32_t new_val = table.GetIntColInRow(txn, catalog::col_oid_t(1), row2_slot);
  EXPECT_EQ(new_val, 11001);

  // update (400, 10003, 42) -> (400, 11003, 420)
  LOG_INFO("----------------------------")
  update_oids.clear();
  update_oids.emplace_back(catalog::col_oid_t(1));
  update_oids.emplace_back(catalog::col_oid_t(2));
  table.StartUpdateRow(update_oids);
  table.SetIntColInRow(catalog::col_oid_t(1), 11003);
  table.SetIntColInRow(catalog::col_oid_t(2), 420);
  result = table.EndUpdateRow(txn, row4_slot);

  EXPECT_TRUE(result.first);
  EXPECT_EQ(result.second.GetBlock(), row4_slot.GetBlock());
  EXPECT_EQ(result.second.GetOffset(), row4_slot.GetOffset());
  new_val = table.GetIntColInRow(txn, catalog::col_oid_t(1), result.second);
  EXPECT_EQ(new_val, 11003);
  new_val = table.GetIntColInRow(txn, catalog::col_oid_t(2), result.second);
  EXPECT_EQ(new_val, 420);

  // update (100, 10000, null) -> (100, 11000, 420)
  LOG_INFO("----------------------------")
  update_oids.clear();
  update_oids.emplace_back(catalog::col_oid_t(1));
  update_oids.emplace_back(catalog::col_oid_t(2));
  table.StartUpdateRow(update_oids);
  table.SetIntColInRow(catalog::col_oid_t(1), 11000);
  table.SetIntColInRow(catalog::col_oid_t(2), 420);
  result = table.EndUpdateRow(txn, row1_slot);

  EXPECT_TRUE(result.first);
  EXPECT_NE(result.second.GetBlock(), row1_slot.GetBlock());
  new_val = table.GetIntColInRow(txn, catalog::col_oid_t(1), result.second);
  EXPECT_EQ(new_val, 11000);
  new_val = table.GetIntColInRow(txn, catalog::col_oid_t(2), result.second);
  EXPECT_EQ(new_val, 420);

  txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
  delete txn;
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, ScanTest) {
  SqlTableTestRW table(catalog::table_oid_t(2));
  auto txn = txn_manager_.BeginTransaction();
  table.DefineColumn("id", type::TypeId::INTEGER, false, catalog::col_oid_t(0));
  table.DefineColumn("datname", type::TypeId::INTEGER, false, catalog::col_oid_t(1));
  table.Create();

  // insert (100, 10000)
  table.StartInsertRow();
  table.SetIntColInRow(catalog::col_oid_t(0), 100);
  table.SetIntColInRow(catalog::col_oid_t(1), 10000);
  table.EndInsertRow(txn);

  // insert (200, 10001)
  table.StartInsertRow();
  table.SetIntColInRow(catalog::col_oid_t(0), 200);
  table.SetIntColInRow(catalog::col_oid_t(1), 10001);
  table.EndInsertRow(txn);

  // manually set the version of the transaction to be 1
  table.version_ = storage::layout_version_t(1);
  table.AddColumn(txn, "new_col", type::TypeId::INTEGER, true, catalog::col_oid_t(2));

  // insert (300, 10002, null)
  table.StartInsertRow();
  table.SetIntColInRow(catalog::col_oid_t(0), 300);
  table.SetIntColInRow(catalog::col_oid_t(1), 10002);
  table.EndInsertRow(txn);

  // insert (400, 10003, 42)
  table.StartInsertRow();
  table.SetIntColInRow(catalog::col_oid_t(0), 400);
  table.SetIntColInRow(catalog::col_oid_t(1), 10003);
  table.SetIntColInRow(catalog::col_oid_t(2), 42);
  table.EndInsertRow(txn);

  // begin scan
  std::vector<catalog::col_oid_t> all_col_oids;
  all_col_oids.emplace_back(0);
  all_col_oids.emplace_back(1);
  all_col_oids.emplace_back(2);

  auto pc_pair = table.table_->InitializerForProjectedColumns(all_col_oids, 10, table.version_);
  byte *buffer = common::AllocationUtil::AllocateAligned(pc_pair.first.ProjectedColumnsSize());
  storage::ProjectedColumns *pc = pc_pair.first.Initialize(buffer);

  // scan
  auto start_pos = table.table_->begin();
  table.table_->Scan(txn, &start_pos, pc, pc_pair.second, table.version_);

  // check the number of tuples we found
  EXPECT_EQ(pc->NumTuples(), 2);

  // check the if we get (100, 10000, null)
  auto row1 = pc->InterpretAsRow(*table.GetLayout(), 0);
  byte *value = row1.AccessWithNullCheck(pc_pair.second.at(catalog::col_oid_t(0)));
  EXPECT_NE(value, nullptr);
  uint32_t id = *reinterpret_cast<uint32_t *>(value);
  EXPECT_EQ(id, 100);
  value = row1.AccessWithNullCheck(pc_pair.second.at(catalog::col_oid_t(1)));
  EXPECT_NE(value, nullptr);
  uint32_t datname = *reinterpret_cast<uint32_t *>(value);
  EXPECT_EQ(datname, 10000);

  // check the if we get (200, 10001, null)
  auto row2 = pc->InterpretAsRow(*table.GetLayout(), 1);
  value = row2.AccessWithNullCheck(pc_pair.second.at(catalog::col_oid_t(0)));
  EXPECT_NE(value, nullptr);
  id = *reinterpret_cast<uint32_t *>(value);
  EXPECT_EQ(id, 200);
  value = row2.AccessWithNullCheck(pc_pair.second.at(catalog::col_oid_t(1)));
  EXPECT_NE(value, nullptr);
  datname = *reinterpret_cast<uint32_t *>(value);
  EXPECT_EQ(datname, 10001);

  pc = pc_pair.first.Initialize(buffer);
  //Need to scan again to get the rest of the data
  table.table_->Scan(txn, &start_pos, pc, pc_pair.second, table.version_);
  // check the if we get (400, 10003, 42)
  auto row4 = pc->InterpretAsRow(*table.GetLayout(), 1);
  value = row4.AccessWithNullCheck(pc_pair.second.at(catalog::col_oid_t(0)));
  EXPECT_NE(value, nullptr);
  id = *reinterpret_cast<uint32_t *>(value);
  EXPECT_EQ(id, 400);
  value = row4.AccessWithNullCheck(pc_pair.second.at(catalog::col_oid_t(1)));
  EXPECT_NE(value, nullptr);
  datname = *reinterpret_cast<uint32_t *>(value);
  EXPECT_EQ(datname, 10003);
  value = row4.AccessWithNullCheck(pc_pair.second.at(catalog::col_oid_t(2)));
  EXPECT_NE(value, nullptr);
  uint32_t new_col = *reinterpret_cast<uint32_t *>(value);
  EXPECT_EQ(new_col, 42);

  delete[] buffer;
  txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
  delete txn;
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, VarlenInsertTest) {
  SqlTableTestRW table(catalog::table_oid_t(2));
  auto txn = txn_manager_.BeginTransaction();

  table.DefineColumn("id", type::TypeId::INTEGER, false, catalog::col_oid_t(0));
  table.DefineColumn("datname", type::TypeId::VARCHAR, false, catalog::col_oid_t(1));
  table.Create();

  table.StartInsertRow();
  table.SetIntColInRow(catalog::col_oid_t(0), 100);
  table.SetVarcharColInRow(catalog::col_oid_t(1), "name");
  storage::TupleSlot row_slot = table.EndInsertRow(txn);

  uint32_t id = table.GetIntColInRow(txn, catalog::col_oid_t(0), row_slot);
  EXPECT_EQ(100, id);
  char *table_name = table.GetVarcharColInRow(txn, catalog::col_oid_t(1), row_slot);
  EXPECT_STREQ("name", table_name);
  delete[] table_name;

  txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
  delete txn;
}

}  // namespace terrier
