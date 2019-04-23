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
class SqlTableRW {
 public:
  explicit SqlTableRW(catalog::table_oid_t table_oid) : table_oid_(table_oid) {}
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
    if (type != type::TypeId::VARCHAR && type != type::TypeId::VARBINARY)
      cols_.emplace_back(name, type, nullable, oid);
    else
      cols_.emplace_back(name, type, 255, nullable, oid);
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
  storage::TupleSlot EndRowAndInsert() {
    auto txn = txn_manager_.BeginTransaction();
    auto slot = table_->Insert(txn, *insert_);
    insert_ = nullptr;
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);

    delete[] insert_buffer_;
    delete txn;
    return storage::TupleSlot(slot.GetBlock(), slot.GetOffset());
  }

  /**
   * Read an integer from a row
   * @param col_num column number in the schema
   * @param slot - tuple to read from
   * @return integer value
   */
  uint32_t GetIntColInRow(int32_t col_num, storage::TupleSlot slot) {
    auto txn = txn_manager_.BeginTransaction();
    auto read_buffer = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    storage::ProjectedRow *read = pri_->InitializeRow(read_buffer);
    table_->Select(txn, slot, read);
    byte *col_p = read->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    auto ret_val = *(reinterpret_cast<uint32_t *>(col_p));

    delete txn;
    delete[] read_buffer;
    return ret_val;
  }

  /**
   * Read an uint64 from a row
   * @param col_num column number in the schema
   * @param slot - tuple to read from
   * @return integer value
   */
  uint64_t GetInt64ColInRow(int32_t col_num, storage::TupleSlot slot) {
    auto txn = txn_manager_.BeginTransaction();
    auto read_buffer = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    storage::ProjectedRow *read = pri_->InitializeRow(read_buffer);
    table_->Select(txn, slot, read);
    byte *col_p = read->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    auto ret_val = *(reinterpret_cast<uint64_t *>(col_p));

    delete txn;
    delete[] read_buffer;
    return ret_val;
  }

  /**
   * Read an uint16 from a row
   * @param col_num column number in the schema
   * @param slot - tuple to read from
   * @return integer value
   */
  uint16_t GetInt16ColInRow(int32_t col_num, storage::TupleSlot slot) {
    auto txn = txn_manager_.BeginTransaction();
    auto read_buffer = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    storage::ProjectedRow *read = pri_->InitializeRow(read_buffer);
    table_->Select(txn, slot, read);
    byte *col_p = read->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    auto ret_val = *(reinterpret_cast<uint16_t *>(col_p));

    delete txn;
    delete[] read_buffer;
    return ret_val;
  }

  /**
   * Read an uint8 from a row
   * @param col_num column number in the schema
   * @param slot - tuple to read from
   * @return integer value
   */
  uint8_t GetInt8ColInRow(int32_t col_num, storage::TupleSlot slot) {
    auto txn = txn_manager_.BeginTransaction();
    auto read_buffer = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    storage::ProjectedRow *read = pri_->InitializeRow(read_buffer);
    table_->Select(txn, slot, read);
    byte *col_p = read->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    auto ret_val = *(reinterpret_cast<uint8_t *>(col_p));

    delete txn;
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
   * Save an uint64, for insertion by EndRowAndInsert
   * @param col_num column number in the schema
   * @param value to save
   */
  void SetInt64ColInRow(int32_t col_num, uint64_t value) {
    byte *col_p = insert_->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));
    (*reinterpret_cast<uint64_t *>(col_p)) = value;
  }

  /**
   * Save an uint16, for insertion by EndRowAndInsert
   * @param col_num column number in the schema
   * @param value to save
   */
  void SetInt16ColInRow(int32_t col_num, uint16_t value) {
    byte *col_p = insert_->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));
    (*reinterpret_cast<uint16_t *>(col_p)) = value;
  }

  /**
   * Save an uint8, for insertion by EndRowAndInsert
   * @param col_num column number in the schema
   * @param value to save
   */
  void SetInt8ColInRow(int32_t col_num, uint8_t value) {
    byte *col_p = insert_->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));
    (*reinterpret_cast<uint8_t *>(col_p)) = value;
  }

  /**
   * Read a string from a row
   * @param col_num column number in the schema
   * @param slot - tuple to read from
   * @return malloc'ed C string (with null terminator). Caller must
   *   free.
   */
  char *GetVarcharColInRow(int32_t col_num, storage::TupleSlot slot) {
    auto txn = txn_manager_.BeginTransaction();
    auto read_buffer = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    storage::ProjectedRow *read = pri_->InitializeRow(read_buffer);
    table_->Select(txn, slot, read);
    byte *col_p = read->AccessForceNotNull(pr_map_->at(col_oids_[col_num]));

    auto *entry = reinterpret_cast<storage::VarlenEntry *>(col_p);
    // stored string has no null terminator, add space for it
    uint32_t size = entry->Size() + 1;
    // allocate return string
    auto *ret_st = reinterpret_cast<char *>(common::AllocationUtil::AllocateAligned(size));
    std::memcpy(ret_st, entry->Content(), size);
    // add the null terminator
    *(ret_st + size - 1) = 0;
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
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

 private:
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};
  transaction::TransactionManager txn_manager_ = {&buffer_pool_, true, LOGGING_DISABLED};

  storage::BlockStore block_store_{100, 100};
  catalog::table_oid_t table_oid_;
  storage::SqlTable *table_ = nullptr;

  catalog::Schema *schema_ = nullptr;
  std::vector<catalog::Schema::Column> cols_;
  std::vector<catalog::col_oid_t> col_oids_;

  storage::ProjectedRowInitializer *pri_ = nullptr;
  storage::ProjectionMap *pr_map_ = nullptr;

  byte *insert_buffer_ = nullptr;
  storage::ProjectedRow *insert_ = nullptr;
};

struct SqlTableTests : public TerrierTest {
  void SetUp() override { TerrierTest::SetUp(); }

  void TearDown() override { TerrierTest::TearDown(); }
};

// NOLINTNEXTLINE
TEST_F(SqlTableTests, SelectInsertTest) {
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
TEST_F(SqlTableTests, VarlenInsertTest) {
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
  delete[] table_name;
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, MultipleColumnWidths) {
  SqlTableRW table(catalog::table_oid_t(2));

  table.DefineColumn("bigint", type::TypeId::BIGINT, false, catalog::col_oid_t(1001));
  table.DefineColumn("integer", type::TypeId::INTEGER, false, catalog::col_oid_t(1002));
  table.DefineColumn("smallint", type::TypeId::SMALLINT, false, catalog::col_oid_t(1003));
  table.DefineColumn("tinyint", type::TypeId::TINYINT, false, catalog::col_oid_t(1004));
  table.Create();

  table.StartRow();
  table.SetInt64ColInRow(0, 10000000000);
  table.SetIntColInRow(1, 100000);
  table.SetInt16ColInRow(2, 512);
  table.SetInt8ColInRow(3, 42);
  storage::TupleSlot row_slot = table.EndRowAndInsert();

  // Check data
  uint64_t bigint = table.GetInt64ColInRow(0, row_slot);
  uint32_t integer = table.GetIntColInRow(1, row_slot);
  uint16_t smallint = table.GetInt16ColInRow(2, row_slot);
  uint8_t tinyint = table.GetInt8ColInRow(3, row_slot);

  EXPECT_EQ(bigint, 10000000000);
  EXPECT_EQ(integer, 100000);
  EXPECT_EQ(smallint, 512);
  EXPECT_EQ(tinyint, 42);
}
}  // namespace terrier
