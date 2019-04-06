#include <string>
#include <random>
#include "common/object_pool.h"
#include "storage/storage_util.h"
#include "storage/sql_table.h"
#include "storage/checkpoint_manager.h"
#include "util/storage_test_util.h"
#include "util/transaction_test_util.h"
#include "util/random_test_util.h"


#define CHECKPOINT_FILE_PREFIX "checkpoint_"

namespace terrier {

class RandomSqlTableTestObject {
 public:
  explicit RandomSqlTableTestObject() {}
  ~RandomSqlTableTestObject() {
    delete pri_;
    delete pr_map_;
    delete schema_;
    delete table_;
  }

  /**
   * Generate random columns, and add them sequencially to the internal list.
   * @param num_cols number of columns to add.
   * @param varlen_allowed allow VARCHAR to be used in columns.
   * @param generator of random numbers.
   */
  template <class Random>
  void GenerateRandomColumns(int num_cols, bool varlen_allowed, Random *generator) {
    std::string prefix = "col_";
    std::vector<type::TypeId> types = DataTypeAll(varlen_allowed);
    for (int i = 0; i < num_cols; i++) {
      type::TypeId type = *RandomTestUtil::UniformRandomElement(&types, generator);
      DefineColumn(prefix + std::to_string(uint8_t(type)), type, true, catalog::col_oid_t(i));
    }
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

  template<class Random>
  void InsertRandomRow(transaction::TransactionContext *txn, const double null_bias,
          Random *generator) {
    std::bernoulli_distribution coin(1 - null_bias);
    std::uniform_int_distribution<uint32_t> varlen_size(1, 2 * storage::VarlenEntry::InlineThreshold());
    std::uniform_int_distribution<uint8_t> char_dist(0, UINT8_MAX);
    std::uniform_int_distribution<int32_t> int_dist(INT32_MIN, INT32_MAX);

    auto insert_buffer = common::AllocationUtil::AllocateAligned(pri_->ProjectedRowSize());
    auto insert = pri_->InitializeRow(insert_buffer);

    for (int i = 0; i < cols_.size(); i++) {
      if (coin(*generator)) { // not null
        uint16_t offset = pr_map_->at(col_oids_[i]);
        insert->SetNotNull(offset);
        auto col = cols_[i];
        byte *col_p = insert->AccessForceNotNull(offset);
        uint32_t size;
        switch (col.GetType()) {
          case type::TypeId::INTEGER:
            (*reinterpret_cast<uint32_t *>(col_p)) = int_dist(*generator);
            break;
          case type::TypeId::VARCHAR:
            size = varlen_size(*generator);
            if (size > storage::VarlenEntry::InlineThreshold()) {
              byte *varlen = common::AllocationUtil::AllocateAligned(size);
              StorageTestUtil::FillWithRandomBytes(size, varlen, generator);
              // varlen entries always start off not inlined
              *reinterpret_cast<storage::VarlenEntry *>(col_p) =
                      storage::VarlenEntry::Create(varlen, size, true);
            } else {
              byte buf[storage::VarlenEntry::InlineThreshold()];
              StorageTestUtil::FillWithRandomBytes(size, buf, generator);
              *reinterpret_cast<storage::VarlenEntry *>(col_p) =
                      storage::VarlenEntry::CreateInline(buf, size);
            }
            break;
          default:
            break;
        }
      } else { // null
        insert->SetNull(pr_map_->at(col_oids_[i]));
      }
    }

    table_->Insert(txn, *insert);
    delete[] insert_buffer;
  }

  template<class Random>
  void InsertRandomRows(const int num_rows, const double null_bias, Random *generator) {
    auto txn = txn_manager_.BeginTransaction();
    for (int i = 0; i < num_rows; i++) {
      InsertRandomRow(txn, null_bias, generator);
    }
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
  }

  storage::SqlTable *GetTable() {
    return table_;
  }

  storage::BlockLayout GetLayout() {
    auto layout = storage::StorageUtil::BlockLayoutFromSchema(*schema_).first;
    return layout;
  }

  transaction::TransactionManager *GetTxnManager() {
    return &txn_manager_;
  }

 private:

  static std::vector<type::TypeId> DataTypeAll(bool varlen_allowed) {
    if (varlen_allowed)
      return {type::TypeId::INTEGER, type::TypeId::VARCHAR};
    else
      return {type::TypeId::INTEGER};
  }

  storage::RecordBufferSegmentPool buffer_pool_{100, 100};
  transaction::TransactionManager txn_manager_ = {&buffer_pool_, true, LOGGING_DISABLED};

  storage::BlockStore block_store_{100, 100};
  catalog::table_oid_t table_oid_{2};
  storage::SqlTable *table_ = nullptr;

  catalog::Schema *schema_ = nullptr;
  std::vector<catalog::Schema::Column> cols_;
  std::vector<catalog::col_oid_t> col_oids_;

  storage::ProjectedRowInitializer *pri_ = nullptr;
  storage::ProjectionMap *pr_map_ = nullptr;
};

struct CheckpointTests : public TerrierTest {
  storage::BlockStore block_store_{100, 100};
  storage::RecordBufferSegmentPool buffer_pool_{100000, 10000};
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};
};

TEST_F(CheckpointTests, SimpleCheckpointNoVarlen) {
   const uint32_t num_rows = 1000;
  const uint32_t num_columns = 10;
//  const uint32_t initial_table_size = 100;
  const uint32_t checkpoint_buffer_size = 10000;

  int magic_seed = 13523;

  auto tested = RandomSqlTableTestObject();
  std::default_random_engine random_generator(magic_seed);
  tested.GenerateRandomColumns(num_columns, true, &random_generator);
  tested.Create();
  tested.InsertRandomRows(num_rows, 0.2, &generator_);

  storage::CheckpointManager manager(CHECKPOINT_FILE_PREFIX, checkpoint_buffer_size);
  storage::SqlTable *table = tested.GetTable();
  transaction::TransactionManager *txn_manager = tested.GetTxnManager();

  transaction::TransactionContext *txn = txn_manager->BeginTransaction();
  manager.StartCheckpoint(txn);
  manager.Checkpoint(*table, tested.GetLayout());
  manager.EndCheckpoint();

}

}  // namespace terrier