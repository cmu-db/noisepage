#include "storage/sql_table.h"

#include <cstring>
#include <vector>

#include "common/container/concurrent_map.h"
#include "tbb/concurrent_hash_map.h"

#include "storage/storage_util.h"
#include "test_util/multithread_test_util.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_context.h"

namespace terrier::storage {

class SqlTableTests : public TerrierTest {
 public:
  storage::BlockStore block_store_{100, 100};
  storage::RecordBufferSegmentPool buffer_pool_{100000, 10000};
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};
  void SetUp() override {}
  void TearDown() override {}
};

// given new columns without oids, add them to the end, and assign them oids larger than existing oids
static std::unique_ptr<catalog::Schema> AddColumnsToEnd(const catalog::Schema &schema,
                                                        const std::vector<catalog::Schema::Column *> &new_columns) {
  std::vector<catalog::Schema::Column> columns(schema.GetColumns());

  catalog::col_oid_t max_oid = columns.begin()->Oid();
  for (auto &col : columns) {
    if (col.Oid() > max_oid) {
      max_oid = col.Oid();
    }
  }

  // add the new columns, set their oids to be larger than all existing oids
  for (size_t i = 0; i < new_columns.size(); i++) {
    catalog::col_oid_t new_oid = max_oid + 1 + i;
    StorageTestUtil::SetOid(new_columns[i], new_oid);
    columns.push_back(*new_columns[i]);
  }
  return std::make_unique<catalog::Schema>(columns);
}

bool OidsUnique(const std::vector<catalog::Schema::Column> &columns) {
  std::unordered_set<catalog::col_oid_t> oids;
  for (const auto &col : columns) {
    oids.insert(col.Oid());
  }
  return oids.size() == columns.size();
}

// given new columns with oids, add them to the column vector so that the column vector is still sorted
static std::unique_ptr<catalog::Schema> AddColumns(const catalog::Schema &schema,
                                                   const std::vector<catalog::Schema::Column> &new_columns) {
  std::vector<catalog::Schema::Column> columns(schema.GetColumns());

  for (const auto &new_col : new_columns) {
    columns.push_back(new_col);
  }

  struct {
    bool operator()(const catalog::Schema::Column &a, const catalog::Schema::Column &b) const {
      return a.Oid() < b.Oid();
    }
  } column_less;
  std::sort(columns.begin(), columns.end(), column_less);

  TERRIER_ASSERT(OidsUnique(columns), "New column oids should not conflict with existing ones");
  return std::make_unique<catalog::Schema>(columns);
}

// drop the columns with oids in the set drop_oids
static std::unique_ptr<catalog::Schema> DropColumns(const catalog::Schema &schema,
                                                    const std::unordered_set<catalog::col_oid_t> &drop_oids) {
  auto old_columns = schema.GetColumns();
  std::vector<catalog::Schema::Column> columns;

  for (auto &col : old_columns) {
    // only include cols that are not dropped
    if (drop_oids.find(col.Oid()) == drop_oids.end()) {
      columns.push_back(col);
    }
  }
  TERRIER_ASSERT(old_columns.size() == columns.size() + drop_oids.size(), "all drop oids should exist");

  return std::make_unique<catalog::Schema>(columns);
}

class RandomSqlTableTestObject {
 public:
  struct TupleVersion {
    transaction::timestamp_t ts_;
    storage::ProjectedRow *pr_;
    storage::layout_version_t version_;
  };

  template <class Random>
  RandomSqlTableTestObject(storage::BlockStore *block_store, const uint16_t max_col, Random *generator,
                           double null_bias)
      : null_bias_(null_bias) {
    auto schema = StorageTestUtil::RandomSchemaNoVarlen(max_col, generator);
    table_ = std::make_unique<storage::SqlTable>(common::ManagedPointer<storage::BlockStore>(block_store), *schema);
    UpdateSchema({nullptr}, std::unique_ptr<catalog::Schema>(schema), storage::layout_version_t(0));
  }

  // TODO(Schema-Change): redos_ seems to be producing problem here.
  //  We should look into this function further
  ~RandomSqlTableTestObject() {
    for (auto &it : buffers_) {
      delete[] it.second;
    }
    for (auto &txn : txns_) {
      txn->redo_buffer_.Finalize(false);
    }
  }

  template <class Random>
  void FillNullValue(storage::ProjectedRow *pr, const catalog::Schema &schema, const ColumnIdToOidMap &col_id_to_oid,
                     const storage::BlockLayout &layout, Random *const generator) {
    // Make sure we have a mix of inlined and non-inlined values
    std::uniform_int_distribution<uint32_t> varlen_size(1, MAX_TEST_VARLEN_SIZE);

    for (uint16_t pr_idx = 0; pr_idx < pr->NumColumns(); pr_idx++) {
      auto col_id = pr->ColumnIds()[pr_idx];
      auto col_oid = col_id_to_oid.at(col_id);
      const auto &schema_col = schema.GetColumn(col_oid);

      if (pr->IsNull(pr_idx) && !schema_col.Nullable()) {
        // Ricky (Schema-Change):
        // Some non-nullable columns could be null. But the schema we generated have default values all being null, we
        // just need to fill some random values here. Ideally we want to do this while we populate the projectedrow,
        // but that function has coupled with too many other parts

        if (layout.IsVarlen(col_id)) {
          uint32_t size = varlen_size(*generator);
          if (size > storage::VarlenEntry::InlineThreshold()) {
            byte *varlen = common::AllocationUtil::AllocateAligned(size);
            StorageTestUtil::FillWithRandomBytes(size, varlen, generator);
            // varlen entries always start off not inlined
            *reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(pr_idx)) =
                storage::VarlenEntry::Create(varlen, size, true);
          } else {
            byte buf[storage::VarlenEntry::InlineThreshold()];
            StorageTestUtil::FillWithRandomBytes(size, buf, generator);
            *reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(pr_idx)) =
                storage::VarlenEntry::CreateInline(buf, size);
          }
        } else {
          StorageTestUtil::FillWithRandomBytes(layout.AttrSize(col_id), pr->AccessForceNotNull(pr_idx), generator);
        }
      }
    }
  }

  template <class Random>
  storage::TupleSlot InsertRandomTuple(const transaction::timestamp_t timestamp, Random *generator,
                                       storage::RecordBufferSegmentPool *buffer_pool,
                                       storage::layout_version_t layout_version,
                                       int slot_location = -1) {
    // generate a txn with an UndoRecord to populate on Insert
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);

    if (slot_location == -1) {
      txns_.emplace_back(txn);
    } else {
      txns_[slot_location] = std::unique_ptr<transaction::TransactionContext>(txn);
    }

    // generate a random ProjectedRow to Insert
    auto redo_initializer = pris_.at(layout_version);
    auto *insert_redo = txn->StageWrite(catalog::db_oid_t{0}, catalog::table_oid_t{0}, redo_initializer);
    auto *insert_tuple = insert_redo->Delta();
    auto layout = table_->GetBlockLayout(layout_version);
    StorageTestUtil::PopulateRandomRow(insert_tuple, layout, null_bias_, generator);

    // Fill up the random bytes for non-nullable columns
    FillNullValue(insert_tuple, GetSchema(layout_version), table_->GetColumnIdToOidMap(layout_version),
                  table_->GetBlockLayout(layout_version), generator);

    if (slot_location == -1) {
      redos_.emplace_back(insert_redo);
    } else {
      redos_[slot_location] = common::ManagedPointer<storage::RedoRecord>(insert_redo);
    }

    storage::TupleSlot slot = table_->Insert(common::ManagedPointer(txn), insert_redo, layout_version);
    if (slot_location == -1) {
      inserted_slots_.push_back(slot);
    } else {
      inserted_slots_[slot_location] = slot;
    }
    tuple_versions_accessor_ accessor;
    tuple_versions_.insert(accessor, slot);
    accessor->second.push_back({timestamp, insert_tuple, layout_version});

    return slot;
  }

  template <class Random>
  TupleSlot UpdateTuple(const storage::TupleSlot slot, const transaction::timestamp_t timestamp, Random *generator,
                        storage::RecordBufferSegmentPool *buffer_pool, storage::layout_version_t layout_version,
                        int slot_location = -1) {
    // generate a txn with an UndoRecord to populate on Insert
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
    if (slot_location == -1) {
      txns_.emplace_back(txn);
    } else {
      txns_[slot_location] = std::unique_ptr<transaction::TransactionContext>(txn);
    }

    // generate a random ProjectedRow to Insert
    auto redo_initializer = pris_.at(layout_version);
    auto *update_redo = txn->StageWrite(catalog::db_oid_t{0}, catalog::table_oid_t{0}, redo_initializer);
    update_redo->SetTupleSlot(slot);
    auto *update_tuple = update_redo->Delta();
    auto layout = table_->GetBlockLayout(layout_version);
    StorageTestUtil::PopulateRandomRow(update_tuple, layout, null_bias_, generator);

    // Fill up the random bytes for non-nullable columns
    FillNullValue(update_tuple, GetSchema(layout_version), table_->GetColumnIdToOidMap(layout_version),
                  table_->GetBlockLayout(layout_version), generator);

    if (slot_location == -1) {
      redos_.emplace_back(update_redo);
    } else {
      redos_[slot_location] = common::ManagedPointer<storage::RedoRecord>(update_redo);
    }
    storage::TupleSlot updated_slot;
    bool res = table_->Update(common::ManagedPointer(txn), update_redo, layout_version, &updated_slot);

    EXPECT_TRUE(res == true);
    if (!res) {
      return TupleSlot();
    }

    tuple_versions_accessor_ accessor;
    tuple_versions_.insert(accessor, slot);
    accessor->second.push_back({timestamp, update_tuple, layout_version});

    updated_slots_.push_back(updated_slot);

    return updated_slot;
  }

  storage::ProjectedColumns *AllocateColumnBuffer(const storage::layout_version_t version, byte **bufferp,
                                                  size_t size) {
    auto old_layout = GetBlockLayout(version);
    storage::ProjectedColumnsInitializer initializer(old_layout, StorageTestUtil::ProjectionListAllColumns(old_layout),
                                                     size);
    auto *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedColumnsSize());
    storage::ProjectedColumns *columns = initializer.Initialize(buffer);
    *bufferp = buffer;
    return columns;
  }

  TupleVersion GetReferenceVersionedTuple(const storage::TupleSlot slot, const transaction::timestamp_t timestamp) {
    tuple_versions_accessor_ accessor;
    const auto found = tuple_versions_.find(accessor, slot);
    TERRIER_ASSERT(found, "slot should exist in tuple_versions_");
    auto &versions = accessor->second;

    // search backwards so the first entry with smaller timestamp can be returned
    for (auto i = static_cast<int64_t>(versions.size() - 1); i >= 0; i--) {
      if (transaction::TransactionUtil::NewerThan(timestamp, versions[i].ts_) || timestamp == versions[i].ts_)
        return versions[i];
    }
    return {transaction::timestamp_t{transaction::INVALID_TXN_TIMESTAMP}, nullptr, storage::layout_version_t{0}};
  }

  storage::ProjectedRow *Select(const storage::TupleSlot slot, const transaction::timestamp_t timestamp,
                                storage::RecordBufferSegmentPool *buffer_pool,
                                storage::layout_version_t layout_version, int slot_location = -1) {
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
    if (slot_location == -1) {
      txns_.emplace_back(txn);
    } else {
      txns_[slot_location] = std::unique_ptr<transaction::TransactionContext>(txn);
    }

    // generate a redo ProjectedRow for Select
    storage::ProjectedRow *select_row = pris_.at(layout_version).InitializeRow(buffers_.at(layout_version));
    table_->Select(common::ManagedPointer(txn), slot, select_row, layout_version);
    return select_row;
  }

  bool Delete(const storage::TupleSlot slot, const transaction::timestamp_t timestamp,
              storage::RecordBufferSegmentPool *buffer_pool, int slot_location = -1) {
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
    if (slot_location == -1) {
      txns_.emplace_back(txn);
    } else {
      txns_[slot_location] = std::unique_ptr<transaction::TransactionContext>(txn);
    }
    txn->StageDelete(catalog::db_oid_t{0}, catalog::table_oid_t{0}, slot);

    return table_->Delete(common::ManagedPointer(txn), slot);
  }

  bool UpdateSchema(common::ManagedPointer<transaction::TransactionContext> txn,
                    std::unique_ptr<catalog::Schema> schema, const storage::layout_version_t layout_version) {
    if (txn != nullptr) {
      if (!table_->UpdateSchema(txn, *schema, layout_version)) return false;
    }

    auto columns = schema->GetColumns();
    std::vector<catalog::col_oid_t> oids;
    oids.reserve(columns.size());
    for (auto &col : columns) {
      oids.push_back(col.Oid());
    }
    auto pri = table_->InitializerForProjectedRow(oids, layout_version);
    schemas_.insert(std::make_pair(layout_version, std::unique_ptr<catalog::Schema>(std::move(schema))));
    pris_.insert(std::make_pair(layout_version, pri));
    buffers_.insert(std::make_pair(layout_version, common::AllocationUtil::AllocateAligned(pri.ProjectedRowSize())));
    return true;
  }

  common::ManagedPointer<transaction::TransactionContext> NewTransaction(
      transaction::timestamp_t timestamp, storage::RecordBufferSegmentPool *buffer_pool) {
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
    txns_.emplace_back(txn);
    return common::ManagedPointer<transaction::TransactionContext>(txn);
  }

  const std::vector<storage::TupleSlot> &InsertedTuples() const { return inserted_slots_; }
  const std::vector<storage::TupleSlot> &UpdatedTuples() const { return updated_slots_; }

  void ResizeInsertedSlots(int size) { inserted_slots_.resize(size); }
  void ResizeRedos(int size) { redos_.resize(size); }
  void ResizeTxns(int size) { txns_.resize(size); }

  storage::BlockLayout GetBlockLayout(storage::layout_version_t version) const {
    return table_->GetBlockLayout(version);
  }

  const catalog::Schema &GetSchema(storage::layout_version_t version) const { return *schemas_.at(version); }
  const storage::SqlTable &GetTable() const { return *table_; }
  storage::SqlTable &GetTable() { return *table_; }

  storage::ProjectionMap GetProjectionMapForOids(storage::layout_version_t version) {
    auto &schema = schemas_.at(version);
    auto columns = schema->GetColumns();
    std::vector<catalog::col_oid_t> oids;
    oids.reserve(columns.size());
    for (auto &col : columns) oids.push_back(col.Oid());
    return table_->ProjectionMapForOids(oids, version);
  }

  // Structure that defines hashing and comparison operations for user's type.
  struct SlotHash {
    static size_t hash( const storage::TupleSlot& slot ) {
      return std::hash<storage::TupleSlot>{}(slot);
    }
    //! True if strings are equal
    static bool equal( const storage::TupleSlot& slot1, const storage::TupleSlot& slot2 ) {
      return slot1==slot2;
    }
  };

 private:
  std::unique_ptr<storage::SqlTable> table_;
  double null_bias_;
  std::unordered_map<storage::layout_version_t, storage::ProjectedRowInitializer> pris_;
  std::unordered_map<storage::layout_version_t, byte *> buffers_;
  std::vector<common::ManagedPointer<storage::RedoRecord>> redos_;
  std::vector<std::unique_ptr<transaction::TransactionContext>> txns_;
  std::vector<storage::TupleSlot> inserted_slots_;
  std::vector<storage::TupleSlot> updated_slots_;
  // oldest to newest
  //std::unordered_map<storage::TupleSlot, std::vector<TupleVersion>> tuple_versions_;
  tbb::concurrent_hash_map<storage::TupleSlot, std::vector<TupleVersion>, SlotHash> tuple_versions_;
  typedef tbb::concurrent_hash_map<storage::TupleSlot, std::vector<TupleVersion>, SlotHash>::accessor tuple_versions_accessor_;

  std::unordered_map<storage::layout_version_t, std::unique_ptr<catalog::Schema>> schemas_;
};

// NOLINTNEXTLINE
TEST_F(SqlTableTests, SimpleInsertSelect) {
  const uint16_t max_columns = 20;
  const uint32_t num_inserts = 100;

  storage::layout_version_t version(0);

  // Insert into SqlTable
  RandomSqlTableTestObject test_table(&block_store_, max_columns, &generator_, null_ratio_(generator_));
  for (uint16_t i = 0; i < num_inserts; i++) {
    test_table.InsertRandomTuple(transaction::timestamp_t(0), &generator_, &buffer_pool_, version);
  }

  EXPECT_EQ(num_inserts, test_table.InsertedTuples().size());

  // Compare each inserted
  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    storage::ProjectedRow *stored =
        test_table.Select(inserted_tuple, transaction::timestamp_t(1), &buffer_pool_, version);
    auto ref = test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(1));

    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(ref.version_), ref.pr_, test_table.GetProjectionMapForOids(ref.version_),
        test_table.GetBlockLayout(version), stored, test_table.GetProjectionMapForOids(version), {}, {}));
  }
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, ConcurrentInsertSelect) {
  const uint16_t max_columns = 20;
  const uint32_t num_inserts_per_thread = 1;

  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});
  const uint32_t total_inserts = num_threads * num_inserts_per_thread;

  storage::layout_version_t version(0);

  RandomSqlTableTestObject test_table(&block_store_, max_columns, &generator_, null_ratio_(generator_));

  test_table.ResizeInsertedSlots(total_inserts);
  test_table.ResizeTxns(total_inserts * 2);
  test_table.ResizeRedos(total_inserts);

  // Each thread inserts into SqlTable concurrently
  auto workload = [&](uint32_t thread_id) {
    for (int i = 0; i < num_inserts_per_thread; i++) {
      int next_location = thread_id * num_inserts_per_thread + i;
      test_table.InsertRandomTuple(transaction::timestamp_t(0), &generator_, &buffer_pool_, version, next_location);
    }
  };

  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
  EXPECT_EQ(total_inserts, test_table.InsertedTuples().size());

  // Each thread selects into SqlTable concurrently, and compares the selected result with expected
  //auto workload2 = [&](uint32_t thread_id) {
  for (int thread_id = 0; thread_id < num_threads; thread_id++) {
    for (int i = 0; i < num_inserts_per_thread; i++) {
      int next_location = thread_id * num_inserts_per_thread + i;
      const auto &inserted_tuple = test_table.InsertedTuples()[next_location];
      storage::ProjectedRow *stored = test_table.Select(inserted_tuple, transaction::timestamp_t(1),
                                                        &buffer_pool_, version, total_inserts + next_location);
      auto ref = test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(1));
      EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
          test_table.GetBlockLayout(ref.version_), ref.pr_, test_table.GetProjectionMapForOids(ref.version_),
          test_table.GetBlockLayout(version), stored, test_table.GetProjectionMapForOids(version), {}, {}));
    }
  };
  //MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload2);
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, ConcurrentInsertWithSchemaChange) {
  const uint16_t max_columns = 20;
  uint64_t txn_ts = 0;

  uint32_t num_inserts_per_thread = 10;
  const uint32_t machine_threads = MultiThreadTestUtil::HardwareConcurrency();
  const uint32_t num_threads = machine_threads % 2 == 0 ? machine_threads: machine_threads - 1;
  common::WorkerPool thread_pool(num_threads, {});
  const uint32_t num_inserts = num_threads * num_inserts_per_thread;

  storage::layout_version_t version(0);

  RandomSqlTableTestObject test_table(&block_store_, max_columns, &generator_, null_ratio_(generator_));
  test_table.ResizeInsertedSlots(num_inserts/2);
  test_table.ResizeTxns(num_inserts/2);
  test_table.ResizeRedos(num_inserts/2);

  // Insert first half into SqlTable
  auto workload = [&](uint32_t thread_id) {
    for (int i = 0; i < num_inserts_per_thread; i++) {
      int next_location = thread_id * num_inserts_per_thread + i;
      test_table.InsertRandomTuple(transaction::timestamp_t(txn_ts), &generator_, &buffer_pool_, version, next_location);
    }
  };

  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads / 2, workload);

  EXPECT_EQ(num_inserts / 2, test_table.InsertedTuples().size());

  // Schema Update: drop the first column, and add 2 new columns to the end
  storage::layout_version_t new_version(1);
  txn_ts++;
  catalog::Schema::Column col1("new_col1", type::TypeId::INTEGER, false,
                               parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(1)));
  catalog::Schema::Column col2("new_col2", type::TypeId::INTEGER, false,
                               parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(2)));
  std::vector<catalog::Schema::Column *> cols{&col1, &col2};

  catalog::Schema::Column col_to_drop = test_table.GetSchema(version).GetColumns()[0];
  std::unordered_set<catalog::col_oid_t> drop_oids{col_to_drop.Oid()};
  std::unique_ptr<catalog::Schema> temp_schema = DropColumns(test_table.GetSchema(version), drop_oids);
  auto new_schema = AddColumnsToEnd(*temp_schema, cols);

  int update_schema_ts = txn_ts;
  txn_ts++;

  test_table.ResizeInsertedSlots(num_inserts);
  test_table.ResizeTxns(num_inserts);
  test_table.ResizeRedos(num_inserts);

  // Concurrently insert the second half with new version. Also concurrently update schema using smaller txn timestamp
  auto workload2 = [&](uint32_t thread_id) {
    if (thread_id == num_threads / 2) {
      test_table.UpdateSchema(test_table.NewTransaction(transaction::timestamp_t{update_schema_ts}, &buffer_pool_),
                              std::move(new_schema), new_version);
    } else {
      for (int i = 0; i < num_inserts_per_thread; i++) {
        int next_location = num_inserts / 2 + thread_id * num_inserts_per_thread + i;
        test_table.InsertRandomTuple(transaction::timestamp_t(txn_ts), &generator_, &buffer_pool_, version, next_location);
      }
    }
  };

  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads / 2 + 1, workload2);

  EXPECT_EQ(num_inserts, test_table.InsertedTuples().size());
  // Compare each inserted by selecting as the new version
  txn_ts++;
  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    storage::ProjectedRow *stored =
        test_table.Select(inserted_tuple, transaction::timestamp_t(txn_ts), &buffer_pool_, new_version);
    auto tuple_version = test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(txn_ts));
    std::unordered_set<catalog::col_oid_t> add_cols;
    std::unordered_set<catalog::col_oid_t> drop_cols;
    if (tuple_version.version_ != new_version) {
      for (auto col : cols) {
        add_cols.insert(col->Oid());
      }
      drop_cols.insert(col_to_drop.Oid());
    }
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(tuple_version.version_), tuple_version.pr_,
        test_table.GetProjectionMapForOids(tuple_version.version_), test_table.GetBlockLayout(new_version), stored,
        test_table.GetProjectionMapForOids(new_version), add_cols, drop_cols));
  }
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, InsertWithSchemaChange) {
  const uint16_t max_columns = 20;
  const uint32_t num_inserts = 8;
  uint64_t txn_ts = 0;

  storage::layout_version_t version(0);

  // Insert first half into SqlTable
  RandomSqlTableTestObject test_table(&block_store_, max_columns, &generator_, null_ratio_(generator_));
  for (uint16_t i = 0; i < num_inserts / 2; i++) {
    test_table.InsertRandomTuple(transaction::timestamp_t(txn_ts), &generator_, &buffer_pool_, version);
  }

  EXPECT_EQ(num_inserts / 2, test_table.InsertedTuples().size());

  // Schema Update: drop the first column, and add 2 new columns to the end
  storage::layout_version_t new_version(1);
  txn_ts++;
  catalog::Schema::Column col1("new_col1", type::TypeId::INTEGER, false,
                               parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(1)));
  catalog::Schema::Column col2("new_col2", type::TypeId::INTEGER, false,
                               parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(2)));
  std::vector<catalog::Schema::Column *> cols{&col1, &col2};

  catalog::Schema::Column col_to_drop = test_table.GetSchema(version).GetColumns()[0];
  std::unordered_set<catalog::col_oid_t> drop_oids{col_to_drop.Oid()};
  std::unique_ptr<catalog::Schema> temp_schema = DropColumns(test_table.GetSchema(version), drop_oids);
  auto new_schema = AddColumnsToEnd(*temp_schema, cols);

  test_table.UpdateSchema(test_table.NewTransaction(transaction::timestamp_t{txn_ts}, &buffer_pool_),
                          std::move(new_schema), new_version);

  // Insert the second half with new version
  txn_ts++;
  for (uint16_t i = num_inserts / 2; i < num_inserts; i++) {
    test_table.InsertRandomTuple(transaction::timestamp_t(txn_ts), &generator_, &buffer_pool_, new_version);
  }

  EXPECT_EQ(num_inserts, test_table.InsertedTuples().size());
  // Compare each inserted by selecting as the new version
  txn_ts++;
  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    storage::ProjectedRow *stored =
        test_table.Select(inserted_tuple, transaction::timestamp_t(txn_ts), &buffer_pool_, new_version);
    auto tuple_version = test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(txn_ts));
    std::unordered_set<catalog::col_oid_t> add_cols;
    std::unordered_set<catalog::col_oid_t> drop_cols;
    if (tuple_version.version_ != new_version) {
      for (auto col : cols) {
        add_cols.insert(col->Oid());
      }
      drop_cols.insert(col_to_drop.Oid());
    }
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(tuple_version.version_), tuple_version.pr_,
        test_table.GetProjectionMapForOids(tuple_version.version_), test_table.GetBlockLayout(new_version), stored,
        test_table.GetProjectionMapForOids(new_version), add_cols, drop_cols));
  }

  // This txn should not observe the updated schema
  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    auto tuple_version = test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(txn_ts));
    if (tuple_version.version_ != new_version) {
      // Select the tuple with its tuple version
      storage::ProjectedRow *stored =
          test_table.Select(inserted_tuple, transaction::timestamp_t(txn_ts), &buffer_pool_, tuple_version.version_);
      EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(test_table.GetBlockLayout(tuple_version.version_), stored,
                                                              tuple_version.pr_));
    }
  }

  // Delete the first tuple. Note that this affects all transactions with higher timestamp, regardless of version
  txn_ts++;
  test_table.Delete(test_table.InsertedTuples()[0], transaction::timestamp_t(txn_ts), &buffer_pool_);

  // Scan the table with version 0, seeing the first half of the tuples
  byte *buffer = nullptr;
  auto columns = test_table.AllocateColumnBuffer(version, &buffer, num_inserts / 2);
  auto it = test_table.GetTable().begin();
  test_table.GetTable().Scan(test_table.NewTransaction(transaction::timestamp_t(txn_ts), &buffer_pool_), &it, columns,
                             version);
  EXPECT_EQ(num_inserts / 2 - 1, columns->NumTuples());
  EXPECT_EQ(it, test_table.GetTable().end(version));
  for (uint32_t i = 0; i < columns->NumTuples(); i++) {
    storage::ProjectedColumns::RowView stored = columns->InterpretAsRow(i);
    auto ref = test_table.GetReferenceVersionedTuple(columns->TupleSlots()[i], transaction::timestamp_t(txn_ts));
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(ref.version_), ref.pr_, test_table.GetProjectionMapForOids(ref.version_),
        test_table.GetBlockLayout(version), &stored, test_table.GetProjectionMapForOids(version), {}, {}));
  }
  delete[] buffer;

  // Scan the table with the newest version, seeing all the tuples except for the first tuple
  buffer = nullptr;
  columns = test_table.AllocateColumnBuffer(new_version, &buffer, num_inserts - 1);
  it = test_table.GetTable().begin();
  test_table.GetTable().Scan(test_table.NewTransaction(transaction::timestamp_t(txn_ts), &buffer_pool_), &it, columns,
                             new_version);
  EXPECT_EQ(num_inserts - 1, columns->NumTuples());
  EXPECT_EQ(it, test_table.GetTable().end(new_version));
  for (uint32_t i = 0; i < columns->NumTuples(); i++) {
    storage::ProjectedColumns::RowView stored = columns->InterpretAsRow(i);
    auto ref = test_table.GetReferenceVersionedTuple(columns->TupleSlots()[i], transaction::timestamp_t(txn_ts));
    std::unordered_set<catalog::col_oid_t> add_cols;
    std::unordered_set<catalog::col_oid_t> drop_cols;
    if (ref.version_ != new_version) {
      for (auto col : cols) {
        add_cols.insert(col->Oid());
      }
      drop_cols.insert(col_to_drop.Oid());
    }
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(ref.version_), ref.pr_, test_table.GetProjectionMapForOids(ref.version_),
        test_table.GetBlockLayout(new_version), &stored, test_table.GetProjectionMapForOids(new_version), add_cols,
        drop_cols));
  }
  delete[] buffer;

  // update the first half of the tuples, under new version, these will migrate
  txn_ts++;
  for (uint32_t i = 1; i < num_inserts / 2; i++) {
    auto slot = test_table.InsertedTuples()[i];
    auto updated_slot =
        test_table.UpdateTuple(slot, transaction::timestamp_t(txn_ts), &generator_, &buffer_pool_, new_version);
    EXPECT_TRUE(slot != updated_slot);
  }
  EXPECT_EQ(test_table.UpdatedTuples().size(), num_inserts / 2 - 1);

  // Scan the table with the newest version, seeing all updated tuples correctly
  txn_ts++;
  buffer = nullptr;
  columns = test_table.AllocateColumnBuffer(new_version, &buffer, num_inserts / 2 - 1);
  it = test_table.GetTable().begin();
  test_table.GetTable().Scan(test_table.NewTransaction(transaction::timestamp_t(txn_ts), &buffer_pool_), &it, columns,
                             new_version);
  EXPECT_EQ(num_inserts / 2 - 1, columns->NumTuples());
  for (uint32_t i = 0; i < columns->NumTuples(); i++) {
    storage::ProjectedColumns::RowView stored = columns->InterpretAsRow(i);
    auto ref = test_table.GetReferenceVersionedTuple(columns->TupleSlots()[i], transaction::timestamp_t(txn_ts));
    EXPECT_EQ(ref.version_, new_version);

    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(ref.version_), ref.pr_, test_table.GetProjectionMapForOids(ref.version_),
        test_table.GetBlockLayout(new_version), &stored, test_table.GetProjectionMapForOids(new_version), {}, {}));
  }

  delete[] buffer;
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, AddThenDropColumns) {
  const uint16_t max_columns = 20;
  const uint32_t num_inserts = 8;
  uint64_t txn_ts = 0;

  RandomSqlTableTestObject test_table(&block_store_, max_columns, &generator_, null_ratio_(generator_));

  // Update the schema
  storage::layout_version_t version(0);

  // Insert some tuples
  for (uint16_t i = 0; i < num_inserts; i++) {
    test_table.InsertRandomTuple(transaction::timestamp_t(txn_ts), &generator_, &buffer_pool_, version);
  }

  EXPECT_EQ(num_inserts, test_table.InsertedTuples().size());
  storage::layout_version_t new_version(1);

  // We will check the default values of those selected. For now, only test Integer default values
  int32_t default_int = 15719;
  std::vector<int> default_values;
  int num_new_cols = 3;
  default_values.reserve(num_new_cols);
  for (int i = 0; i < num_new_cols; i++) {
    default_values.push_back(default_int + i);
  }
  catalog::Schema::Column col1(
      "new_col1", type::TypeId::INTEGER, false,
      parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(default_values[0])));
  catalog::Schema::Column col2(
      "new_col2", type::TypeId::INTEGER, false,
      parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(default_values[1])));
  catalog::Schema::Column col3(
      "new_col3", type::TypeId::INTEGER, false,
      parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(default_values[2])));
  std::vector<catalog::Schema::Column *> cols{&col1, &col2, &col3};

  auto new_schema = AddColumnsToEnd(test_table.GetSchema(version), cols);

  std::vector<catalog::col_oid_t> oids;
  oids.reserve(num_new_cols);
  for (auto col_ptr : cols) {
    oids.push_back(col_ptr->Oid());
  }
  std::unordered_set<catalog::col_oid_t> oids_set(oids.begin(), oids.end());

  // insert 3 new columns
  test_table.UpdateSchema(test_table.NewTransaction(transaction::timestamp_t{txn_ts}, &buffer_pool_),
                          std::move(new_schema), new_version);

  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    // Check added column default value
    storage::ProjectedRow *stored =
        test_table.Select(inserted_tuple, transaction::timestamp_t(txn_ts), &buffer_pool_, new_version);
    EXPECT_TRUE(StorageTestUtil::ProjectionListAtOidsEqual(stored, test_table.GetProjectionMapForOids(new_version),
                                                           test_table.GetBlockLayout(new_version), oids,
                                                           default_values));
    // Check tuple equality
    auto tuple_version = test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(txn_ts));

    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(tuple_version.version_), tuple_version.pr_,
        test_table.GetProjectionMapForOids(tuple_version.version_), test_table.GetBlockLayout(new_version), stored,
        test_table.GetProjectionMapForOids(new_version), oids_set, {}));
  }

  // Drop all 3 columns we added
  txn_ts++;
  storage::layout_version_t vers2(2);

  //  std::vector<catalog::Schema::Column*> new_cols;
  new_schema = DropColumns(test_table.GetSchema(new_version), oids_set);
  test_table.UpdateSchema(test_table.NewTransaction(transaction::timestamp_t{txn_ts}, &buffer_pool_),
                          std::move(new_schema), vers2);

  // Select check if the columns are dropped
  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    storage::ProjectedRow *stored =
        test_table.Select(inserted_tuple, transaction::timestamp_t(txn_ts), &buffer_pool_, vers2);
    EXPECT_TRUE(StorageTestUtil::ProjectionListAtOidsNone(stored, test_table.GetProjectionMapForOids(vers2),
                                                          test_table.GetBlockLayout(vers2), oids));

    auto tuple_version = test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(txn_ts));
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(tuple_version.version_), tuple_version.pr_,
        test_table.GetProjectionMapForOids(tuple_version.version_), test_table.GetBlockLayout(vers2), stored,
        test_table.GetProjectionMapForOids(vers2), {}, {}));
  }
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, DropThenAddColumns) {
  const uint16_t max_columns = 20;
  const uint32_t num_inserts = 8;
  uint64_t txn_ts = 0;

  RandomSqlTableTestObject test_table(&block_store_, max_columns, &generator_, null_ratio_(generator_));

  // Update the schema
  storage::layout_version_t version(0);

  // Insert some tuples
  for (uint16_t i = 0; i < num_inserts; i++) {
    test_table.InsertRandomTuple(transaction::timestamp_t(txn_ts), &generator_, &buffer_pool_, version);
  }

  EXPECT_EQ(num_inserts, test_table.InsertedTuples().size());
  storage::layout_version_t version1(1);

  int num_cols_to_drop = 3;
  std::vector<catalog::Schema::Column> cols_to_drop;
  std::vector<catalog::col_oid_t> drop_oids;
  std::unordered_set<catalog::col_oid_t> drop_oids_set;

  // drop the first 3 columns (insert them back later with same oids)
  for (int i = 0; i < num_cols_to_drop; i++) {
    std::vector<catalog::Schema::Column> columns = test_table.GetSchema(version).GetColumns();
    cols_to_drop.push_back(columns[i]);
    drop_oids.push_back(columns[i].Oid());
    drop_oids_set.insert(columns[i].Oid());
  }

  auto new_schema = DropColumns(test_table.GetSchema(version), drop_oids_set);

  test_table.UpdateSchema(test_table.NewTransaction(transaction::timestamp_t{txn_ts}, &buffer_pool_),
                          std::move(new_schema), version1);

  // Select check if the columns are dropped
  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    storage::ProjectedRow *stored =
        test_table.Select(inserted_tuple, transaction::timestamp_t(txn_ts), &buffer_pool_, version1);
    EXPECT_TRUE(StorageTestUtil::ProjectionListAtOidsNone(stored, test_table.GetProjectionMapForOids(version1),
                                                          test_table.GetBlockLayout(version1), drop_oids));

    auto tuple_version = test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(txn_ts));
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(tuple_version.version_), tuple_version.pr_,
        test_table.GetProjectionMapForOids(tuple_version.version_), test_table.GetBlockLayout(version1), stored,
        test_table.GetProjectionMapForOids(version1), {}, drop_oids_set));
  }

  // add back the columns we dropped with same oids as before
  storage::layout_version_t version2(2);

  new_schema = AddColumns(test_table.GetSchema(version1), cols_to_drop);
  test_table.UpdateSchema(test_table.NewTransaction(transaction::timestamp_t{txn_ts}, &buffer_pool_),
                          std::move(new_schema), version2);

  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    storage::ProjectedRow *stored =
        test_table.Select(inserted_tuple, transaction::timestamp_t(txn_ts), &buffer_pool_, version2);

    // Check tuple equality
    auto tuple_version = test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(txn_ts));

    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(tuple_version.version_), tuple_version.pr_,
        test_table.GetProjectionMapForOids(tuple_version.version_), test_table.GetBlockLayout(version2), stored,
        test_table.GetProjectionMapForOids(version2), {}, {}));
  }
}

}  // namespace terrier::storage
