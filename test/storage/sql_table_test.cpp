#include "storage/sql_table.h"

#include <cstring>
#include <vector>

#include "tbb/concurrent_hash_map.h"

#include "storage/storage_util.h"
#include "test_util/multithread_test_util.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_context.h"

namespace terrier::storage {

using ByteVecPtr = std::unique_ptr<std::vector<byte>>;

class SqlTableTests : public TerrierTest {
 public:
  storage::BlockStore block_store_{100, 100};
  storage::RecordBufferSegmentPool buffer_pool_{100000, 10000};
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};
  void SetUp() override {}
  void TearDown() override {}
};

static std::unique_ptr<catalog::Schema> ChangeColType(const catalog::Schema &schema, catalog::col_oid_t oid,
                                                      type::TypeId type_id) {
  auto columns = schema.GetColumns();
  size_t i = 0;
  for (; i < columns.size(); i++) {
    if (columns[i].Oid() == oid) {
      StorageTestUtil::SetType(&columns[i], type_id);
      break;
    }
  }

  return std::make_unique<catalog::Schema>(columns);
}

static std::unique_ptr<catalog::Schema> ChangeDefaultValue(const catalog::Schema &schema, const catalog::col_oid_t oid,
                                                           std::unique_ptr<parser::AbstractExpression> default_value) {
  std::vector<catalog::Schema::Column> columns(schema.GetColumns());
  for (auto &col : columns) {
    if (col.Oid() == oid) {
      // NOLINTNEXTLINE
      StorageTestUtil::SetDefaultValue(&col, std::move(default_value));
    }
  }

  return std::make_unique<catalog::Schema>(columns);
}

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

template <class T>
static ByteVecPtr CastValue(T val, type::TypeId id) {
  auto vals = std::make_unique<std::vector<byte>>(type::TypeUtil::GetTypeSize(id));
  std::memcpy(vals->data(), &val, type::TypeUtil::GetTypeSize(id));
  return vals;
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
    for (auto &buffer : concurrent_buffers_) {
      delete[] buffer;
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

  // concurrently insert tuples with num_threads threads, and optionally also concurrently update schema
  template <class Random>
  void ConcurrentInsertRandomTuples(const transaction::timestamp_t timestamp, Random *generator,
                                    storage::RecordBufferSegmentPool *buffer_pool,
                                    storage::layout_version_t layout_version, uint32_t num_threads,
                                    uint32_t num_inserts_per_thread,
                                    common::ManagedPointer<transaction::TransactionContext> update_schema_txn = nullptr,
                                    std::unique_ptr<catalog::Schema> updated_schema = nullptr,
                                    const storage::layout_version_t updated_layout_version = 0) {
    uint32_t num_new_tuples = num_threads * num_inserts_per_thread;
    uint32_t inserted_slots_size = inserted_slots_.size();
    inserted_slots_.resize(inserted_slots_size + num_new_tuples);

    std::vector<transaction::TransactionContext *> new_txns;
    std::vector<storage::RedoRecord *> new_redos;

    for (size_t i = 0; i < num_new_tuples; i++) {
      // generate a txn with an UndoRecord to populate on Insert
      auto *txn =
          new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);

      txns_.emplace_back(txn);

      // generate a random ProjectedRow to Insert
      auto redo_initializer = pris_.at(layout_version);
      auto *insert_redo = txn->StageWrite(catalog::db_oid_t{0}, catalog::table_oid_t{0}, redo_initializer);
      auto *insert_tuple = insert_redo->Delta();
      auto layout = table_->GetBlockLayout(layout_version);
      StorageTestUtil::PopulateRandomRow(insert_tuple, layout, null_bias_, generator);

      // Fill up the random bytes for non-nullable columns
      FillNullValue(insert_tuple, GetSchema(layout_version), table_->GetColumnIdToOidMap(layout_version),
                    table_->GetBlockLayout(layout_version), generator);

      redos_.emplace_back(insert_redo);

      new_txns.push_back(txn);
      new_redos.push_back(insert_redo);
    }

    auto total_threads = num_threads + (update_schema_txn != nullptr);
    common::WorkerPool thread_pool(total_threads, {});

    auto workload = [&](uint32_t thread_id) {
      if (update_schema_txn != nullptr && thread_id == num_threads) {
        UpdateSchema(update_schema_txn, std::move(updated_schema), updated_layout_version);
      } else {
        for (size_t i = 0; i < num_inserts_per_thread; i++) {
          auto loc = thread_id * num_inserts_per_thread + i;
          auto insert_redo = new_redos[loc];
          storage::TupleSlot slot = table_->Insert(common::ManagedPointer(new_txns[loc]), insert_redo, layout_version);
          inserted_slots_[inserted_slots_size + loc] = slot;
          tuple_versions_accessor_ accessor;
          tuple_versions_.insert(accessor, slot);
          accessor->second.push_back({timestamp, insert_redo->Delta(), layout_version});
        }
      }
    };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, total_threads, workload);
  }

  template <class Random>
  storage::TupleSlot InsertTupleWithValues(const transaction::timestamp_t timestamp, Random *generator,
                                           storage::RecordBufferSegmentPool *buffer_pool,
                                           storage::layout_version_t layout_version,
                                           const std::vector<catalog::Schema::Column> &cols,
                                           const std::vector<const byte *> &values) {
    // generate a txn with an UndoRecord to populate on Insert
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
    txns_.emplace_back(txn);

    auto redo_initializer = pris_.at(layout_version);
    auto *insert_redo = txn->StageWrite(catalog::db_oid_t{0}, catalog::table_oid_t{0}, redo_initializer);
    auto *insert_tuple = insert_redo->Delta();
    auto layout = table_->GetBlockLayout(layout_version);
    StorageTestUtil::PopulateRandomRow(insert_tuple, layout, null_bias_, generator);

    // Overwrite the values at the columns
    std::vector<catalog::col_oid_t> oids;
    oids.reserve(cols.size());
    for (const auto &col : cols) oids.push_back(col.Oid());
    auto col_oid_to_pr_idx = GetProjectionMapForOids(layout_version);
    for (size_t i = 0; i < cols.size(); i++) {
      auto col = cols[i];
      auto pr_idx = col_oid_to_pr_idx.at(col.Oid());
      auto valp = values[i];
      std::memcpy(insert_tuple->AccessForceNotNull(pr_idx), valp, type::TypeUtil::GetTypeSize(col.Type()));
    }

    // Do insert
    redos_.emplace_back(insert_redo);
    storage::TupleSlot slot = table_->Insert(common::ManagedPointer(txn), insert_redo, layout_version);
    inserted_slots_.push_back(slot);
    tuple_versions_accessor_ accessor;
    tuple_versions_.insert(accessor, slot);
    accessor->second.push_back({timestamp, insert_tuple, layout_version});

    return slot;
  }

  template <class Random>
  storage::TupleSlot InsertRandomTuple(const transaction::timestamp_t timestamp, Random *generator,
                                       storage::RecordBufferSegmentPool *buffer_pool,
                                       storage::layout_version_t layout_version) {
    // generate a txn with an UndoRecord to populate on Insert
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);

    txns_.emplace_back(txn);

    // generate a random ProjectedRow to Insert
    auto redo_initializer = pris_.at(layout_version);
    auto *insert_redo = txn->StageWrite(catalog::db_oid_t{0}, catalog::table_oid_t{0}, redo_initializer);
    auto *insert_tuple = insert_redo->Delta();
    auto layout = table_->GetBlockLayout(layout_version);
    StorageTestUtil::PopulateRandomRow(insert_tuple, layout, null_bias_, generator);

    // Fill up the random bytes for non-nullable columns
    FillNullValue(insert_tuple, GetSchema(layout_version), table_->GetColumnIdToOidMap(layout_version),
                  table_->GetBlockLayout(layout_version), generator);

    redos_.emplace_back(insert_redo);

    storage::TupleSlot slot = table_->Insert(common::ManagedPointer(txn), insert_redo, layout_version);
    inserted_slots_.push_back(slot);
    tuple_versions_accessor_ accessor;
    tuple_versions_.insert(accessor, slot);
    accessor->second.push_back({timestamp, insert_tuple, layout_version});

    return slot;
  }

  template <class Random>
  std::vector<TupleSlot> ConcurrentlyUpdateTuples(std::vector<storage::TupleSlot> slots,
                                                  const transaction::timestamp_t timestamp, Random *generator,
                                                  storage::RecordBufferSegmentPool *buffer_pool,
                                                  storage::layout_version_t layout_version, int num_threads) {
    int num_new_updates = slots.size();

    TERRIER_ASSERT(num_new_updates % num_threads == 0, "concurrent updates can be evenly spread among threads");
    int num_updates_per_thread = num_new_updates / num_threads;

    int updated_slots_size = updated_slots_.size();
    updated_slots_.resize(updated_slots_size + num_new_updates);
    std::vector<transaction::TransactionContext *> new_txns;
    std::vector<storage::RedoRecord *> new_redos;
    std::vector<storage::ProjectedRow *> new_updated_tuples;
    std::vector<storage::TupleSlot> new_slots(num_new_updates);

    for (int i = 0; i < num_new_updates; i++) {
      // generate a txn with an UndoRecord to populate on Insert
      auto *txn =
          new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
      txns_.emplace_back(txn);
      new_txns.push_back(txn);

      // generate a random ProjectedRow to Insert
      auto redo_initializer = pris_.at(layout_version);
      auto *update_redo = txn->StageWrite(catalog::db_oid_t{0}, catalog::table_oid_t{0}, redo_initializer);
      update_redo->SetTupleSlot(slots[i]);
      auto *update_tuple = update_redo->Delta();
      auto layout = table_->GetBlockLayout(layout_version);
      StorageTestUtil::PopulateRandomRow(update_tuple, layout, null_bias_, generator);

      // Fill up the random bytes for non-nullable columns
      FillNullValue(update_tuple, GetSchema(layout_version), table_->GetColumnIdToOidMap(layout_version),
                    table_->GetBlockLayout(layout_version), generator);

      redos_.emplace_back(update_redo);
      new_redos.push_back(update_redo);
      new_updated_tuples.push_back(update_tuple);
    }

    common::WorkerPool thread_pool(num_threads, {});
    auto workload = [&](uint32_t thread_id) {
      for (int i = 0; i < num_updates_per_thread; i++) {
        int loc = static_cast<int>(thread_id) * num_updates_per_thread + i;
        storage::TupleSlot updated_slot;
        bool res = table_->Update(common::ManagedPointer(new_txns[loc]), new_redos[loc], layout_version, &updated_slot);
        EXPECT_TRUE(res == true);

        tuple_versions_accessor_ accessor;
        tuple_versions_.insert(accessor, updated_slot);
        accessor->second.push_back({timestamp, new_updated_tuples[loc], layout_version});

        updated_slots_[updated_slots_size + loc] = updated_slot;
      }
    };
    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);

    return new_slots;
  }

  template <class Random>
  TupleSlot UpdateTuple(const storage::TupleSlot slot, const transaction::timestamp_t timestamp, Random *generator,
                        storage::RecordBufferSegmentPool *buffer_pool, storage::layout_version_t layout_version) {
    // generate a txn with an UndoRecord to populate on Insert
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
    txns_.emplace_back(txn);

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

    redos_.emplace_back(update_redo);
    storage::TupleSlot updated_slot;
    bool res = table_->Update(common::ManagedPointer(txn), update_redo, layout_version, &updated_slot);

    EXPECT_TRUE(res == true);
    if (!res) {
      return TupleSlot();
    }

    tuple_versions_accessor_ accessor;
    tuple_versions_.insert(accessor, updated_slot);
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
    bool found = tuple_versions_.find(accessor, slot);
    TERRIER_ASSERT(found, "slot must be within tuple_versions_");
    auto &versions = accessor->second;

    // search backwards so the first entry with smaller timestamp can be returned
    for (auto i = static_cast<int64_t>(versions.size() - 1); i >= 0; i--) {
      if (transaction::TransactionUtil::NewerThan(timestamp, versions[i].ts_) || timestamp == versions[i].ts_)
        return versions[i];
    }
    return {transaction::timestamp_t{transaction::INVALID_TXN_TIMESTAMP}, nullptr, storage::layout_version_t{0}};
  }

  std::vector<storage::ProjectedRow *> ConcurrentlySelectTuples(std::vector<storage::TupleSlot> slots,
                                                                const transaction::timestamp_t timestamp,
                                                                storage::RecordBufferSegmentPool *buffer_pool,
                                                                storage::layout_version_t layout_version,
                                                                int num_threads) {
    int num_slots = slots.size();
    TERRIER_ASSERT(num_slots % num_threads == 0, "num slots must be multiple of num_threads");
    int slots_per_thread = num_slots / num_threads;

    std::vector<storage::ProjectedRow *> rows;
    std::vector<transaction::TransactionContext *> new_txns;

    for (int i = 0; i < num_slots; i++) {
      auto *txn =
          new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
      txns_.emplace_back(txn);
      auto pri = pris_.at(layout_version);
      auto buffer = common::AllocationUtil::AllocateAligned(pri.ProjectedRowSize());
      concurrent_buffers_.push_back(buffer);
      storage::ProjectedRow *select_row = pri.InitializeRow(buffer);
      rows.push_back(select_row);
      new_txns.push_back(txn);
    }

    common::WorkerPool thread_pool(num_threads, {});

    auto workload = [&](uint32_t thread_id) {
      for (int i = 0; i < slots_per_thread; i++) {
        int loc = static_cast<int>(thread_id) * slots_per_thread + i;
        table_->Select(common::ManagedPointer(new_txns[loc]), slots[loc], rows[loc], layout_version);
      }
    };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);

    return rows;
  }

  storage::ProjectedRow *Select(const storage::TupleSlot slot, const transaction::timestamp_t timestamp,
                                storage::RecordBufferSegmentPool *buffer_pool,
                                storage::layout_version_t layout_version) {
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
    txns_.emplace_back(txn);

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
    schemas_.insert(std::make_pair(layout_version, std::move(schema)));
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
  size_t NumTxns() { return txns_.size(); }

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
    // NOLINTNEXTLINE
    static size_t hash(const storage::TupleSlot &slot) { return std::hash<storage::TupleSlot>{}(slot); }

    // NOLINTNEXTLINE
    static bool equal(const storage::TupleSlot &slot1, const storage::TupleSlot &slot2) { return slot1 == slot2; }
  };

 private:
  std::unique_ptr<storage::SqlTable> table_;
  double null_bias_;
  std::unordered_map<storage::layout_version_t, storage::ProjectedRowInitializer> pris_;
  std::unordered_map<storage::layout_version_t, byte *> buffers_;
  std::vector<byte *> concurrent_buffers_;
  std::vector<common::ManagedPointer<storage::RedoRecord>> redos_;
  std::vector<std::unique_ptr<transaction::TransactionContext>> txns_;
  std::vector<storage::TupleSlot> inserted_slots_;
  std::vector<storage::TupleSlot> updated_slots_;
  // oldest to newest
  tbb::concurrent_hash_map<storage::TupleSlot, std::vector<TupleVersion>, SlotHash> tuple_versions_;
  // NOLINTNEXTLINE
  typedef tbb::concurrent_hash_map<storage::TupleSlot, std::vector<TupleVersion>, SlotHash>::accessor
      tuple_versions_accessor_;

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
  const uint32_t total_inserts = num_threads * num_inserts_per_thread;

  storage::layout_version_t version(0);

  RandomSqlTableTestObject test_table(&block_store_, max_columns, &generator_, null_ratio_(generator_));

  // insert random tuples concurrently
  test_table.ConcurrentInsertRandomTuples(transaction::timestamp_t(0), &generator_, &buffer_pool_, version, num_threads,
                                          num_inserts_per_thread);

  EXPECT_EQ(total_inserts, test_table.InsertedTuples().size());

  // Multiple thread selects into SqlTable concurrently
  std::vector<storage::ProjectedRow *> stored_vec = test_table.ConcurrentlySelectTuples(
      test_table.InsertedTuples(), transaction::timestamp_t(1), &buffer_pool_, version, num_threads);
  EXPECT_EQ(2 * total_inserts, test_table.NumTxns());

  // Compares selected tuples with expected tuples
  for (size_t i = 0; i < total_inserts; i++) {
    const auto &inserted_tuple = test_table.InsertedTuples()[i];
    auto ref = test_table.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(1));

    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(ref.version_), ref.pr_, test_table.GetProjectionMapForOids(ref.version_),
        test_table.GetBlockLayout(version), stored_vec[i], test_table.GetProjectionMapForOids(version), {}, {}));
  }
}

// test concurrent insert, concurrent update schema, concurrent scan, concurrent update, and concurrently delete
// NOLINTNEXTLINE
TEST_F(SqlTableTests, ConcurrentOperationsWithSchemaChange) {
  const uint16_t max_columns = 20;
  uint64_t txn_ts = 0;

  int num_inserts_per_thread = 8;
  const uint32_t machine_threads = MultiThreadTestUtil::HardwareConcurrency();
  const uint32_t num_threads = machine_threads % 2 == 0 ? machine_threads : machine_threads - 1;
  common::WorkerPool thread_pool(num_threads, {});
  const uint32_t num_inserts = num_threads * static_cast<uint32_t>(num_inserts_per_thread);

  storage::layout_version_t version(0);

  RandomSqlTableTestObject test_table(&block_store_, max_columns, &generator_, null_ratio_(generator_));

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

  auto update_schema_txn = test_table.NewTransaction(transaction::timestamp_t{txn_ts}, &buffer_pool_);

  // concurrently insert first half of the tuples with old version, and concurrently update schema to new version
  test_table.ConcurrentInsertRandomTuples(transaction::timestamp_t(txn_ts - 1), &generator_, &buffer_pool_, version,
                                          num_threads, num_inserts_per_thread / 2, update_schema_txn,
                                          std::move(new_schema), new_version);

  EXPECT_EQ(num_inserts / 2, test_table.InsertedTuples().size());

  txn_ts++;

  // concurrently insert second half of tuples, with new version
  test_table.ConcurrentInsertRandomTuples(transaction::timestamp_t(txn_ts), &generator_, &buffer_pool_, new_version,
                                          num_threads, num_inserts_per_thread / 2);

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

  // Scan the table with version 0, seeing the first half of the tuples (except for the deleted one)
  byte *buffer = nullptr;
  auto columns = test_table.AllocateColumnBuffer(version, &buffer, num_inserts / 2 - 1);
  auto it = test_table.GetTable().begin();
  test_table.GetTable().Scan(test_table.NewTransaction(transaction::timestamp_t(txn_ts), &buffer_pool_), &it, columns,
                             version);
  EXPECT_EQ(num_inserts / 2 - 1, columns->NumTuples());
  for (uint32_t i = 0; i < columns->NumTuples(); i++) {
    storage::ProjectedColumns::RowView stored = columns->InterpretAsRow(i);
    auto ref = test_table.GetReferenceVersionedTuple(columns->TupleSlots()[i], transaction::timestamp_t(txn_ts));
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallowMatchSchema(
        test_table.GetBlockLayout(ref.version_), ref.pr_, test_table.GetProjectionMapForOids(ref.version_),
        test_table.GetBlockLayout(version), &stored, test_table.GetProjectionMapForOids(version), {}, {}));
  }
  delete[] buffer;

  int scan_threads = 3;
  std::vector<byte *> buffer_vec(scan_threads);
  std::vector<storage::ProjectedColumns *> columns_vec(scan_threads);
  for (int thread_id = 0; thread_id < scan_threads; thread_id++) {
    columns = test_table.AllocateColumnBuffer(new_version, &buffer_vec[thread_id], num_inserts - 1);
    columns_vec[thread_id] = columns;
  }

  // Scan the table concurrently with the newest version, seeing all the tuples except for the first tuple
  auto workload = [&](uint32_t thread_id) {
    auto it2 = test_table.GetTable().begin();
    test_table.GetTable().Scan(test_table.NewTransaction(transaction::timestamp_t(txn_ts), &buffer_pool_), &it2,
                               columns_vec[thread_id], new_version);
  };
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, scan_threads, workload);

  for (int thread_id = 0; thread_id < scan_threads; thread_id++) {
    columns = columns_vec[thread_id];
    EXPECT_EQ(num_inserts - 1, columns->NumTuples());
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
    delete[] buffer_vec[thread_id];
  }

  // update the first half of the tuples except for the first one, under new version, these will migrate
  txn_ts++;

  std::vector<storage::TupleSlot> slots_to_update(test_table.InsertedTuples().begin() + 1,
                                                  test_table.InsertedTuples().begin() + num_inserts / 4 + 1);
  int num_updates = slots_to_update.size();
  EXPECT_EQ(slots_to_update.size(), num_inserts / 4);
  std::vector<storage::TupleSlot> updated_slots = test_table.ConcurrentlyUpdateTuples(
      slots_to_update, transaction::timestamp_t(txn_ts), &generator_, &buffer_pool_, new_version, num_threads);
  for (int i = 0; i < num_updates; i++) {
    EXPECT_TRUE(slots_to_update[i] != updated_slots[i]);
  }

  //  for (uint32_t i = 1; i < num_inserts / 2; i++) {
  //    auto slot = test_table.InsertedTuples()[i];
  //    auto updated_slot =
  //        test_table.UpdateTuple(slot, transaction::timestamp_t(txn_ts), &generator_, &buffer_pool_, new_version);
  //    EXPECT_TRUE(slot != updated_slot);
  //  }
  EXPECT_EQ(test_table.UpdatedTuples().size(), num_updates);
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

  // Scan the table with version 0, seeing the first half of the tuples except for the first one
  byte *buffer = nullptr;
  auto columns = test_table.AllocateColumnBuffer(version, &buffer, num_inserts / 2 - 1);
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

  // update the first half of the tuples except for the first one, under new version, these will migrate
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
  columns = test_table.AllocateColumnBuffer(new_version, &buffer, num_inserts - 1);
  it = test_table.GetTable().begin();
  test_table.GetTable().Scan(test_table.NewTransaction(transaction::timestamp_t(txn_ts), &buffer_pool_), &it, columns,
                             new_version);
  EXPECT_EQ(num_inserts - 1, columns->NumTuples());
  for (uint32_t i = 0; i < columns->NumTuples() / 2; i++) {
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
  std::vector<ByteVecPtr> default_values;
  int num_new_cols = 3;
  default_values.reserve(num_new_cols);
  for (int i = 0; i < num_new_cols; i++) {
    default_values.push_back(CastValue(default_int + i, type::TypeId::INTEGER));
  }
  catalog::Schema::Column col1("new_col1", type::TypeId::INTEGER, false,
                               parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(default_int)));
  catalog::Schema::Column col2(
      "new_col2", type::TypeId::INTEGER, false,
      parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(default_int + 1)));
  catalog::Schema::Column col3(
      "new_col3", type::TypeId::INTEGER, false,
      parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(default_int + 2)));
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
TEST_F(SqlTableTests, ChangeIntType) {
  const uint16_t max_columns = 20;
  const uint32_t num_inserts = 8;
  uint64_t txn_ts = 0;

  RandomSqlTableTestObject test_table(&block_store_, max_columns, &generator_, null_ratio_(generator_));
  storage::layout_version_t vers1(1);
  int8_t default_tiny_int = 15;
  catalog::Schema::Column col(
      "new_col", type::TypeId::TINYINT, true,
      parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(default_tiny_int)));
  std::vector<catalog::Schema::Column *> cols{&col};
  auto schema1 = AddColumnsToEnd(test_table.GetSchema(storage::layout_version_t{0}), cols);
  test_table.UpdateSchema(test_table.NewTransaction(transaction::timestamp_t{txn_ts}, &buffer_pool_),
                          std::move(schema1), vers1);

  // Insert with new schema
  for (uint16_t i = 0; i < num_inserts; i++) {
    test_table.InsertTupleWithValues(transaction::timestamp_t(txn_ts), &generator_, &buffer_pool_, vers1, {col},
                                     {reinterpret_cast<const byte *>(&default_tiny_int)});
  }

  // Update the schema by changeing the col type
  auto default_smallint = int16_t(default_tiny_int);
  std::vector<ByteVecPtr> default_vals;
  default_vals.push_back(CastValue(default_smallint, type::TypeId::SMALLINT));
  auto vers2 = vers1 + 1;
  auto schema2 = ChangeColType(test_table.GetSchema(vers1), col.Oid(), type::TypeId::SMALLINT);
  txn_ts++;
  test_table.UpdateSchema(test_table.NewTransaction(transaction::timestamp_t{txn_ts}, &buffer_pool_),
                          std::move(schema2), vers2);

  // Select to check that changed column
  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    // Check added column default value
    storage::ProjectedRow *stored =
        test_table.Select(inserted_tuple, transaction::timestamp_t(txn_ts), &buffer_pool_, vers2);
    EXPECT_TRUE(StorageTestUtil::ProjectionListAtOidsEqual(stored, test_table.GetProjectionMapForOids(vers2),
                                                           test_table.GetBlockLayout(vers2), {col.Oid()},
                                                           default_vals));
  }
}

// NOLINTNEXTLINE
TEST_F(SqlTableTests, MultiDefaultValue) {
  const uint16_t max_columns = 8;
  const uint32_t num_inserts = 8;
  uint64_t txn_ts = 0;

  RandomSqlTableTestObject test_table(&block_store_, max_columns, &generator_, null_ratio_(generator_));
  // Insert some tuples
  storage::layout_version_t vers0(0);
  for (uint16_t i = 0; i < num_inserts; i++) {
    test_table.InsertRandomTuple(transaction::timestamp_t(txn_ts), &generator_, &buffer_pool_, vers0);
  }

  // New Schema 1 - Add a column with default value
  storage::layout_version_t vers1(1);
  auto old_default_val = 15712;
  txn_ts++;
  catalog::Schema::Column col_to_add(
      "new_col1", type::TypeId::INTEGER, false,
      parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(old_default_val)));

  auto schema1 = AddColumnsToEnd(test_table.GetSchema(vers0), {&col_to_add});
  test_table.UpdateSchema(test_table.NewTransaction(transaction::timestamp_t(txn_ts), &buffer_pool_),
                          std::move(schema1), vers1);

  // New Schema 2 - Change the default value
  auto new_val = 15412;
  auto new_default_val =
      std::make_unique<parser::ConstantValueExpression>(type::TransientValueFactory::GetInteger(new_val));
  auto schema2 = ChangeDefaultValue(test_table.GetSchema(vers1), col_to_add.Oid(), std::move(new_default_val));
  storage::layout_version_t vers2(2);
  txn_ts++;
  test_table.UpdateSchema(test_table.NewTransaction(transaction::timestamp_t(txn_ts), &buffer_pool_),
                          std::move(schema2), vers2);

  // Select Should be the New Schema 1's Default Value
  txn_ts++;
  std::vector<ByteVecPtr> default_values;
  default_values.push_back(CastValue(old_default_val, type::TypeId::INTEGER));
  for (const auto &inserted_tuple : test_table.InsertedTuples()) {
    storage::ProjectedRow *stored =
        test_table.Select(inserted_tuple, transaction::timestamp_t(txn_ts), &buffer_pool_, vers2);
    EXPECT_TRUE(StorageTestUtil::ProjectionListAtOidsEqual(stored, test_table.GetProjectionMapForOids(vers1),
                                                           test_table.GetBlockLayout(vers1), {col_to_add.Oid()},
                                                           default_values));
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
