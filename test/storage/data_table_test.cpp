#include "common/test_util.h"
#include "storage/data_table.h"
#include "storage/storage_utils.h"
#include "util/storage_test_util.h"

namespace terrier {
struct DataTableTests : public ::testing::Test {
  storage::BlockStore block_store_{100};
  std::default_random_engine generator_;

  static void ApplyDelta(storage::DataTable *table,
                         const storage::BlockLayout &layout,
                         const storage::ProjectedRow &delta,
                         storage::ProjectedRow *buffer) {

    // Creates a mapping from col offset to project list index. This allows us to efficiently
    // access columns since deltas can concern a different set of columns when chasing the
    // version chain
    std::unordered_map<uint16_t, uint16_t> projection_list_col_to_index;
    for (uint16_t i = 0; i < buffer->NumColumns(); i++) projection_list_col_to_index.emplace(buffer->ColumnIds()[i], i);

    storage::ApplyDelta(layout, delta, buffer, projection_list_col_to_index);
  }

};

// Generates a random table layout and coin flip bias for an attribute being null, inserts num_inserts random tuples
// into an empty DataTable. Then, Selects the inserted TupleSlots and compares the results to the original inserted
// random tuple. Repeats for num_iterations.
TEST_F(DataTableTests, SimpleInsertSelect) {
  const uint32_t num_iterations = 100;
  const uint32_t num_inserts = 1000;
  const uint16_t max_columns = 100;

  std::uniform_real_distribution<double> distribution(0.0, 1.0);

  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {

    double null_bias = distribution(generator_);

    storage::BlockLayout layout = testutil::RandomLayout(generator_, max_columns);
    storage::DataTable table(block_store_, layout);

    std::vector<byte *> redo_buffers(num_inserts);
    std::vector<byte *> undo_buffers(num_inserts);
    std::vector<std::pair<storage::TupleSlot, storage::ProjectedRow *>> inserted_tuples;

    std::vector<uint16_t> col_ids = testutil::ProjectionListAllColumns(layout);

    uint32_t redo_size = storage::ProjectedRow::Size(layout, col_ids);
    uint32_t undo_size = storage::DeltaRecord::Size(layout, col_ids);

    for (uint32_t i = 0; i < num_inserts; ++i) {

      // generate a random redo ProjectedRow to Insert
      byte *redo_buffer = new byte[redo_size];
      redo_buffers[i] = redo_buffer;
      storage::ProjectedRow *redo = storage::ProjectedRow::InitializeProjectedRow(layout, col_ids, redo_buffer);
      testutil::GenerateRandomRow(redo, layout, generator_, null_bias);

      // generate an undo DeltaRecord to populate on Insert
      byte *undo_buffer = new byte[undo_size];
      undo_buffers[i] = undo_buffer;
      storage::DeltaRecord *undo =
          storage::DeltaRecord::InitializeDeltaRecord(nullptr, timestamp_t(0), layout, col_ids, undo_buffer);

      storage::TupleSlot tuple = table.Insert(*redo, undo);

      inserted_tuples.emplace_back(tuple, redo);
    }

    EXPECT_EQ(num_inserts, inserted_tuples.size());

    std::vector<byte *> select_buffers(num_inserts);

    for (uint32_t i = 0; i < num_inserts; ++i) {
      // generate a redo ProjectedRow for Select
      byte *select_buffer = new byte[redo_size];
      select_buffers[i] = select_buffer;
      storage::ProjectedRow *select_row = storage::ProjectedRow::InitializeProjectedRow(layout, col_ids, select_buffer);

      table.Select(timestamp_t(1), inserted_tuples[i].first, select_row);

      EXPECT_TRUE(testutil::ProjectionListEqual(layout, select_row, inserted_tuples[i].second));
    }

    for (uint32_t i = 0; i < num_inserts; i++) {
      delete[] redo_buffers[i];
      delete[] undo_buffers[i];
      delete[] select_buffers[i];
    }
  }
}

// Generates a random table layout and coin flip bias for an attribute being null, inserts 1 random tuple into an empty
// DataTable. Then, randomly updates the tuple num_updates times. Finally, Selects at each timestamp to verify that the
// delta chain produces the correct tuple. Repeats for num_iterations.
TEST_F(DataTableTests, SimpleVersionChain) {
  const uint32_t num_iterations = 100;
  const uint32_t num_updates = 10;
  const uint16_t max_columns = 100;

  std::uniform_real_distribution<double> distribution(0.0, 1.0);

  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {

    double null_bias = distribution(generator_);

    storage::BlockLayout layout = testutil::RandomLayout(generator_, max_columns);
    storage::DataTable table(block_store_, layout);

    std::vector<byte *> update_buffers(num_updates);
    std::vector<byte *> undo_buffers(num_updates + 1);
    std::vector<std::pair<timestamp_t, storage::ProjectedRow *>> tuple_versions;
    timestamp_t timestamp = 0;

    std::vector<uint16_t> col_ids = testutil::ProjectionListAllColumns(layout);

    uint32_t redo_size = storage::ProjectedRow::Size(layout, col_ids);
    uint32_t undo_size = storage::DeltaRecord::Size(layout, col_ids);

    // generate a random redo ProjectedRow to Insert
    byte *insert_buffer = new byte[redo_size];
    storage::ProjectedRow *insert = storage::ProjectedRow::InitializeProjectedRow(layout, col_ids, insert_buffer);
    testutil::GenerateRandomRow(insert, layout, generator_, null_bias);

    // generate an undo DeltaRecord to populate on Insert
    byte *undo_buffer = new byte[undo_size];
    undo_buffers[0] = undo_buffer;
    storage::DeltaRecord *undo =
        storage::DeltaRecord::InitializeDeltaRecord(nullptr, timestamp, layout, col_ids, undo_buffer);

    storage::TupleSlot tuple = table.Insert(*insert, undo);

    tuple_versions.emplace_back(timestamp++, insert);

    EXPECT_EQ(1, tuple_versions.size());

    for (uint32_t i = 1; i < num_updates + 1; ++i) {
      std::vector<uint16_t> update_col_ids = testutil::ProjectionListRandomColumns(layout, generator_);

      // generate a random update ProjectedRow to Update
      byte *update_buffer = new byte[redo_size]; // safe to overprovision this
      storage::ProjectedRow
          *update = storage::ProjectedRow::InitializeProjectedRow(layout, update_col_ids, update_buffer);
      testutil::GenerateRandomRow(update, layout, generator_, null_bias);

      // generate a version of this tuple for this timestamp
      byte *version_buffer = new byte[redo_size];
      storage::ProjectedRow *version = storage::ProjectedRow::InitializeProjectedRow(layout, col_ids, version_buffer);
      PELOTON_MEMCPY(version, tuple_versions.back().second, redo_size);
      ApplyDelta(&table, layout, *update, version);

      // generate an undo DeltaRecord to populate on Insert
      undo_buffer = new byte[undo_size]; // safe to overprovision this
      undo_buffers[i] = undo_buffer;
      undo = storage::DeltaRecord::InitializeDeltaRecord(nullptr, timestamp, layout, update_col_ids, undo_buffer);

      EXPECT_TRUE(table.Update(tuple, *update, undo));

      tuple_versions.emplace_back(timestamp++, version);

      delete[] update_buffer;
    }

    std::vector<byte *> select_buffers(num_updates + 1);

    for (uint32_t i = 0; i < tuple_versions.size(); ++i) {
      // reference version of the tuple
      timestamp = tuple_versions[i].first;
      storage::ProjectedRow *version = tuple_versions[i].second;

      // generate a redo ProjectedRow for Select
      byte *select_buffer = new byte[redo_size];
      select_buffers[i] = select_buffer;
      storage::ProjectedRow *select_row = storage::ProjectedRow::InitializeProjectedRow(layout, col_ids, select_buffer);

      table.Select(timestamp, tuple, select_row);

      EXPECT_TRUE(testutil::ProjectionListEqual(layout, select_row, version));
    }

    for (auto i : select_buffers) {
      delete[] i;
    }

    for (auto i : undo_buffers) {
      delete[] i;
    }

    for (auto i : tuple_versions) {
      delete[] reinterpret_cast<byte *>(i.second);
    }
  }
}

TEST_F(DataTableTests, WriteWriteConflictUpdateFails) {
  const uint32_t num_iterations = 100;
  const uint16_t max_columns = 100;

  std::uniform_real_distribution<double> distribution(0.0, 1.0);

  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {

    double null_bias = distribution(generator_);

    storage::BlockLayout layout = testutil::RandomLayout(generator_, max_columns);
    storage::DataTable table(block_store_, layout);

    std::vector<byte *> redo_buffers(2);
    std::vector<byte *> undo_buffers(3);

    std::vector<uint16_t> col_ids = testutil::ProjectionListAllColumns(layout);

    uint32_t redo_size = storage::ProjectedRow::Size(layout, col_ids);
    uint32_t undo_size = storage::DeltaRecord::Size(layout, col_ids);

    // generate a random redo ProjectedRow to Insert
    byte *insert_buffer = new byte[redo_size];
    redo_buffers[0] = insert_buffer;
    storage::ProjectedRow *insert = storage::ProjectedRow::InitializeProjectedRow(layout, col_ids, insert_buffer);
    testutil::GenerateRandomRow(insert, layout, generator_, null_bias);

    // generate an undo DeltaRecord to populate on Insert
    byte *undo_buffer = new byte[undo_size];
    undo_buffers[0] = undo_buffer;
    storage::DeltaRecord *undo =
        storage::DeltaRecord::InitializeDeltaRecord(nullptr, timestamp_t(0), layout, col_ids, undo_buffer);

    storage::TupleSlot tuple = table.Insert(*insert, undo);

    std::vector<uint16_t> update_col_ids = testutil::ProjectionListRandomColumns(layout, generator_);

    // take the write lock

    // generate a random update ProjectedRow to Update
    byte *update_buffer = new byte[redo_size]; // safe to overprovision this
    storage::ProjectedRow
        *update = storage::ProjectedRow::InitializeProjectedRow(layout, update_col_ids, update_buffer);
    testutil::GenerateRandomRow(update, layout, generator_, null_bias);

    // generate an undo DeltaRecord to populate on Insert
    undo_buffer = new byte[undo_size]; // safe to overprovision this
    undo_buffers[1] = undo_buffer;
    undo = storage::DeltaRecord::InitializeDeltaRecord(nullptr, timestamp_t(-1), layout, update_col_ids, undo_buffer);

    EXPECT_TRUE(table.Update(tuple, *update, undo));

    delete[] update_buffer;

    // another transaction attempts to write, should fail

    // generate a random update ProjectedRow to Update
    update_buffer = new byte[redo_size]; // safe to overprovision this
    update = storage::ProjectedRow::InitializeProjectedRow(layout, update_col_ids, update_buffer);
    testutil::GenerateRandomRow(update, layout, generator_, null_bias);

    // generate an undo DeltaRecord to populate on Insert
    undo_buffer = new byte[undo_size]; // safe to overprovision this
    undo_buffers[2] = undo_buffer;
    undo = storage::DeltaRecord::InitializeDeltaRecord(nullptr, timestamp_t(2), layout, update_col_ids, undo_buffer);

    EXPECT_FALSE(table.Update(tuple, *update, undo));

    delete[] update_buffer;

    // commit the transaction by changing the timestamp

    reinterpret_cast<storage::DeltaRecord *>(undo_buffers[1])->timestamp_ = 1;

    // another transaction attempts to write, should succeed

    // generate a random update ProjectedRow to Update
    EXPECT_TRUE(table.Update(tuple, *update, undo));

    // generate a redo ProjectedRow for Select
    byte *select_buffer = new byte[redo_size];
    storage::ProjectedRow *select_row = storage::ProjectedRow::InitializeProjectedRow(layout, col_ids, select_buffer);

    table.Select(0, tuple, select_row);

    EXPECT_TRUE(testutil::ProjectionListEqual(layout, select_row, insert));

    delete[] select_buffer;

    for (auto i : redo_buffers) {
      delete[] i;
    }

    for (auto i : undo_buffers) {
      delete[] i;
    }
  }
}
}  // namespace terrier
