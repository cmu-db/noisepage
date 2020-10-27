#include <fstream>
#include <unordered_map>
#include <vector>

#include "common/hash_util.h"
#include "storage/arrow_serializer.h"
#include "storage/block_access_controller.h"
#include "storage/block_compactor.h"
#include "storage/garbage_collector.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/deferred_action_manager.h"

#define EXPORT_TEST_EXPORT_TABLE_NAME "test_export_table_test_table.arrow"
#define EXPORT_TEST_CSV_TABLE_NAME "test_export_table_test_table.csv"
#define EXPORT_TEST_PYSCRIPT_NAME "test_export_table_test_transform_table.py"
#define EXPORT_TEST_PYSCRIPT                                      \
  "import pyarrow as pa\n"                                        \
  "pa_table = pa.ipc.open_stream('" EXPORT_TEST_EXPORT_TABLE_NAME \
  "').read_next_batch()\n"                                        \
  "pa_table = pa_table.to_pandas()\n"                             \
  "pa_table.to_csv('" EXPORT_TEST_CSV_TABLE_NAME "', index=False, header=False)\n"

namespace noisepage {

struct ExportTableTest : public ::noisepage::TerrierTest {
  // This function decodes a utf-8 string from a csv file char by char
  // Example:
  //             \xc8\x99\r&x""
  // will be decoded to:
  //            192    153    13   38  120  34
  //           (\xc8) (\x99) (\r) (&)  (x)  ("")
  bool ParseNextChar(std::ifstream &csv_file, char stop_char, char *tmp_char) {
    csv_file.get(*tmp_char);
    bool end = false;
    switch (*tmp_char) {
      case '\\':
        // The next char starts with \. We know there are two possible cases:
        //   1. for a char with value > 127, it's begin with \x
        //   2. for special chars, it uses \ as escape character
        csv_file.get(*tmp_char);
        if (*tmp_char == 'x') {
          // \x, simply parse the hexadecimal number
          char hex_char = '\0';
          csv_file.get(hex_char);
          *tmp_char = static_cast<char>((hex_char >= 'a' ? (hex_char - 'a' + 10) : (hex_char - '0')) * 16);
          csv_file.get(hex_char);
          *tmp_char = static_cast<char>(*tmp_char + (hex_char >= 'a' ? (hex_char - 'a' + 10) : hex_char - '0'));
        } else {
          // \ is an escape character
          switch (*tmp_char) {
            case '\\':
              *tmp_char = '\\';
              break;
            case 'r':
              *tmp_char = '\r';
              break;
            case 't':
              *tmp_char = '\t';
              break;
            case 'n':
              *tmp_char = '\n';
              break;
          }
        }
        break;
      case '"':
        // in the csv file, quote char is used to identify an item when the item itself contains comma.
        // So, when we encounter a single " followed by a comma or \n, we know current item has been
        // completely parsed. Since "" will be parsed as ", so if " is followed by another ", then
        // we get an " (tmp_char is ") but we know there are still more remaining chars to parse (end = false).
        csv_file.get(*tmp_char);
        if (*tmp_char == ',' || *tmp_char == '\n') {
          end = true;
        }
        break;
      case ',':
        // We just need to distinguish if the , is a real comma or is a delimiter. stop_char indicates if current
        // item is identified by comma (instead of quote).
        if (stop_char == ',') {
          end = true;
        }
        break;
      case '\n':
        end = true;
        break;
      default:
        // Normal cases, we get a general ascii char.
        break;
    }
    return end;
  }

  void CheckContent(std::ifstream &csv_file, transaction::TransactionManager *txn_manager,
                    const storage::BlockLayout &layout,
                    const std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> &tuples,
                    const storage::DataTable &table, storage::RawBlock *block) {
    auto initializer =
        storage::ProjectedRowInitializer::Create(layout, StorageTestUtil::ProjectionListAllColumns(layout));
    byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    auto *read_row = initializer.InitializeRow(buffer);
    // This transaction is guaranteed to start after the compacting one commits
    transaction::TransactionContext *txn = txn_manager->BeginTransaction();
    int num_tuples = tuples.size();
    // For all the rows.
    for (int i = 0; i < static_cast<int>(layout.NumSlots()); i++) {
      storage::TupleSlot slot(block, i);
      if (i < num_tuples) {
        table.Select(common::ManagedPointer(txn), slot, read_row);
        // For all the columns of a row
        for (int j = 0; j < read_row->NumColumns(); j++) {
          auto col_id = read_row->ColumnIds()[j];

          // We try to parse the current column (item)
          std::string integer;
          std::vector<byte> bytes;
          char tmp_char;
          csv_file.get(tmp_char);
          switch (tmp_char) {
            case '"':
              // Current column is a utf-8 string delimited by " or a row with a single null value
              csv_file.seekg(1, std::ios_base::cur);
              while (!ParseNextChar(csv_file, '"', &tmp_char)) {
                bytes.emplace_back(static_cast<byte>(tmp_char));
              }
              break;
            case 'b':
              // Current column is a normal utf-8 string
              while (!ParseNextChar(csv_file, ',', &tmp_char)) {
                bytes.emplace_back(static_cast<byte>(tmp_char));
              }
              break;
            case ',':
              // there is nothing between two commas. It indicates a null value
              break;
            case '\n':
              // Similarly, nothing between the previous comma and current \n. It indicates a null value
              break;
            default:
              // Integer value
              integer = tmp_char;
              std::string remaining_digits;
              if (j == read_row->NumColumns() - 1) {
                std::getline(csv_file, remaining_digits, '\n');
              } else {
                std::getline(csv_file, remaining_digits, ',');
              }
              integer += remaining_digits;
              break;
          }
          auto data = read_row->AccessWithNullCheck(j);
          if (data == nullptr) {
            // Expect null value
            EXPECT_TRUE(bytes.empty() && integer.length() == 0)
                << "Value of row " << i << " column " << j << " should be null.\n";
          } else {
            if (layout.IsVarlen(col_id)) {
              auto *varlen = reinterpret_cast<storage::VarlenEntry *>(data);
              auto content = varlen->Content();
              auto content_len = varlen->Size();
              EXPECT_EQ(content_len, bytes.size() - 2)
                  << "Value of row " << i << " column " << j << " should have length " << content_len << "\n";

              // the first and last element of bytes are always useless.
              for (int k = 0; k < static_cast<int>(content_len); ++k) {
                EXPECT_EQ(bytes[k + 1], content[k])
                    << "The " << k << "th char of row " << i << " column " << j << " should be "
                    << static_cast<uint8_t>(content[k]) << " instead of " << static_cast<uint8_t>(bytes[k + 1]) << "\n";
              }
            } else {
              int64_t true_integer = 0;
              // Extract the true integer (generated in C++) based on their bit-length
              switch (layout.AttrSize(col_id)) {
                case 1:
                  true_integer = *reinterpret_cast<int8_t *>(data);
                  break;
                case 2:
                  true_integer = *reinterpret_cast<int16_t *>(data);
                  break;
                case 4:
                  true_integer = *reinterpret_cast<int32_t *>(data);
                  break;
                case 8:
                  true_integer = *reinterpret_cast<int64_t *>(data);
                  break;
                default:
                  throw NOT_IMPLEMENTED_EXCEPTION("Unsupported Attribute Size.");
              }
              EXPECT_TRUE(std::fabs(1 - (std::stof(integer) + 1e-6) / (true_integer + 1e-6)) < 1e-6)
                  << "Value of row " << i << " column " << j << " should be " << true_integer << " instead of "
                  << integer << "\n";
            }
          }
        }
      }
    }
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    delete[] buffer;
  }

  storage::BlockStore block_store_{5000, 5000};
  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool buffer_pool_{100000, 100000};
  double percent_empty_ = 0.5;
};

// NOLINTNEXTLINE
TEST_F(ExportTableTest, ExportDictionaryCompressedTableTest) {
  unlink(EXPORT_TEST_EXPORT_TABLE_NAME);
  unlink(EXPORT_TEST_CSV_TABLE_NAME);
  unlink(EXPORT_TEST_PYSCRIPT_NAME);
  std::ofstream outfile(EXPORT_TEST_PYSCRIPT_NAME, std::ios_base::out);
  outfile << EXPORT_TEST_PYSCRIPT;
  outfile.close();
  auto seed_chosen =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  generator_.seed(seed_chosen);
  storage::BlockLayout layout = StorageTestUtil::RandomLayoutWithVarlens(100, &generator_);
  storage::TupleAccessStrategy accessor(layout);
  // Technically, the block above is not "in" the table, but since we don't sequential scan that does not matter
  storage::DataTable table(common::ManagedPointer<storage::BlockStore>(&block_store_), layout,
                           storage::layout_version_t(0));
  storage::RawBlock *block = *table.GetBlocks().begin();
  accessor.InitializeRawBlock(&table, block, storage::layout_version_t(0));

  // Enable GC to cleanup transactions started by the block compactor
  transaction::TimestampManager timestamp_manager;
  transaction::DeferredActionManager deferred_action_manager{common::ManagedPointer(&timestamp_manager)};
  transaction::TransactionManager txn_manager{common::ManagedPointer(&timestamp_manager),
                                              common::ManagedPointer(&deferred_action_manager),
                                              common::ManagedPointer(&buffer_pool_), true, DISABLED};
  storage::GarbageCollector gc{common::ManagedPointer(&timestamp_manager),
                               common::ManagedPointer(&deferred_action_manager), common::ManagedPointer(&txn_manager),
                               DISABLED};
  auto tuples = StorageTestUtil::PopulateBlockRandomly(&table, block, percent_empty_, &generator_);

  // Manually populate the block header's arrow metadata for test initialization
  auto &arrow_metadata = accessor.GetArrowBlockMetadata(block);

  std::vector<type::TypeId> column_types;
  column_types.resize(layout.NumColumns());

  for (storage::col_id_t col_id : layout.AllColumns()) {
    if (layout.IsVarlen(col_id)) {
      arrow_metadata.GetColumnInfo(layout, col_id).Type() = storage::ArrowColumnType::DICTIONARY_COMPRESSED;
      column_types[col_id.UnderlyingValue()] = type::TypeId::VARCHAR;
    } else {
      arrow_metadata.GetColumnInfo(layout, col_id).Type() = storage::ArrowColumnType::FIXED_LENGTH;
      column_types[col_id.UnderlyingValue()] = type::TypeId::INTEGER;
    }
  }

  storage::BlockCompactor compactor;
  compactor.PutInQueue(block);
  compactor.ProcessCompactionQueue(&deferred_action_manager, &txn_manager);  // compaction pass

  // Need to prune the version chain in order to make sure that the second pass succeeds
  gc.PerformGarbageCollection();
  compactor.PutInQueue(block);
  compactor.ProcessCompactionQueue(&deferred_action_manager, &txn_manager);  // gathering pass

  storage::ArrowSerializer arrow_serializer(table);
  arrow_serializer.ExportTable(EXPORT_TEST_EXPORT_TABLE_NAME, &column_types);
  EXPECT_EQ(system((std::string("python3 ") + EXPORT_TEST_PYSCRIPT_NAME).c_str()), 0);

  std::ifstream csv_file(EXPORT_TEST_CSV_TABLE_NAME, std::ios_base::in);
  CheckContent(csv_file, &txn_manager, layout, tuples, table, block);
  csv_file.close();

  if (::testing::Test::HasFailure()) {
    std::string error_csv_file_name(EXPORT_TEST_EXPORT_TABLE_NAME);
    error_csv_file_name = error_csv_file_name + "_" + std::to_string(seed_chosen);
    rename(EXPORT_TEST_EXPORT_TABLE_NAME, error_csv_file_name.c_str());
    STORAGE_LOG_WARN("Error happened, see detailed exported data file {}. You may use the same seed suffix to debug.",
                     error_csv_file_name);
  } else {
    unlink(EXPORT_TEST_EXPORT_TABLE_NAME);
  }
  for (auto &entry : tuples) delete[] reinterpret_cast<byte *>(entry.second);  // reclaim memory used for bookkeeping
  gc.PerformGarbageCollection();
  gc.PerformGarbageCollection();  // Second call to deallocate
}

// NOLINTNEXTLINE
TEST_F(ExportTableTest, ExportVarlenTableTest) {
  unlink(EXPORT_TEST_EXPORT_TABLE_NAME);
  unlink(EXPORT_TEST_CSV_TABLE_NAME);
  unlink(EXPORT_TEST_PYSCRIPT_NAME);
  std::ofstream outfile(EXPORT_TEST_PYSCRIPT_NAME, std::ios_base::out);
  outfile << EXPORT_TEST_PYSCRIPT;
  outfile.close();
  auto seed_chosen =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  generator_.seed(seed_chosen);
  storage::BlockLayout layout = StorageTestUtil::RandomLayoutWithVarlens(100, &generator_);
  storage::TupleAccessStrategy accessor(layout);
  // Technically, the block above is not "in" the table, but since we don't sequential scan that does not matter
  storage::DataTable table(common::ManagedPointer<storage::BlockStore>(&block_store_), layout,
                           storage::layout_version_t(0));
  storage::RawBlock *block = *table.GetBlocks().begin();
  accessor.InitializeRawBlock(&table, block, storage::layout_version_t(0));

  // Enable GC to cleanup transactions started by the block compactor
  transaction::TimestampManager timestamp_manager;
  transaction::DeferredActionManager deferred_action_manager{common::ManagedPointer(&timestamp_manager)};
  transaction::TransactionManager txn_manager{common::ManagedPointer(&timestamp_manager),
                                              common::ManagedPointer(&deferred_action_manager),
                                              common::ManagedPointer(&buffer_pool_), true, DISABLED};
  storage::GarbageCollector gc{common::ManagedPointer(&timestamp_manager),
                               common::ManagedPointer(&deferred_action_manager), common::ManagedPointer(&txn_manager),
                               DISABLED};
  auto tuples = StorageTestUtil::PopulateBlockRandomly(&table, block, percent_empty_, &generator_);

  // Manually populate the block header's arrow metadata for test initialization
  auto &arrow_metadata = accessor.GetArrowBlockMetadata(block);

  std::vector<type::TypeId> column_types;
  column_types.resize(layout.NumColumns());

  for (storage::col_id_t col_id : layout.AllColumns()) {
    if (layout.IsVarlen(col_id)) {
      arrow_metadata.GetColumnInfo(layout, col_id).Type() = storage::ArrowColumnType::GATHERED_VARLEN;
      column_types[col_id.UnderlyingValue()] = type::TypeId::VARCHAR;
    } else {
      arrow_metadata.GetColumnInfo(layout, col_id).Type() = storage::ArrowColumnType::FIXED_LENGTH;
      column_types[col_id.UnderlyingValue()] = type::TypeId::INTEGER;
    }
  }

  storage::BlockCompactor compactor;
  compactor.PutInQueue(block);
  compactor.ProcessCompactionQueue(&deferred_action_manager, &txn_manager);  // compaction pass

  // Need to prune the version chain in order to make sure that the second pass succeeds
  gc.PerformGarbageCollection();
  compactor.PutInQueue(block);
  compactor.ProcessCompactionQueue(&deferred_action_manager, &txn_manager);  // gathering pass

  storage::ArrowSerializer arrow_serializer(table);
  arrow_serializer.ExportTable(EXPORT_TEST_EXPORT_TABLE_NAME, &column_types);
  EXPECT_EQ(system((std::string("python3 ") + EXPORT_TEST_PYSCRIPT_NAME).c_str()), 0);

  std::ifstream csv_file(EXPORT_TEST_CSV_TABLE_NAME, std::ios_base::in);
  CheckContent(csv_file, &txn_manager, layout, tuples, table, block);
  csv_file.close();

  if (::testing::Test::HasFailure()) {
    std::string error_csv_file_name(EXPORT_TEST_EXPORT_TABLE_NAME);
    error_csv_file_name = error_csv_file_name + "_" + std::to_string(seed_chosen);
    rename(EXPORT_TEST_EXPORT_TABLE_NAME, error_csv_file_name.c_str());
    STORAGE_LOG_WARN("Error happened, see detailed exported data file {}. You may use the same seed suffix to debug.",
                     error_csv_file_name);
  } else {
    unlink(EXPORT_TEST_EXPORT_TABLE_NAME);
  }
  for (auto &entry : tuples) delete[] reinterpret_cast<byte *>(entry.second);  // reclaim memory used for bookkeeping
  gc.PerformGarbageCollection();
  gc.PerformGarbageCollection();  // Second call to deallocate.
}

}  // namespace noisepage
