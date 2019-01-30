#include "storage/block_compactor.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"
#include "arrow/api.h"

namespace terrier {
// NOLINTNEXTLINE
TEST(BlockCompactorTest, SimpleTest) {
  storage::BlockStore block_store{1, 1};
  storage::RawBlock *block = block_store.Get();
  storage::BlockLayout layout({8, 8, VARLEN_COLUMN});
  storage::TupleAccessStrategy accessor(layout);
  accessor.InitializeRawBlock(block, storage::layout_version_t(0));

  storage::DataTable table(&block_store, layout, storage::layout_version_t(0));
  storage::RecordBufferSegmentPool buffer_pool{10000, 10000};
  transaction::TransactionManager txn_manager(&buffer_pool, false, LOGGING_DISABLED);

  storage::ProjectedRowInitializer initializer(layout, StorageTestUtil::ProjectionListAllColumns(layout));
  byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  auto *row = initializer.InitializeRow(buffer);

  for (uint32_t i = 0; i < 20; i++) {
    storage::TupleSlot slot;
    accessor.Allocate(block, &slot);
    if (slot.GetOffset() % 5 == 0) {
      *reinterpret_cast<byte **>(accessor.AccessForceNotNull(slot, storage::col_id_t(0))) = nullptr;
      auto *foo = new char[4];
      *reinterpret_cast<storage::VarlenEntry *>(accessor.AccessForceNotNull(slot, storage::col_id_t(1))) = {
          reinterpret_cast<byte *>(foo), 4, false};
      *reinterpret_cast<uint64_t *>(accessor.AccessForceNotNull(slot, storage::col_id_t(2))) = slot.GetOffset() / 5;
    } else {
      accessor.Deallocate(slot);
    }
  }
  block->insert_head_ = layout.NumSlots();

  storage::BlockCompactor compactor;
  compactor.PutInQueue({block, &table});
  compactor.ProcessCompactionQueue(&txn_manager);

  transaction::TransactionContext *txn = txn_manager.BeginTransaction();

  for (uint32_t i = 0; i < 4; i++) {
    table.Select(txn, {block, i}, row);
    StorageTestUtil::PrintRow(*row, layout);
  }
  for (uint32_t i = 4; i < 20; i++) {
    EXPECT_FALSE(table.Select(txn, {block, i}, row));
  }

  storage::ArrowBlockMetadata &arrow_metadata = accessor.GetArrowBlockMetadata(block);
  auto id_bitmap = std::make_shared<arrow::Buffer>(reinterpret_cast<uint8_t *>(
                                                       accessor.ColumnNullBitmap(block, storage::col_id_t(2))), 1);
  auto id_data = std::make_shared<arrow::Buffer>(reinterpret_cast<uint8_t *>(
                                                     accessor.ColumnStart(block, storage::col_id_t(2))), 32);
  auto id_array_data = arrow::ArrayData::Make(arrow::int64(),
                                              arrow_metadata.NumRecords(),
                                              {id_bitmap, id_data},
                                              arrow_metadata.NullCount(storage::col_id_t(2)));

  auto id_array = std::make_shared<arrow::Int64Array>(id_array_data);
//  auto id_array = std::make_shared<arrow::Array>();
//  id_array->SetData(id_array_data);

  auto varlen_bitmap =
      std::make_shared<arrow::Buffer>(reinterpret_cast<uint8_t *>(accessor.ColumnNullBitmap(block,
                                                                                            storage::col_id_t(1))), 1);
  storage::ArrowVarlenColumn varlen_col = arrow_metadata.GetVarlenColumn(layout, storage::col_id_t(1));
  auto varlen_offset = std::make_shared<arrow::Buffer>(reinterpret_cast<uint8_t *>(varlen_col.offsets_), 20);

  auto varlen_values_buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<uint8_t *>(varlen_col.values_), 16);
  auto varlen_values_array_data = arrow::ArrayData::Make(arrow::uint8(), 16, {NULLPTR, varlen_values_buffer}, 0);

  auto varlen_array_data = arrow::ArrayData::Make(arrow::utf8(),
                                                  arrow_metadata.NumRecords(),
                                                  {varlen_bitmap, varlen_offset},
                                                  {varlen_values_array_data},
                                                  arrow_metadata.NullCount(storage::col_id_t(1)));
  auto varlen_array = std::make_shared<arrow::Array>();
  varlen_array->SetData(id_array_data);

  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      arrow::field("id", arrow::int64()), arrow::field("varlen", arrow::utf8())};
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  auto arrow_table = arrow::Table::Make(schema, {std::static_pointer_cast<arrow::Array>(id_array), varlen_array});
  auto ids = std::static_pointer_cast<arrow::Int64Array>(arrow_table->column(0)->data()->chunk(0));
  for (uint32_t i = 0; i < arrow_table->num_rows(); i++) {
    printf("%llu\n", ids->Value(i));
  }
};

}  // namespace terrier
