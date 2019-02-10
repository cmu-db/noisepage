#include <arrow/api.h>
#include "storage/tuple_access_strategy.h"
#include "storage/storage_defs.h"
#include "storage/arrow_block_metadata.h"

namespace terrier::storage {
class ArrowUtil {
 public:
  ArrowUtil() = delete;

  // In general we should avoid using shared_ptrs in the storage layer. However, in the case of Arrow, since they
  // seem to use it liberally, and any block that goes through this code path is unlikely to be a contention hot spot,
  // we should be okay.
  static std::shared_ptr<arrow::Table> AssembleToArrowTable(const TupleAccessStrategy &accessor, RawBlock *block) {
    const storage::BlockLayout &layout = accessor.GetBlockLayout();
    std::vector<std::shared_ptr<arrow::Field>> schema_vector;
    std::vector<std::shared_ptr<arrow::Array>> table_vector;
    for (uint16_t i = NUM_RESERVED_COLUMNS; i < layout.NumColumns(); i++) {
      col_id_t col_id(i);
      // TODO(Tianyu): At some point, we want to do this up in the SQL layer so we get a meaningful name as well
      schema_vector.push_back(arrow::field("", InferArrowType(layout, col_id)));
      if (layout.IsVarlen(col_id))
        BuildVarlenColumn(accessor, block, col_id, &schema_vector, &table_vector);
      else
        BuildPrimitiveColumn(accessor, block, col_id, &schema_vector, &table_vector);
    }
    auto schema = std::make_shared<arrow::Schema>(schema_vector);
    return arrow::Table::Make(schema, table_vector);
  }

 private:
  // TODO(Tianyu):
  // This for now simply returns either a string if input is varlen or a integer of matching length, without regards to
  // potentially richer semantics (doubles, floats, timestamps, etc.). We can imagine that later this functionality be
  // moved up from storage to also encapsulate SQL layer concepts for better corresponsdence to SQL types.
  static std::shared_ptr<arrow::DataType> InferArrowType(const storage::BlockLayout &layout, col_id_t col_id) {
    // TODO(Tianyu): varlen types can map to things more than strings.
    if (layout.IsVarlen(col_id)) return arrow::utf8();
    switch (layout.AttrSize(col_id)) {
      case 1:return arrow::uint8();
      case 2:return arrow::uint16();
      case 4:return arrow::uint32();
      case 8:return arrow::uint64();
      default:throw std::runtime_error("Unexpected type size");
    }
  }

  static void BuildPrimitiveColumn(const TupleAccessStrategy &accessor,
                                   RawBlock *block,
                                   col_id_t col_id,
                                   std::vector<std::shared_ptr<arrow::Field>> *schema_vector,
                                   std::vector<std::shared_ptr<arrow::Array>> *table_vector) {
    TERRIER_ASSERT(!layout.IsVarlen(col_id), "Calling function for primitive column on a varlen column");
    const storage::BlockLayout &layout = accessor.GetBlockLayout();
    storage::ArrowBlockMetadata &metadata = accessor.GetArrowBlockMetadata(block);
    auto col_bitmap = std::make_shared<arrow::Buffer>(
        reinterpret_cast<uint8_t *>(accessor.ColumnNullBitmap(block, col_id)),
        common::RawBitmap::SizeInBytes(layout.NumSlots()));
    auto col_values = std::make_shared<arrow::Buffer>(reinterpret_cast<uint8_t *>(accessor.ColumnStart(block, col_id)),
                                                      layout.AttrSize(col_id) * layout.NumSlots());
    auto col_array_data = arrow::ArrayData::Make(InferArrowType(layout, col_id),
                                                 metadata.NumRecords(),
                                                 {col_bitmap, col_values},
                                                 metadata.NullCount(col_id));
    table_vector->push_back(arrow::MakeArray(col_array_data));
  }

  static void BuildVarlenColumn(const TupleAccessStrategy &accessor,
                                RawBlock *block,
                                col_id_t col_id,
                                std::vector<std::shared_ptr<arrow::Field>> *schema_vector,
                                std::vector<std::shared_ptr<arrow::Array>> *table_vector) {
    TERRIER_ASSERT(!layout.IsVarlen(col_id), "Calling function for primitive column on a varlen column");
    const storage::BlockLayout &layout = accessor.GetBlockLayout();
    storage::ArrowBlockMetadata &metadata = accessor.GetArrowBlockMetadata(block);
    storage::ArrowVarlenColumn &varlen_col = metadata.GetVarlenColumn(layout, col_id);
    auto col_bitmap = std::make_shared<arrow::Buffer>(
        reinterpret_cast<uint8_t *>(accessor.ColumnNullBitmap(block, col_id)),
        common::RawBitmap::SizeInBytes(layout.NumSlots()));
    auto varlen_offset = std::make_shared<arrow::Buffer>(reinterpret_cast<uint8_t *>(varlen_col.offsets_),
                                                         sizeof(uint32_t) * (metadata.NumRecords() + 1));
    auto varlen_values_buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<uint8_t *>(varlen_col.values_),
                                                                varlen_col.varlen_size_);
    auto varlen_values_array_data = arrow::ArrayData::Make(arrow::uint8(),
                                                           metadata.NumRecords(),
                                                           {NULLPTR, varlen_values_buffer},
                                                           0);
    table_vector->push_back(arrow::MakeArray(varlen_values_array_data));
  }

};
} // terrier::storage