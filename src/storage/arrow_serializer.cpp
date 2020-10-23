#include "storage/arrow_serializer.h"

#include <cstring>
#include <fstream>
#include <list>
#include <string>
#include <vector>

namespace noisepage::storage {

constexpr int32_t FLATBUF_CONTINUZATION = -1;
constexpr uint8_t ARROW_ALIGNMENT = 8;
constexpr char ALIGNMENT[8] = {0};
constexpr flatbuf::MetadataVersion METADATA_VERSION = flatbuf::MetadataVersion_V4;

void ArrowSerializer::WriteDataBlock(std::ofstream &outfile, const char *src, size_t len) {
  len = StorageUtil::PadUpToSize(ARROW_ALIGNMENT, len);
  outfile.write(src, len);
}

void ArrowSerializer::AddBufferInfo(size_t *offset, size_t len, std::vector<flatbuf::Buffer> *buffers) {
  len = StorageUtil::PadUpToSize(ARROW_ALIGNMENT, len);
  buffers->emplace_back(*offset, len);
  *offset += len;
}

void ArrowSerializer::AssembleMetadataBuffer(std::ofstream &outfile, flatbuf::MessageHeader header_type,
                                             flatbuffers::Offset<void> header, int64_t body_len,
                                             flatbuffers::FlatBufferBuilder *flatbuf_builder) {
  auto message = flatbuf::CreateMessage(*flatbuf_builder, METADATA_VERSION, header_type, header, body_len);
  flatbuf_builder->Finish(message);
  int32_t flatbuf_size = flatbuf_builder->GetSize();
  auto padded_flatbuf_size = StorageUtil::PadUpToSize(ARROW_ALIGNMENT, flatbuf_size);
  outfile.write(reinterpret_cast<const char *>(&FLATBUF_CONTINUZATION), sizeof(int32_t));
  outfile.write(reinterpret_cast<const char *>(&padded_flatbuf_size), sizeof(int32_t));
  outfile.write(reinterpret_cast<const char *>(flatbuf_builder->GetBufferPointer()), flatbuf_size);
  if (padded_flatbuf_size != static_cast<uint32_t>(flatbuf_size)) {
    outfile.write(ALIGNMENT, padded_flatbuf_size - flatbuf_size);
  }
  outfile.flush();
}

void ArrowSerializer::WriteSchemaMessage(std::ofstream &outfile, std::unordered_map<col_id_t, int64_t> *dictionary_ids,
                                         std::vector<type::TypeId> *col_types,
                                         flatbuffers::FlatBufferBuilder *flatbuf_builder) {
  RawBlock *block = *data_table_.GetBlocks().begin();
  const BlockLayout &layout = data_table_.GetBlockLayout();
  ArrowBlockMetadata &metadata = data_table_.accessor_.GetArrowBlockMetadata(block);
  std::vector<flatbuffers::Offset<flatbuf::Field>> fields;
  int64_t dictionary_id = 0;

  // For each column, write its metadata into the schema message that will be parsed at the very beginning when
  // reading a exported table file.
  //
  // Such metadata includes:
  // 1. Name of the column;
  // 2. Logical Type of the column (fixed length, varlen, or dictionary compressed);
  //    2.1. If it's fixed length, then its byte width will be included;
  //    2.2. If it's dictionary compressed, then we pre-assign an id for the dictionary. Its real dictionary will
  //         be built later with the id.
  for (col_id_t col_id : layout.AllColumns()) {
    ArrowColumnInfo &col_info = metadata.GetColumnInfo(layout, col_id);
    // TODO(Yuze): Change column name when we have the information, which we may need from upper layers.
    auto name = flatbuf_builder->CreateString("Col" + std::to_string(col_id.UnderlyingValue()));
    flatbuf::Type type;
    flatbuffers::Offset<void> type_offset;
    flatbuffers::Offset<flatbuf::DictionaryEncoding> dictionary = 0;
    if (!layout.IsVarlen(col_id) || col_info.Type() == ArrowColumnType::FIXED_LENGTH) {
      uint8_t byte_width = data_table_.accessor_.GetBlockLayout().AttrSize(col_id);
      switch ((*col_types)[col_id.UnderlyingValue()]) {
        case type::TypeId::BOOLEAN:
          type = flatbuf::Type_Bool;
          type_offset = flatbuf::CreateBool(*flatbuf_builder).Union();
          break;
        case type::TypeId::TINYINT:
          NOISEPAGE_FALLTHROUGH;
        case type::TypeId::SMALLINT:
          NOISEPAGE_FALLTHROUGH;
        case type::TypeId::INTEGER:
          NOISEPAGE_FALLTHROUGH;
        case type::TypeId::BIGINT:
          type = flatbuf::Type_Int;
          type_offset = flatbuf::CreateInt(*flatbuf_builder, 8 * byte_width, true).Union();
          break;
        case type::TypeId::TIMESTAMP:
          type = flatbuf::Type_Timestamp;
          type_offset = flatbuf::CreateTimestamp(*flatbuf_builder, flatbuf::TimeUnit_MICROSECOND).Union();
          break;
        case type::TypeId::DECIMAL:
          type = flatbuf::Type_Decimal;
          type_offset = flatbuf::CreateFloatingPoint(*flatbuf_builder, flatbuf::Precision_DOUBLE).Union();
          break;
        default:
          throw std::runtime_error("unexpected column type");
      }
    } else {
      switch (col_info.Type()) {
        case ArrowColumnType::DICTIONARY_COMPRESSED:
          dictionary = flatbuf::CreateDictionaryEncoding(
              *flatbuf_builder, dictionary_id, flatbuf::CreateInt(*flatbuf_builder, 8 * sizeof(uint64_t), true), false);
          dictionary_ids->emplace(col_id, dictionary_id++);
          NOISEPAGE_FALLTHROUGH;
        case ArrowColumnType::GATHERED_VARLEN:
          type = flatbuf::Type_LargeBinary;
          type_offset = flatbuf::CreateLargeBinary(*flatbuf_builder).Union();
          break;
        default:
          throw std::runtime_error("unexpected control flow");
      }
    }
    // Apache Arrow supports nested logical types. For example, for type List<Int64>, the parent type is List,
    // and its children type is Int64. Another example, for type List<List<Int64>>, List<Int64> is the children
    // of the outer List, and Int64 is the children of the inner List. Since we don't have nested types, we use
    // an empty array as fake children.
    std::vector<flatbuffers::Offset<flatbuf::Field>> fake_children;
    fields.emplace_back(flatbuf::CreateField(*flatbuf_builder, name, true, type, type_offset, dictionary,
                                             flatbuf_builder->CreateVector(fake_children)));
  }

  auto schema =
      flatbuf::CreateSchema(*flatbuf_builder, flatbuf::Endianness_Little, flatbuf_builder->CreateVector(fields));
  AssembleMetadataBuffer(outfile, flatbuf::MessageHeader_Schema, schema.Union(), 0, flatbuf_builder);
}

void ArrowSerializer::WriteDictionaryMessage(std::ofstream &outfile, int64_t dictionary_id,
                                             const ArrowVarlenColumn &varlen_col,
                                             flatbuffers::FlatBufferBuilder *flatbuf_builder) {
  std::vector<flatbuf::FieldNode> field_nodes;
  std::vector<flatbuf::Buffer> buffers;
  uint32_t num_elements = varlen_col.OffsetsLength() - 1;
  size_t buffer_offset = 0;
  field_nodes.emplace_back(num_elements, 0);

  // Add a fake validity buffer. This is required by flatbuffer. The dictionary message is
  // actually a warpped recordbatch message, i.e., the dictionary entries are written
  // in one RecordBatch. RecordBatch is something requires a validity buffer
  buffers.emplace_back(buffer_offset, 0);

  AddBufferInfo(&buffer_offset, varlen_col.OffsetsLength() * sizeof(uint64_t), &buffers);

  AddBufferInfo(&buffer_offset, varlen_col.ValuesLength(), &buffers);

  auto record_batch =
      flatbuf::CreateRecordBatch(*flatbuf_builder, num_elements, flatbuf_builder->CreateVectorOfStructs(field_nodes),
                                 flatbuf_builder->CreateVectorOfStructs(buffers));
  auto dictionary_batch = flatbuf::CreateDictionaryBatch(*flatbuf_builder, dictionary_id, record_batch);
  auto aligned_offset = StorageUtil::PadUpToSize(ARROW_ALIGNMENT, buffer_offset);
  AssembleMetadataBuffer(outfile, flatbuf::MessageHeader_DictionaryBatch, dictionary_batch.Union(), aligned_offset,
                         flatbuf_builder);
  WriteDataBlock(outfile, reinterpret_cast<const char *>(varlen_col.Offsets()),
                 varlen_col.OffsetsLength() * sizeof(uint64_t));

  WriteDataBlock(outfile, reinterpret_cast<const char *>(varlen_col.Values()), varlen_col.ValuesLength());

  outfile.flush();
}

void ArrowSerializer::ExportTable(const std::string &file_name, std::vector<type::TypeId> *col_types) {
  flatbuffers::FlatBufferBuilder flatbuf_builder;
  std::ofstream outfile(file_name, std::ios_base::out | std::ios_base::trunc | std::ios_base::binary);
  std::unordered_map<col_id_t, int64_t> dictionary_ids;
  WriteSchemaMessage(outfile, &dictionary_ids, col_types, &flatbuf_builder);

  const BlockLayout &layout = data_table_.accessor_.GetBlockLayout();
  auto column_ids = layout.AllColumns();

  for (auto it : data_table_.GetBlocks()) {  // NOLINT
    RawBlock *block = it;
    std::vector<flatbuf::FieldNode> field_nodes;
    std::vector<flatbuf::Buffer> buffers;

    // Make sure varlen columns have correct data when reading
    while (!block->controller_.TryAcquireInPlaceRead()) {
    }
    ArrowBlockMetadata &metadata = data_table_.accessor_.GetArrowBlockMetadata(block);
    uint32_t num_slots = metadata.NumRecords();

    size_t buffer_offset = 0;
    size_t column_id_size = column_ids.size();

    // First pass, write metadata_flatbuffer
    for (size_t i = 0; i < column_id_size; ++i) {
      auto col_id = column_ids[i];
      common::RawConcurrentBitmap *column_bitmap = data_table_.accessor_.ColumnNullBitmap(block, col_id);
      std::byte *column_start = data_table_.accessor_.ColumnStart(block, col_id);

      ArrowColumnInfo &col_info = metadata.GetColumnInfo(layout, col_id);
      field_nodes.emplace_back(num_slots, metadata.NullCount(col_id));

      AddBufferInfo(&buffer_offset,
                    reinterpret_cast<uintptr_t>(column_start) - reinterpret_cast<uintptr_t>(column_bitmap), &buffers);
      if (layout.IsVarlen(col_id) && !(col_info.Type() == ArrowColumnType::FIXED_LENGTH)) {
        switch (col_info.Type()) {
          case ArrowColumnType::GATHERED_VARLEN: {
            ArrowVarlenColumn &varlen_col = col_info.VarlenColumn();
            AddBufferInfo(&buffer_offset, varlen_col.OffsetsLength() * sizeof(uint64_t), &buffers);
            AddBufferInfo(&buffer_offset, varlen_col.ValuesLength(), &buffers);
            break;
          }
          case ArrowColumnType::DICTIONARY_COMPRESSED: {
            ArrowVarlenColumn &varlen_col = col_info.VarlenColumn();
            WriteDictionaryMessage(outfile, dictionary_ids[col_id], varlen_col, &flatbuf_builder);
            AddBufferInfo(&buffer_offset, num_slots * sizeof(uint64_t), &buffers);
            break;
          }
          default:
            throw std::runtime_error("unexpected control flow");
        }
      } else {
        int32_t cur_buffer_len;
        // Calculate the length of the data region of current column. For the columns except the last one, we calculate
        // their length by using the start of next column's bit map - the start of current column's data. For the
        // last column, we calculate the length by using the beginning address of the next block - the start of current
        // column data.
        if (i == column_id_size - 1) {
          auto casted_column_start = reinterpret_cast<uintptr_t>(column_start);
          uintptr_t mask = common::Constants::BLOCK_SIZE - 1;
          cur_buffer_len = ((casted_column_start + mask) & (~mask)) - casted_column_start;
        } else {
          cur_buffer_len =
              reinterpret_cast<uintptr_t>(data_table_.accessor_.ColumnNullBitmap(block, column_ids[i + 1])) -
              reinterpret_cast<uintptr_t>(column_start);
        }
        AddBufferInfo(&buffer_offset, cur_buffer_len, &buffers);
      }
    }
    auto record_batch =
        flatbuf::CreateRecordBatch(flatbuf_builder, num_slots, flatbuf_builder.CreateVectorOfStructs(field_nodes),
                                   flatbuf_builder.CreateVectorOfStructs(buffers));
    auto aligned_offset = StorageUtil::PadUpToSize(ARROW_ALIGNMENT, buffer_offset);
    AssembleMetadataBuffer(outfile, flatbuf::MessageHeader_RecordBatch, record_batch.Union(), aligned_offset,
                           &flatbuf_builder);

    // Second pass, write data.
    for (size_t i = 0; i < column_id_size; ++i) {
      auto col_id = column_ids[i];
      common::RawConcurrentBitmap *column_bitmap = data_table_.accessor_.ColumnNullBitmap(block, col_id);
      std::byte *column_start = data_table_.accessor_.ColumnStart(block, col_id);

      ArrowColumnInfo &col_info = metadata.GetColumnInfo(layout, col_id);
      field_nodes.emplace_back(num_slots, metadata.NullCount(col_id));

      WriteDataBlock(outfile, reinterpret_cast<const char *>(column_bitmap),
                     reinterpret_cast<uintptr_t>(column_start) - reinterpret_cast<uintptr_t>(column_bitmap));

      if (layout.IsVarlen(col_id) && !(col_info.Type() == ArrowColumnType::FIXED_LENGTH)) {
        switch (col_info.Type()) {
          case ArrowColumnType::GATHERED_VARLEN: {
            ArrowVarlenColumn &varlen_col = col_info.VarlenColumn();
            WriteDataBlock(outfile, reinterpret_cast<const char *>(varlen_col.Offsets()),
                           varlen_col.OffsetsLength() * sizeof(uint64_t));
            WriteDataBlock(outfile, reinterpret_cast<const char *>(varlen_col.Values()), varlen_col.ValuesLength());
            break;
          }
          case ArrowColumnType::DICTIONARY_COMPRESSED: {
            auto indices = col_info.Indices();
            WriteDataBlock(outfile, reinterpret_cast<const char *>(indices), num_slots * sizeof(uint64_t));
            break;
          }
          default:
            throw std::runtime_error("unexpected control flow");
        }
      } else {
        int32_t cur_buffer_len;
        if (i == column_id_size - 1) {
          auto casted_column_start = reinterpret_cast<uintptr_t>(column_start);
          uintptr_t mask = common::Constants::BLOCK_SIZE - 1;
          cur_buffer_len = ((casted_column_start + mask) & (~mask)) - casted_column_start;
        } else {
          cur_buffer_len =
              reinterpret_cast<uintptr_t>(data_table_.accessor_.ColumnNullBitmap(block, column_ids[i + 1])) -
              reinterpret_cast<uintptr_t>(column_start);
        }
        WriteDataBlock(outfile, reinterpret_cast<const char *>(column_start), cur_buffer_len);
      }
    }
    outfile.flush();
    block->controller_.ReleaseInPlaceRead();
  }
  outfile.close();
}
}  // namespace noisepage::storage
