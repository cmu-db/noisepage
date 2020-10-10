#pragma once
#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/generated/Message_generated.h>
#include <flatbuffers/generated/Schema_generated.h>

#include <string>
#include <unordered_map>
#include <vector>

#include "storage/arrow_block_metadata.h"
#include "storage/data_table.h"
#include "type/type_id.h"

namespace flatbuf = org::apache::arrow::flatbuf;

namespace terrier::storage {

/**
 * An Arrow Serializer is an auxiliary object bound to a data table so that the in-memory blocks
 * which are organized in arrow format can be exported to external storage in arrow IPC format.
 * The exported table can be read by other frameworks that are compatible with arrow, e.g., pandas.
 */
class ArrowSerializer {
 public:
  /**
   * Constructor of an arrow serializer. Each arrow serializer is bound to a specific data table. However,
   * a column can be interpreted with different types when exported, as is shown below (ExportTable).
   *
   * @param dataTable the data table to be serialized
   */
  explicit ArrowSerializer(const DataTable &dataTable) : data_table_(dataTable) {}

  /**
   * Dump a table to disk in arrow IPC format. The high-level IPC format is defined as below:
   *
   *    SCHEMA MESSAGE
   *    DICTIONARY MESSAGE 0
   *     ...
   *    DICTIONARY MESSAGE k - 1
   *    RECORDBATCH MESSAGE 0
   *     ...
   *    RECORDBATCH MESSAGE n - 1
   *
   * Where Dictionary messages and RecordBatch messages can interleave. Table data are primarily
   * stored in RecordBatch messages, each of which has the following format:
   *
   *    metadata_size: int32
   *    metadata_flatbuffer: bytes
   *    padding to 8-byte
   *    message body
   *
   * metadata_flatbuffer is the real Flatbuffer value. It primarily contains an array of Buffer metadata. Each element
   * of the array points out the length and position of a Buffer.
   *
   * Message body is the data. They are organized as several Buffers, where a Buffer corresponds to a continuous memory
   * block and is exactly the same concept as the Buffer above. Therefore, by parsing the metadata_flatbuffer, we can
   * pinpoint and read the corresponding data.
   *
   * @param file_name the file that the table will be exported to
   * @param col_types since in the data table level, we don't know the type of each column. We need to use this
   *        parameter that is provided to get the types of columns.
   */
  void ExportTable(const std::string &file_name, std::vector<terrier::type::TypeId> *col_types);

 private:
  const DataTable &data_table_;
  /**
   * WriteDataBlock write a memory block to file. Such a memory block can be: offsets of varlen column,
   * data of a fixed-size column, bitmap of a column, etc.
   * @param outfile the output file
   * @param src the memory block
   * @param len the size, will be padded to arrow alignment according to the specification
   */
  void WriteDataBlock(std::ofstream &outfile, const char *src, size_t len);

  /**
   * AddBufferInfo adds a buffer info to the buffers array. A buffer is a continuous memory region defined by its
   * length and offset.
   * @param offset the offset of current buffer. It will be updated by adding current len when return from this
   *               function
   * @param len the length of the buffer, will be padded to arrow alignment
   * @param buffers the array that gathers the buffer info
   */
  void AddBufferInfo(size_t *offset, size_t len, std::vector<flatbuf::Buffer> *buffers);

  /**
   * Build the metadata_flatbuffer from all its components and export it to the file.
   * @param outfile the output file
   * @param header_type one of MessageHeader_Schema, MessageHeader_RecordBatch, or MessageHeader_DictionaryBatch
   * @param header the auto-generated offset of the header
   * @param body_len the length of follwoing message body (not the length of this metadata_flatbuffer)
   * @param flatbuf_builder flatbuffer builder
   */
  void AssembleMetadataBuffer(std::ofstream &outfile, flatbuf::MessageHeader header_type,
                              flatbuffers::Offset<void> header, int64_t body_len,
                              flatbuffers::FlatBufferBuilder *flatbuf_builder);

  /**
   * This function write a Schema message. The Schema message provides metadata describing the following RecordBatch
   * messages (you may think of it as a batch of rows), which includes but not limited to the logical type of each
   * column, the byte-width, etc. It's serialized via Flatbuffer and should be written to the file at the very
   * beginning.
   *
   * For the detailed definition of Schema message and RecordBatch message, please refer to:
   *    https://arrow.apache.org/docs/format/Columnar.html
   *
   * For the flatbuffer schema of Schema message, please refer to:
   *    https://github.com/apache/arrow/blob/master/format/Schema.fbs
   *
   * @param outfile the output file
   * @param dictionary_ids The dictionary entries and the indices for a batch of rows are written seperately.
   *                       Therefore, when a column is dictionary-compressed, we need to assign an id to it,
   *                       so that the dictionary and the indices can be paired.
   * @param flatbuf_builder flatbuffer builder
   */
  void WriteSchemaMessage(std::ofstream &outfile, std::unordered_map<col_id_t, int64_t> *dictionary_ids,
                          std::vector<type::TypeId> *col_types, flatbuffers::FlatBufferBuilder *flatbuf_builder);

  /**
   * This function write a Dictionary message. The dictionary message is something contains the dictionary entries.
   * It can be reused by multiple following RecordBatch messages (you may think of it as a batch of rows).
   *
   * For the detailed definition of Dictionary message and RecordBatch message, please refer to:
   *    https://arrow.apache.org/docs/format/Columnar.html
   *
   * For the flatbuffer schema of Dictionary message, please refer to:
   *    https://github.com/apache/arrow/blob/master/format/Message.fbs
   *
   * @param outfile the output file
   * @param dictionary_id id of this dictionary, should have been assigned previously when writing the schema message.
   * @param varlen_col varlen_col stores the dictionray for a dictionary compressed column
   * @param flatbuf_builder flatbuffer builder
   */
  void WriteDictionaryMessage(std::ofstream &outfile, int64_t dictionary_id, const ArrowVarlenColumn &varlen_col,
                              flatbuffers::FlatBufferBuilder *flatbuf_builder);
};
}  // namespace terrier::storage
