#pragma once

#include <string>
#include <vector>

#include "common/managed_pointer.h"
#include "network/network_defs.h"
#include "network/network_io_utils.h"
#include "network/packet_writer.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::execution::sql {
struct Val;
}

namespace noisepage::common {
class ErrorData;
}

namespace noisepage::network {
/**
 * Wrapper around an I/O layer WriteQueue to provide Postgres-specific
 * helper methods.
 */
class PostgresPacketWriter : public PacketWriter {
 public:
  /**
   * Normal constructor for PostgresPacketWriter
   * @param write_queue backing data structure for this packet writer
   */
  explicit PostgresPacketWriter(const common::ManagedPointer<WriteQueue> write_queue) : PacketWriter(write_queue) {}

  /**
   * Notify the client a readiness to receive a query
   * @param txn_status
   */
  void WriteReadyForQuery(NetworkTransactionStateType txn_status);

  /**
   * Writes response to startup message
   */
  void WriteStartupResponse();

  /**
   * Writes a simple query
   * @param query string to execute
   */
  void WriteSimpleQuery(const std::string &query);

  /**
   * Write an ErrorData object
   * @param error data to return to client
   */
  void WriteError(const common::ErrorData &error);

  /**
   * Writes an empty query response
   */
  void WriteEmptyQueryResponse();

  /**
   * Writes a no-data response
   */
  void WriteNoData();

  /**
   * Writes parameter description (used in Describe command)
   * @param param_types The types of the parameters in the statement
   */
  void WriteParameterDescription(const std::vector<type::TypeId> &param_types);

  /**
   * Writes row description, as the first packet of sending query results
   * @param columns the column information from the OutputSchema
   * @param field_formats vector formats for the attributes to write
   */
  void WriteRowDescription(const std::vector<planner::OutputSchema::Column> &columns,
                           const std::vector<FieldFormat> &field_formats);

  /**
   * Tells the client that the query command is complete.
   * @param tag records the which kind of query it is
   */
  void WriteCommandComplete(std::string_view tag);

  /**
   * Tells the client that the query command is complete.
   * @param tag records the which kind of query it is
   * @param num_rows number of rows affected by DML query
   */
  void WriteCommandComplete(std::string_view tag, uint32_t num_rows);

  /**
   * Writes Postgres command complete
   * @param query_type what type of query this was
   * @param num_rows number of rows for the queries that need it in their output
   */
  void WriteCommandComplete(QueryType query_type, uint32_t num_rows);

  /**
   * Writes a parse message packet
   * @param destinationStmt The name of the destination statement to parse
   * @param query The query string to be parsed
   * @param params Supplied parameter object types in the query
   */
  void WriteParseCommand(const std::string &destinationStmt, const std::string &query,
                         const std::vector<int32_t> &params);

  /**
   * Writes a Bind message packet
   * @param destinationPortal The portal to bind to
   * @param sourcePreparedStmt The name of the source prepared statement
   * @param paramFormatCodes Binary values format codes describing whether or not the parameters ins paramVals are in
   * text or binary form
   * @param paramVals The parameter values
   * @param resultFormatCodes The format codes to request the results to be formatted to. Same conventions as in
   * paramFormatCodes
   */
  void WriteBindCommand(const std::string &destinationPortal, const std::string &sourcePreparedStmt,
                        std::initializer_list<int16_t> paramFormatCodes,
                        std::initializer_list<std::vector<char> *> paramVals,
                        std::initializer_list<int16_t> resultFormatCodes);

  /**
   * Writes an Execute message packet
   * @param portal The name of the portal to execute
   * @param rowLimit Maximum number of rows to return to the client
   */
  void WriteExecuteCommand(const std::string &portal, int32_t rowLimit);

  /**
   * Writes a Sync message packet
   */
  void WriteSyncCommand();

  /**
   * Writes a Describe message packet
   * @param type The type of object to describe
   * @param objectName The name of the object to describe8
   */
  void WriteDescribeCommand(DescribeCommandObjectType type, const std::string &objectName);

  /**
   * Writes a Close command on an object
   * @param type The type of object to close
   * @param objectName The name of the object to close
   */
  void WriteCloseCommand(DescribeCommandObjectType type, const std::string &objectName);

  /**
   * Tells the client that the parse command is complete.
   */
  void WriteParseComplete();

  /**
   * Tells the client that the bind command is complete.
   */
  void WriteCloseComplete();

  /**
   * Tells the client that the bind command is complete.
   */
  void WriteBindComplete();

  /**
   * Write a data row from the execution engine back to the client
   * @param tuple pointer to the start of the row
   * @param columns OutputSchema describing the tuple
   * @param field_formats vector formats for the attributes to write
   */
  void WriteDataRow(const byte *tuple, const std::vector<planner::OutputSchema::Column> &columns,
                    const std::vector<FieldFormat> &field_formats);

 private:
  template <class native_type, class val_type>
  void WriteBinaryVal(const execution::sql::Val *val, type::TypeId type);

  template <class native_type, class val_type>
  void WriteBinaryValNeedsToNative(const execution::sql::Val *val, type::TypeId type);

  uint32_t WriteBinaryAttribute(const execution::sql::Val *val, type::TypeId type);

  /**
   * Write a data row in Postgres' text format coming from an OutputBuffer in the execution engine. Simple Query
   * messages always reply with text format data.
   * @param tuple pointer to the start of the row
   * @param columns OutputSchema describing the tuple
   */
  uint32_t WriteTextAttribute(const execution::sql::Val *val, type::TypeId type);
};

}  // namespace noisepage::network
