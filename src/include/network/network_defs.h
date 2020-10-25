#pragma once

#include <vector>

#include "common/strong_typedef.h"

namespace noisepage::trafficcop {
class TrafficCop;
}

namespace noisepage::network {
class PostgresPacketWriter;
class ReadBuffer;

// This is to be stashed in a ConnectionContext as a unique identifier. This is really just the socket, but we don't
// want anyone using it to directly access the socket downstream
STRONG_TYPEDEF_HEADER(connection_id_t, uint16_t);

// Number of seconds to timeout on a client read
#define READ_TIMEOUT (20 * 60)

// Limit on the length of a packet
#define PACKET_LEN_LIMIT 2500000

// For all of the enums defined in this header, we will
// use this value to indicate that it is an invalid value
// I don't think it matters whether this is 0 or -1
constexpr int INVALID_TYPE_ID = 0;

//===--------------------------------------------------------------------===//
// Wire protocol typedefs
//===--------------------------------------------------------------------===//
#define SOCKET_BUFFER_CAPACITY 8192

/* byte type */
using uchar = unsigned char;

/* type for buffer of bytes */
using ByteBuf = std::vector<uchar>;

using NetworkCallback = void (*)(void *);

//===--------------------------------------------------------------------===//
// Network Message Types
//===--------------------------------------------------------------------===//

enum class NetworkMessageType : unsigned char {
  // Important: The character '0' is treated as a null message
  // That means we cannot have an invalid type
  NULL_COMMAND = '0',

  // Messages that don't have headers (like Startup message)
  NO_HEADER = 255,

  ////////////////////////////
  // Postgres message types //
  ////////////////////////////

  // Responses
  PG_PARSE_COMPLETE = '1',
  PG_BIND_COMPLETE = '2',
  PG_CLOSE_COMPLETE = '3',
  PG_COMMAND_COMPLETE = 'C',
  PG_PARAMETER_STATUS = 'S',
  PG_AUTHENTICATION_REQUEST = 'R',
  PG_NOTICE_RESPONSE = 'N',
  PG_ERROR_RESPONSE = 'E',
  PG_EMPTY_QUERY_RESPONSE = 'I',
  PG_NO_DATA_RESPONSE = 'n',
  PG_READY_FOR_QUERY = 'Z',
  PG_PARAMETER_DESCRIPTION = 't',
  PG_ROW_DESCRIPTION = 'T',
  PG_DATA_ROW = 'D',
  // Commands
  PG_EXECUTE_COMMAND = 'E',
  PG_SYNC_COMMAND = 'S',
  PG_TERMINATE_COMMAND = 'X',
  PG_DESCRIBE_COMMAND = 'D',
  PG_BIND_COMMAND = 'B',
  PG_PARSE_COMMAND = 'P',
  PG_SIMPLE_QUERY_COMMAND = 'Q',
  PG_CLOSE_COMMAND = 'C',

  ////////////////////////
  // ITP message types  //
  ////////////////////////
  ITP_REPLICATION_COMMAND = 'r',
  ITP_STOP_REPLICATION_COMMAND = 'e',
  ITP_COMMAND_COMPLETE = 'c',
};

enum class DescribeCommandObjectType : unsigned char { PORTAL = 'P', STATEMENT = 'S' };

// The TrafficCop logic relies on very specific ordering of these values. Reorder with care.
enum class QueryType : uint8_t {
  // Transaction statements
  QUERY_BEGIN,
  QUERY_COMMIT,
  QUERY_ROLLBACK,
  // DML
  QUERY_SELECT,
  QUERY_INSERT,
  QUERY_UPDATE,
  QUERY_DELETE,
  // DDL
  QUERY_CREATE_TABLE,
  QUERY_CREATE_DB,
  QUERY_CREATE_INDEX,
  QUERY_CREATE_TRIGGER,
  QUERY_CREATE_SCHEMA,
  QUERY_CREATE_VIEW,
  QUERY_DROP_TABLE,
  QUERY_DROP_DB,
  QUERY_DROP_INDEX,
  QUERY_DROP_TRIGGER,
  QUERY_DROP_SCHEMA,
  QUERY_DROP_VIEW,
  // Misc (non-transactional)
  QUERY_SET,
  // end of what we support in the traffic cop right now
  QUERY_RENAME,
  QUERY_ALTER,
  // Prepared statement stuff
  QUERY_DROP_PREPARED_STATEMENT,
  QUERY_PREPARE,
  QUERY_EXECUTE,
  // Misc
  QUERY_COPY,
  QUERY_ANALYZE,
  QUERY_SHOW,
  QUERY_OTHER,
  QUERY_EXPLAIN,
  QUERY_INVALID
};

enum class NetworkTransactionStateType : unsigned char {
  INVALID = static_cast<unsigned char>(INVALID_TYPE_ID),
  IDLE = 'I',   // Not in a transaction block
  BLOCK = 'T',  // In a transaction block
  FAIL = 'E',   // In a failed transaction
};

// postgres uses 0 for text, 1 for binary, so this is fine
enum class FieldFormat : bool { text = false, binary = true };

}  // namespace noisepage::network
