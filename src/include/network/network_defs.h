#pragma once

#include <unistd.h>
#include <bitset>
#include <climits>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/macros.h"
#include "traffic_cop/result_set.h"
#include "type/type_id.h"

namespace terrier::trafficcop {
class TrafficCop;
}

namespace terrier::network {
class PostgresPacketWriter;
class ReadBuffer;

// For threads
#define CONNECTION_THREAD_COUNT 4

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

using NetworkCallback = std::function<void(void)>;

using SimpleQueryCallback = std::function<void(const trafficcop::ResultSet &, network::PostgresPacketWriter *)>;

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
  PG_ERROR_RESPONSE = 'E',
  PG_EMPTY_QUERY_RESPONSE = 'I',
  PG_NO_DATA_RESPONSE = 'n',
  PG_READY_FOR_QUERY = 'Z',
  PG_PARAMETER_DESCRIPTION = 't',
  PG_ROW_DESCRIPTION = 'T',
  PG_DATA_ROW = 'D',
  // Errors
  PG_HUMAN_READABLE_ERROR = 'M',
  PG_SQLSTATE_CODE_ERROR = 'C',
  // Commands
  PG_EXECUTE_COMMAND = 'E',
  PG_SYNC_COMMAND = 'S',
  PG_TERMINATE_COMMAND = 'X',
  PG_DESCRIBE_COMMAND = 'D',
  PG_BIND_COMMAND = 'B',
  PG_PARSE_COMMAND = 'P',
  PG_SIMPLE_QUERY_COMMAND = 'Q',
  PG_CLOSE_COMMAND = 'C',
  // SSL willingness
  PG_SSL_YES = 'S',
  PG_SSL_NO = 'N',

  ////////////////////////
  // ITP message types  //
  ////////////////////////
  ITP_REPLICATION_COMMAND = 'r',
  ITP_STOP_REPLICATION_COMMAND = 'e',
  ITP_COMMAND_COMPLETE = 'c',
};

//===--------------------------------------------------------------------===//
// Network packet defs
//===--------------------------------------------------------------------===//

//===--------------------------------------------------------------------===//
// Describe Message Types
//===--------------------------------------------------------------------===//

enum class DescribeCommandObjectType : unsigned char { PORTAL = 'P', STATEMENT = 'S' };

//===--------------------------------------------------------------------===//
// Query Types
//===--------------------------------------------------------------------===//

enum class QueryType {
  QUERY_BEGIN = 0,         // begin query
  QUERY_COMMIT = 1,        // commit query
  QUERY_ROLLBACK = 2,      // rollback query
  QUERY_CREATE_TABLE = 3,  // create query
  QUERY_CREATE_DB = 4,
  QUERY_CREATE_INDEX = 5,
  QUERY_DROP = 6,     // other queries
  QUERY_INSERT = 7,   // insert query
  QUERY_PREPARE = 8,  // prepare query
  QUERY_EXECUTE = 9,  // execute query
  QUERY_UPDATE = 10,
  QUERY_DELETE = 11,
  QUERY_RENAME = 12,
  QUERY_ALTER = 13,
  QUERY_COPY = 14,
  QUERY_ANALYZE = 15,
  QUERY_SET = 16,   // set query
  QUERY_SHOW = 17,  // show query
  QUERY_SELECT = 18,
  QUERY_OTHER = 19,
  QUERY_INVALID = 20,
  QUERY_CREATE_TRIGGER = 21,
  QUERY_CREATE_SCHEMA = 22,
  QUERY_CREATE_VIEW = 23,
  QUERY_EXPLAIN = 24
};

//===--------------------------------------------------------------------===//
// Result Types
//===--------------------------------------------------------------------===//

enum class ResultType {
  INVALID = INVALID_TYPE_ID,  // invalid result type
  SUCCESS = 1,
  FAILURE = 2,
  ABORTED = 3,  // aborted
  NOOP = 4,     // no op
  UNKNOWN = 5,
  QUEUING = 6,
  TO_ABORT = 7,
};

enum class NetworkTransactionStateType : unsigned char {
  INVALID = static_cast<unsigned char>(INVALID_TYPE_ID),
  IDLE = 'I',
  BLOCK = 'T',
  FAIL = 'E',
};

}  // namespace terrier::network
