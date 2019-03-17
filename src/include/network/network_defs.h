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

#include "tbb/concurrent_unordered_map.h"
#include "tbb/concurrent_unordered_set.h"
#include "tbb/concurrent_vector.h"

#include "common/macros.h"
#include "loggers/main_logger.h"
#include "parser/pg_trigger.h"
#include "type/type_id.h"

namespace terrier::network {

// For epoch
// static const size_t EPOCH_LENGTH = 40;

// For threads
#define CONNECTION_THREAD_COUNT 1

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

using CallbackFunc = std::function<void(void)>;

enum class NetworkProtocolType {
  POSTGRES_JDBC,
  POSTGRES_PSQL,
};

//===--------------------------------------------------------------------===//
// Network Message Types
//===--------------------------------------------------------------------===//

enum class NetworkMessageType : unsigned char {
  // Important: The character '0' is treated as a null message
  // That means we cannot have an invalid type
  NULL_COMMAND = '0',

  // Messages that don't have headers (like Startup message)
  NO_HEADER,

  // Responses
  PARSE_COMPLETE = '1',
  BIND_COMPLETE = '2',
  CLOSE_COMPLETE = '3',
  COMMAND_COMPLETE = 'C',
  PARAMETER_STATUS = 'S',
  AUTHENTICATION_REQUEST = 'R',
  ERROR_RESPONSE = 'E',
  EMPTY_QUERY_RESPONSE = 'I',
  NO_DATA_RESPONSE = 'n',
  READY_FOR_QUERY = 'Z',
  ROW_DESCRIPTION = 'T',
  DATA_ROW = 'D',
  // Errors
  HUMAN_READABLE_ERROR = 'M',
  SQLSTATE_CODE_ERROR = 'C',
  // Commands
  EXECUTE_COMMAND = 'E',
  SYNC_COMMAND = 'S',
  TERMINATE_COMMAND = 'X',
  DESCRIBE_COMMAND = 'D',
  BIND_COMMAND = 'B',
  PARSE_COMMAND = 'P',
  SIMPLE_QUERY_COMMAND = 'Q',
  CLOSE_COMMAND = 'C',
  // SSL willingness
  SSL_YES = 'S',
  SSL_NO = 'N',
};

//===--------------------------------------------------------------------===//
// Describe Message Types
//===--------------------------------------------------------------------===//

enum class ExtendedQueryObjectType : unsigned char { PORTAL = 'P', PREPARED = 'S' };

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
