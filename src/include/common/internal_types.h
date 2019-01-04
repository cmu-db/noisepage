//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// internal_types.h
//
// Identification: src/include/common/internal_types.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// internal_types.h
//
// Identification: src/include/common/internal_types.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
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

namespace terrier {

// For all of the enums defined in this header, we will
// use this value to indicate that it is an invalid value
// I don't think it matters whether this is 0 or -1
// #define INVALID_TYPE_ID 0

// For epoch
// static const size_t EPOCH_LENGTH = 40;

// For threads
extern size_t CONNECTION_THREAD_COUNT;

std::string TypeIdToString(type::TypeId type);

//===--------------------------------------------------------------------===//
// Wire protocol typedefs
//===--------------------------------------------------------------------===//
#define SOCKET_BUFFER_CAPACITY 8192

/* byte type */
using uchar = unsigned char;

/* type for buffer of bytes */
using ByteBuf = std::vector<uchar>;

}  // namespace terrier
