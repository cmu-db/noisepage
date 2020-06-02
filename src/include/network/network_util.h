#pragma once

#include "network_defs.h"

namespace terrier::network {

class NetworkUtil {
 public:
  NetworkUtil() = delete;
  static bool TransactionalQueryType(const QueryType type) { return type <= QueryType::QUERY_ROLLBACK; }

  static bool DMLQueryType(const QueryType type) { return type <= QueryType::QUERY_DELETE; }

  static bool CreateQueryType(const QueryType type) { return type <= QueryType::QUERY_CREATE_VIEW; }

  static bool DropQueryType(const QueryType type) { return type <= QueryType::QUERY_DROP_VIEW; }

  static bool UnsupportedQueryType(const QueryType type) { return type > QueryType::QUERY_DROP_VIEW; }
};

}  // namespace terrier::network
