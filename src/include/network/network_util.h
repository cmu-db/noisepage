#pragma once

#include "network/network_defs.h"

namespace noisepage::network {

/**
 * Utility class for static network or query-related functions
 */
class NetworkUtil {
 public:
  NetworkUtil() = delete;

  /**
   * @param type query type from the parser
   * @return true if a BEGIN, COMMIT, or ABORT. Order of QueryType enum matters here.
   *
   * @warning This logic relies on ordering of values in the enum's definition and is documented there as well.
   */
  static bool TransactionalQueryType(const QueryType type) { return type <= QueryType::QUERY_ROLLBACK; }

  /**
   * @param type query type from the parser
   * @return true if the statement should NOT be bound nor optimized.
   */
  static bool SkipBindQueryType(const QueryType type) {
    return type == QueryType::QUERY_SET || type == QueryType::QUERY_SHOW;
  }

  /**
   * @param type query type from the parser
   * @return true if a SELECT, INSERT, UPDATE, DELETE, or ANALYZE. Order of QueryType enum matters here.
   */
  static bool DMLQueryType(const QueryType type) {
    return type >= QueryType::QUERY_SELECT && type <= QueryType::QUERY_ANALYZE;
  }

  /**
   * @param type query type from the parser
   * @return true if a CREATE. Order of QueryType enum matters here.
   */
  static bool CreateQueryType(const QueryType type) {
    return type >= QueryType::QUERY_CREATE_TABLE && type <= QueryType::QUERY_CREATE_VIEW;
  }

  /**
   * @param type query type from the parser
   * @return true if a DROP. Order of QueryType enum matters here.
   */
  static bool DropQueryType(const QueryType type) {
    return type >= QueryType::QUERY_DROP_TABLE && type <= QueryType::QUERY_DROP_VIEW;
  }

  /**
   * @param type query type from the parser
   * @return true if a CREATE or DROP. Order of QueryType enum matters here.
   */
  static bool DDLQueryType(const QueryType type) {
    return type >= QueryType::QUERY_CREATE_TABLE && type <= QueryType::QUERY_DROP_VIEW;
  }

  /**
   * @param type query type from the parser
   * @return true for statement types that aren't run in a txn, currently SET but other internal queries might be added
   */
  static bool NonTransactionalQueryType(const QueryType type) {
    return type == QueryType::QUERY_SET || type == QueryType::QUERY_SHOW;
  }

  /**
   * @param type query type from the parser
   * @return true if a query that is current not implemented in the system. Order of QueryType enum matters here.
   */
  static bool UnsupportedQueryType(const QueryType type) { return type > QueryType::QUERY_SHOW; }
};

}  // namespace noisepage::network
