#pragma once

#include <string>
#include <utility>
#include <vector>

#include "network/postgres/postgres_protocol_utils.h"
#include "type/transient_value.h"

namespace terrier::trafficcop {

/**
 * Statement contains a prepared statement by the backend.
 */
class Statement {
 public:
  /**
   * The types of the parameters
   * To satisfy Describe command, we store Postgres type oid here instead of internal type ids.
   */
  std::vector<network::PostgresValueType> param_types_;

  /**
   * Corresponding query string
   */
  std::string query_string_;

  /**
   * Returns the number of the parameters.
   * @return
   */
  size_t NumParams() { return param_types_.size(); }
};

}  // namespace terrier::trafficcop
