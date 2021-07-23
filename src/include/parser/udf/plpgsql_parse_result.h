#pragma once

#include <utility>

#include "libpg_query/pg_query.h"

namespace noisepage::parser::udf {

/**
 * The PLpgSQLParseResult class is a simple RAII
 * wrapper for the parse result returned by libpq_query.
 *
 * NOTE: Could just do this with a std::unique_ptr with
 * a default deleter, but this is more pleasant.
 */
class PLpgSQLParseResult {
 public:
  /**
   * Construct a new PLpgSQLParseResult instance.
   * @param result The raw result
   */
  explicit PLpgSQLParseResult(PgQueryPlpgsqlParseResult &&result) : result_{result} {}

  /** Release resources from the parse result */
  ~PLpgSQLParseResult() { pg_query_free_plpgsql_parse_result(result_); }

  /** @return An immutable reference to the underlying result */
  const PgQueryPlpgsqlParseResult &operator*() const { return result_; }

 private:
  /** The underlying parse result */
  PgQueryPlpgsqlParseResult result_;
};

}  // namespace noisepage::parser::udf
