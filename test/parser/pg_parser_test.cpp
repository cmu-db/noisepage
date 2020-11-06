#include <string>
#include <vector>

#include "libpg_query/pg_query.h"
#include "test_util/test_harness.h"

namespace noisepage::parser {

/**
 * A very basic test, to verify that we can invoke the parser in libpg_query.
 */

// NOLINTNEXTLINE
TEST(PGParserTests, BasicTest) {
  PgQueryInternalParsetreeAndError result;

  pg_query_parse_init();
  result = pg_query_parse("SELECT 1");
  print_pg_parse_tree(result.tree);
  pg_query_free_parse_result(result);
}

// NOLINTNEXTLINE
TEST(PGParserTests, CreateFunctionTest) {
  PgQueryInternalParsetreeAndError result;
  std::vector<std::string> queries;

  queries.emplace_back(
      "CREATE OR REPLACE FUNCTION increment ("
      " i DOUBLE"
      " )"
      " RETURNS DOUBLE AS $$ "
      " BEGIN RETURN i + 1; END; $$ ("
      "LANGUAGE plpgsql;");

  queries.emplace_back(
      "CREATE FUNCTION increment1 ("
      " i DOUBLE, j DOUBLE"
      " )"
      " RETURNS DOUBLE AS $$ "
      " BEGIN RETURN i + j; END; $$ ("
      "LANGUAGE plpgsql;");

  queries.emplace_back(
      "CREATE OR REPLACE FUNCTION increment2 ("
      " i INTEGER, j INTEGER"
      " )"
      " RETURNS INTEGER AS $$ "
      " BEGIN RETURN i + 1; END; $$ ("
      "LANGUAGE plpgsql;");

  pg_query_parse_init();
  for (const auto &query : queries) {
    result = pg_query_parse(query.c_str());
    pg_query_free_parse_result(result);
  }
}

}  // namespace noisepage::parser
