#include <string>
#include <vector>
#include "libpg_query/pg_query.h"

#include "util/test_harness.h"

namespace terrier::parser {

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

}  // namespace terrier::parser
