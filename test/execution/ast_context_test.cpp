#include <string>
#include <unordered_set>

#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/sema/error_reporter.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::ast::test {

class ContextTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(ContextTest, CreateNewStringsTest) {
  util::Region region("test");
  sema::ErrorReporter error_reporter(&region);
  Context ctx(&region, &error_reporter);

  // We request the strings "string-0", "string-1", ..., "string-99" from the
  // context. We expect duplicate input strings to return the same Identifier!

  std::unordered_set<const char *> seen;
  for (uint32_t i = 0; i < 100; i++) {
    auto string = ctx.GetIdentifier("string-" + std::to_string(i));
    EXPECT_EQ(0u, seen.count(string.GetData()));

    // Check all strings j < i. These must return previously acquired pointers
    for (uint32_t j = 0; j < i; j++) {
      auto dup_request = ctx.GetIdentifier("string-" + std::to_string(j));
      EXPECT_EQ(1u, seen.count(dup_request.GetData()));
    }

    seen.insert(string.GetData());
  }
}

}  // namespace noisepage::execution::ast::test
