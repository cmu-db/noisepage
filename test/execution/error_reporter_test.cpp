#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "execution/tpl_test.h"

#include "execution/ast/ast_dump.h"
#include "execution/parsing/parser.h"
#include "execution/parsing/scanner.h"

namespace terrier::execution::parsing::test {

class ErrorReporterTest : public TplTest {
 public:
  void SetUp() override {
    // Set up loggers
    TplTest::SetUp();
    reporter_ = std::make_unique<sema::ErrorReporter>(&region_);
    ctx_ = std::make_unique<ast::Context>(&region_, reporter_.get());
  }

  ast::Context *GetContext() { return ctx_.get(); }
  sema::ErrorReporter *Reporter() { return reporter_.get(); }

 private:
  util::Region region_{"test"};
  std::unique_ptr<sema::ErrorReporter> reporter_;
  std::unique_ptr<ast::Context> ctx_;
};

// NOLINTNEXTLINE
TEST_F(ErrorReporterTest, SerializeErrorsTest) {
  // Throw some busted TPL at the parser and check the error
  const auto src = R"(
    fun bad_function(xyz: int) -> void {
      XXX YYY ZZZ!!!
    }
  )";
  Scanner scanner(src);
  Parser parser(&scanner, GetContext());

  // Attempt parse
  auto *ast = parser.Parse();
  EXPECT_NE(ast, nullptr);
  EXPECT_TRUE(Reporter()->HasErrors());

  auto errors = Reporter()->SerializeErrors();
  EXPECT_FALSE(errors.empty());
  std::cout << errors;

}

}  // namespace terrier::execution::parsing::test
