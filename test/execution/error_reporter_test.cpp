#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "execution/ast/ast_dump.h"
#include "execution/parsing/parser.h"
#include "execution/parsing/scanner.h"
#include "execution/tpl_test.h"

namespace terrier::execution::parsing::test {

class ErrorReporterTest : public TplTest {
 public:
  void SetUp() override {
    // Set up loggers
    TplTest::SetUp();
    reporter_ = std::make_unique<sema::ErrorReporter>(&region_);
    ctx_ = std::make_unique<ast::Context>(&region_, reporter_.get());
  }

  /**
   * Split a string on a delimiter.
   * This used to be in my StringUtil class but we decided to not bring that class in for now.
   * So this mofo is in this test case. Deal with it, son.
   * @param str
   * @param delimiter
   * @return
   */
  std::vector<std::string> Split(const std::string &str, char delimiter) {
    std::stringstream ss(str);
    std::vector<std::string> lines;
    std::string temp;
    while (std::getline(ss, temp, delimiter)) {
      lines.push_back(temp);
    }  // WHILE
    return (lines);
  }

  ast::Context *GetContext() { return ctx_.get(); }
  sema::ErrorReporter *Reporter() { return reporter_.get(); }

 private:
  util::Region region_{"test"};
  std::unique_ptr<sema::ErrorReporter> reporter_;
  std::unique_ptr<ast::Context> ctx_;
};

// TODO(pavlo): This test is disabled until the invalid memory access error in the TPL parser is fixed (#610)
// NOLINTNEXTLINE
TEST_F(ErrorReporterTest, DISABLED_SerializeErrorsTest) {
  // Throw some busted TPL at the parser and check the error
  const auto src = R"(
    fun bad_function(xyz: int) -> void {
      XXX YYY ZZZ!!!
  )";
  Scanner scanner(src);
  Parser parser(&scanner, GetContext());

  // Attempt to parse the bad code
  auto *ast = parser.Parse();
  EXPECT_NE(ast, nullptr);
  EXPECT_TRUE(Reporter()->HasErrors());

  auto errors = Reporter()->SerializeErrors();
  EXPECT_FALSE(errors.empty());

  // There should be two errors, so we should expect two newlines
  // There isn't anything else that we can really check here since
  // the output is meant for human consumption
  auto lines = Split(errors, '\n');
  EXPECT_EQ(lines.size(), 2);
}

}  // namespace terrier::execution::parsing::test
