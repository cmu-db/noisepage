#include <string>

#include "parser/udf/string_utils.h"
#include "test_util/test_harness.h"

namespace noisepage::parser {

class PLpgSQLParserTest : public TerrierTest {};

TEST_F(PLpgSQLParserTest, LowerTest0) {
  const std::string input{"HELLO WORLD"};
  const std::string expected{"hello world"};
  const auto result = udf::StringUtils::Lower(input);
  EXPECT_EQ(expected, result);
}

TEST_F(PLpgSQLParserTest, StripTest0) {
  const std::string input{" hello "};
  const std::string expected{"hello"};
  const auto result = udf::StringUtils::Strip(input);
  EXPECT_EQ(expected, result);
}

}  // namespace noisepage::parser
