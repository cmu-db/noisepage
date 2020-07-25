#include <llvm/ADT/StringRef.h>

#include <limits>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "execution/exec/execution_context.h"
#include "execution/sql/functions/string_functions.h"
#include "execution/sql/value.h"
#include "execution/sql_test.h"
#include "execution/util/timer.h"

namespace terrier::execution::sql::test {

class StringFunctionsTests : public SqlBasedTest {
 public:
  StringFunctionsTests() : exec_ctx_(MakeExecCtx()) {}

  exec::ExecutionContext *Ctx() { return exec_ctx_.get(); }

 protected:
  const char *test_string_1_ = "I only love my bed and my momma, I'm sorry";
  const char *test_string_2_ = "Drake";

 private:
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};

// NOLINTNEXTLINE
TEST_F(StringFunctionsTests, Concat) {
  // Nulls
  {
    auto result = StringVal("");
    StringFunctions::Concat(&result, Ctx(), StringVal::Null(), StringVal::Null());
    EXPECT_TRUE(result.is_null_);

    StringFunctions::Concat(&result, Ctx(), StringVal::Null(), StringVal("xy"));
    EXPECT_TRUE(result.is_null_);

    StringFunctions::Concat(&result, Ctx(), StringVal("xy"), StringVal::Null());
    EXPECT_TRUE(result.is_null_);
  }

  // Simple Case
  {
    auto result = StringVal("");
    auto x = StringVal("xyz");
    auto a = StringVal("abc");

    StringFunctions::Concat(&result, Ctx(), x, a);
    EXPECT_TRUE(StringVal("xyzabc") == result);
  }
}

// NOLINTNEXTLINE
TEST_F(StringFunctionsTests, Substring) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");
    auto pos = Integer(0);
    auto len = Integer(0);

    StringFunctions::Substring(&result, Ctx(), x, pos);
    EXPECT_TRUE(result.is_null_);

    result = StringVal("");
    StringFunctions::Substring(&result, Ctx(), x, pos, len);
    EXPECT_TRUE(result.is_null_);
  }

  // Checks
  auto x = StringVal(test_string_1_);
  auto result = StringVal("");

  // Valid range
  {
    auto pos = Integer(3);
    auto len = Integer(4);
    StringFunctions::Substring(&result, Ctx(), x, pos, len);
    EXPECT_TRUE(StringVal("only") == result);
  }

  // Negative position should return empty string
  {
    auto pos = Integer(-3);
    auto len = Integer(4);
    StringFunctions::Substring(&result, Ctx(), x, pos, len);
    EXPECT_TRUE(StringVal("") == result);
  }

  // Negative length is null
  {
    auto pos = Integer(1);
    auto len = Integer(-1);
    StringFunctions::Substring(&result, Ctx(), x, pos, len);
    EXPECT_TRUE(result.is_null_);
  }

  // Negative length is null
  {
    auto pos = Integer(1);
    auto len = Integer(-1);
    StringFunctions::Substring(&result, Ctx(), x, pos, len);
    EXPECT_TRUE(result.is_null_);
  }
}

// NOLINTNEXTLINE
TEST_F(StringFunctionsTests, SplitPart) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");
    auto delim = StringVal("");
    auto field = Integer(0);

    StringFunctions::SplitPart(&result, Ctx(), x, delim, field);
    EXPECT_TRUE(result.is_null_);

    result = StringVal("");
    StringFunctions::SplitPart(&result, Ctx(), x, delim, field);
    EXPECT_TRUE(result.is_null_);
  }

  // Negative field
  {
    auto x = StringVal::Null();
    auto result = StringVal("");
    auto delim = StringVal("");
    auto field = Integer(-30);
    StringFunctions::SplitPart(&result, Ctx(), x, delim, field);
    EXPECT_TRUE(result.is_null_);
  }

  // Invalid field
  {
    auto x = StringVal(test_string_1_);
    auto result = StringVal("");
    auto delim = StringVal(" ");
    auto field = Integer(30);
    StringFunctions::SplitPart(&result, Ctx(), x, delim, field);
    EXPECT_TRUE(StringVal("") == result);
  }

  // Empty delimiter
  {
    auto x = StringVal(test_string_1_);
    auto result = StringVal("");
    auto delim = StringVal("");
    auto field = Integer(3);
    StringFunctions::SplitPart(&result, Ctx(), x, delim, field);
    EXPECT_TRUE(x == result);
  }

  auto x = StringVal(test_string_1_);
  auto result = StringVal("");

  // Valid
  {
    const char *delim = " ";
    auto s = llvm::StringRef(test_string_1_);

    llvm::SmallVector<llvm::StringRef, 4> splits;
    s.split(splits, delim);

    for (uint32_t i = 0; i < splits.size(); i++) {
      StringFunctions::SplitPart(&result, Ctx(), x, StringVal(delim), Integer(i + 1));
      auto split = splits[i].str();
      EXPECT_TRUE(StringVal(split.c_str()) == result);
    }
  }
}

// NOLINTNEXTLINE
TEST_F(StringFunctionsTests, Repeat) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");
    auto n = Integer(0);

    StringFunctions::Repeat(&result, Ctx(), x, n);
    EXPECT_TRUE(result.is_null_);

    x = StringVal(test_string_2_);
    result = StringVal("");
    n = Integer::Null();

    StringFunctions::Repeat(&result, Ctx(), x, n);
    EXPECT_TRUE(result.is_null_);
  }

  auto x = StringVal(test_string_2_);
  auto result = StringVal("");
  auto n = Integer(0);

  // n = 0, expect empty result
  StringFunctions::Repeat(&result, Ctx(), x, n);
  EXPECT_TRUE(StringVal("") == result);

  // n = -1, expect empty
  n = Integer(-1);
  StringFunctions::Repeat(&result, Ctx(), x, n);
  EXPECT_TRUE(StringVal("") == result);

  // n = 1, expect original back
  n = Integer(1);
  StringFunctions::Repeat(&result, Ctx(), x, n);
  EXPECT_TRUE(x == result);

  // n = 4, expect four copies
  const auto repeats = 4;

  std::string s;
  for (auto i = 0; i < repeats; i++) s += test_string_2_;

  n = Integer(repeats);
  StringFunctions::Repeat(&result, Ctx(), x, n);
  EXPECT_FALSE(result.is_null_);
  EXPECT_TRUE(StringVal(s.c_str()) == result);
}

// NOLINTNEXTLINE
TEST_F(StringFunctionsTests, Lpad) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");
    auto len = Integer(0);
    auto pad = StringVal("");

    StringFunctions::Lpad(&result, Ctx(), x, len, pad);
    EXPECT_TRUE(result.is_null_);
  }

  // No work
  {
    auto x = StringVal("test");
    auto result = StringVal("");
    auto len = Integer(4);
    auto pad = StringVal("");

    StringFunctions::Lpad(&result, Ctx(), x, len, pad);
    EXPECT_TRUE(x == result);
  }

  // Trim
  {
    auto x = StringVal("test");
    auto result = StringVal("");
    auto len = Integer(2);
    auto pad = StringVal("");

    StringFunctions::Lpad(&result, Ctx(), x, len, pad);
    EXPECT_TRUE(StringVal("te") == result);
  }

  auto x = StringVal("hi");
  auto result = StringVal("");
  auto len = Integer(5);
  auto pad = StringVal("xy");

  StringFunctions::Lpad(&result, Ctx(), x, len, pad);
  EXPECT_TRUE(StringVal("xyxhi") == result);
}

// NOLINTNEXTLINE
TEST_F(StringFunctionsTests, Rpad) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");
    auto len = Integer(0);
    auto pad = StringVal("");

    StringFunctions::Lpad(&result, Ctx(), x, len, pad);
    EXPECT_TRUE(result.is_null_);
  }

  // No work
  {
    auto x = StringVal("test");
    auto result = StringVal("");
    auto len = Integer(4);
    auto pad = StringVal("");

    StringFunctions::Lpad(&result, Ctx(), x, len, pad);
    EXPECT_TRUE(x == result);
  }

  // Trim
  {
    auto x = StringVal("test");
    auto result = StringVal("");
    auto len = Integer(2);
    auto pad = StringVal("");

    StringFunctions::Lpad(&result, Ctx(), x, len, pad);
    EXPECT_TRUE(StringVal("te") == result);
  }

  auto x = StringVal("hi");
  auto result = StringVal("");
  auto len = Integer(5);
  auto pad = StringVal("xy");

  StringFunctions::Rpad(&result, Ctx(), x, len, pad);
  EXPECT_TRUE(StringVal("hixyx") == result);
}

// NOLINTNEXTLINE
TEST_F(StringFunctionsTests, Lower) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");

    StringFunctions::Lower(&result, Ctx(), x);
    EXPECT_TRUE(result.is_null_);
  }

  auto x = StringVal("TEST");
  auto result = StringVal("");
  StringFunctions::Lower(&result, Ctx(), x);
  EXPECT_TRUE(StringVal("test") == result);
}

// NOLINTNEXTLINE
TEST_F(StringFunctionsTests, Upper) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");

    StringFunctions::Upper(&result, Ctx(), x);
    EXPECT_TRUE(result.is_null_);
  }

  auto x = StringVal("test");
  auto result = StringVal("");
  StringFunctions::Upper(&result, Ctx(), x);
  EXPECT_TRUE(StringVal("TEST") == result);
}

// NOLINTNEXTLINE
TEST_F(StringFunctionsTests, Reverse) {
  // Nulls
  {
    auto x = StringVal::Null();
    auto result = StringVal("");

    StringFunctions::Upper(&result, Ctx(), x);
    EXPECT_TRUE(result.is_null_);
  }

  // Empty
  {
    auto x = StringVal("");
    auto result = StringVal("");

    StringFunctions::Upper(&result, Ctx(), x);
    EXPECT_TRUE(x == result);
  }

  auto x = StringVal("test");
  auto result = StringVal("");
  StringFunctions::Reverse(&result, Ctx(), x);
  EXPECT_TRUE(StringVal("tset") == result);
}

// NOLINTNEXTLINE
TEST_F(StringFunctionsTests, Left) {
  // Nulls
  {
    auto result = StringVal("");
    StringFunctions::Left(&result, Ctx(), StringVal::Null(), Integer::Null());
    EXPECT_TRUE(result.is_null_);
  }

  // Positive length
  auto x = StringVal("abcde");
  auto n = Integer(2);
  auto result = StringVal("");
  StringFunctions::Left(&result, Ctx(), x, n);
  EXPECT_TRUE(StringVal("ab") == result);

  // Negative length
  n = Integer(-2);
  result = StringVal("");
  StringFunctions::Left(&result, Ctx(), x, n);
  EXPECT_TRUE(StringVal("abc") == result);

  // Large length
  n = Integer(10);
  result = StringVal("");
  StringFunctions::Left(&result, Ctx(), x, n);
  EXPECT_TRUE(x == result);

  // Large negative length
  n = Integer(-10);
  result = StringVal("");
  StringFunctions::Left(&result, Ctx(), x, n);
  EXPECT_TRUE(StringVal("") == result);
}

// NOLINTNEXTLINE
TEST_F(StringFunctionsTests, Right) {
  // Nulls
  {
    auto result = StringVal("");
    StringFunctions::Right(&result, Ctx(), StringVal::Null(), Integer::Null());
    EXPECT_TRUE(result.is_null_);
  }

  // Positive length
  auto x = StringVal("abcde");
  auto n = Integer(2);
  auto result = StringVal("");
  StringFunctions::Right(&result, Ctx(), x, n);
  EXPECT_TRUE(StringVal("de") == result);

  // Negative length
  n = Integer(-2);
  result = StringVal("");
  StringFunctions::Right(&result, Ctx(), x, n);
  EXPECT_TRUE(StringVal("cde") == result);

  // Large length
  n = Integer(10);
  result = StringVal("");
  StringFunctions::Right(&result, Ctx(), x, n);
  EXPECT_TRUE(x == result);

  // Large negative length
  n = Integer(-10);
  result = StringVal("");
  StringFunctions::Right(&result, Ctx(), x, n);
  EXPECT_TRUE(StringVal("") == result);
}

// NOLINTNEXTLINE
TEST_F(StringFunctionsTests, Ltrim) {
  // Nulls
  {
    auto result = StringVal("");
    StringFunctions::Ltrim(&result, Ctx(), StringVal::Null());
    EXPECT_TRUE(result.is_null_);

    StringFunctions::Ltrim(&result, Ctx(), StringVal::Null(), StringVal("xy"));
    EXPECT_TRUE(result.is_null_);
  }

  // Simple
  auto x = StringVal("zzzytest");
  auto chars = StringVal("xyz");
  auto result = StringVal("");
  StringFunctions::Ltrim(&result, Ctx(), x, chars);
  EXPECT_TRUE(StringVal("test") == result);

  // Remove all
  x = StringVal("zzzyxyyz");
  chars = StringVal("xyz");
  StringFunctions::Ltrim(&result, Ctx(), x, chars);
  EXPECT_TRUE(StringVal("") == result);

  // Remove spaces
  x = StringVal("  test");
  StringFunctions::Ltrim(&result, Ctx(), x);
  EXPECT_TRUE(StringVal("test") == result);
}

// NOLINTNEXTLINE
TEST_F(StringFunctionsTests, Rtrim) {
  // Nulls
  {
    auto result = StringVal("");
    StringFunctions::Rtrim(&result, Ctx(), StringVal::Null());
    EXPECT_TRUE(result.is_null_);

    StringFunctions::Rtrim(&result, Ctx(), StringVal::Null(), StringVal("xy"));
    EXPECT_TRUE(result.is_null_);
  }

  // Simple
  auto x = StringVal("testxxzx");
  auto chars = StringVal("xyz");
  auto result = StringVal("");
  StringFunctions::Rtrim(&result, Ctx(), x, chars);
  EXPECT_TRUE(StringVal("test") == result);

  // Remove all
  x = StringVal("zzzyxyyz");
  chars = StringVal("xyz");
  StringFunctions::Rtrim(&result, Ctx(), x, chars);
  EXPECT_TRUE(StringVal("") == result);

  // Remove spaces
  x = StringVal("test   ");
  StringFunctions::Rtrim(&result, Ctx(), x);
  EXPECT_TRUE(StringVal("test") == result);
}

// NOLINTNEXTLINE
TEST_F(StringFunctionsTests, Trim) {
  // Nulls
  {
    auto result = StringVal("");
    StringFunctions::Trim(&result, Ctx(), StringVal::Null());
    EXPECT_TRUE(result.is_null_);

    StringFunctions::Trim(&result, Ctx(), StringVal::Null(), StringVal("xy"));
    EXPECT_TRUE(result.is_null_);
  }

  // Simple
  auto x = StringVal("yxPrashanthxx");
  auto chars = StringVal("xyz");
  auto result = StringVal("");
  StringFunctions::Trim(&result, Ctx(), x, chars);
  EXPECT_TRUE(StringVal("Prashanth") == result);

  // Remove all
  x = StringVal("zzzyxyyz");
  chars = StringVal("xyz");
  StringFunctions::Trim(&result, Ctx(), x, chars);
  EXPECT_TRUE(StringVal("") == result);

  // Remove all, but one
  x = StringVal("zzzyXxyyz");
  chars = StringVal("xyz");
  StringFunctions::Trim(&result, Ctx(), x, chars);
  EXPECT_TRUE(StringVal("X") == result);

  // Remove spaces
  x = StringVal("   test   ");
  StringFunctions::Trim(&result, Ctx(), x);
  EXPECT_TRUE(StringVal("test") == result);
}

}  // namespace terrier::execution::sql::test
