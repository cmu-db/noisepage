#include "binder/binder_util.h"

#include <memory>

#include "common/managed_pointer.h"
#include "execution/sql/value_util.h"
#include "parser/expression/constant_value_expression.h"
#include "test_util/test_harness.h"

namespace noisepage {
class BinderUtilTest : public TerrierTest {};

/**
 * Exercise the BinderUtil CheckAndTryPromoteType function
 * @tparam cpp_type native type to try to parse the string inside the CVE to
 * @param sql_type desired type to promote to
 * @param valid_val string representation of valid value for the conversion
 * @param peek_val reference representation of valid value for the conversion
 * @param invalid_val string representation of invalid value for the conversion
 */
template <typename cpp_type>
void TestCheckAndTryPromoteType(const type::TypeId sql_type, const std::string_view valid_val, const cpp_type peek_val,
                                const std::string_view invalid_val) {
  auto string_val = execution::sql::ValueUtil::CreateStringVal(valid_val);
  auto cve = std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR, string_val.first,
                                                               std::move(string_val.second));
  binder::BinderUtil::CheckAndTryPromoteType(common::ManagedPointer(cve), sql_type);
  EXPECT_EQ(cve->Peek<cpp_type>(), peek_val);

  string_val = execution::sql::ValueUtil::CreateStringVal(invalid_val);
  cve = std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR, string_val.first,
                                                          std::move(string_val.second));
  EXPECT_THROW(binder::BinderUtil::CheckAndTryPromoteType(common::ManagedPointer(cve), sql_type), BinderException);
}

// NOLINTNEXTLINE
TEST_F(BinderUtilTest, VarcharToTinyInt) {
  TestCheckAndTryPromoteType<int8_t>(type::TypeId::TINYINT, "15", 15, "15721");
}

// NOLINTNEXTLINE
TEST_F(BinderUtilTest, VarcharToSmallInt) {
  TestCheckAndTryPromoteType<int16_t>(type::TypeId::SMALLINT, "15721", 15721, "1572115445");
}

// NOLINTNEXTLINE
TEST_F(BinderUtilTest, VarcharToInteger) {
  TestCheckAndTryPromoteType<int32_t>(type::TypeId::INTEGER, "1572115445", 1572115445, "157211544500");
}

// NOLINTNEXTLINE
TEST_F(BinderUtilTest, VarcharToBigInt) {
  TestCheckAndTryPromoteType<int64_t>(type::TypeId::BIGINT, "157211544500", 157211544500,
                                      "92233720368547758079223372036854775807");
}

// NOLINTNEXTLINE
TEST_F(BinderUtilTest, VarcharToDecimal) {
  TestCheckAndTryPromoteType<double>(type::TypeId::DECIMAL, "15721.15445", 15721.15445, "1.79769e+310");
}
}  // namespace noisepage
