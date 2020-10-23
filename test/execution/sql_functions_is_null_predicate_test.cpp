#include "execution/sql/functions/is_null_predicate.h"
#include "execution/sql/value.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::sql::test {

class IsNullPredicateTests : public TplTest {};

// NOLINTNEXTLINE
TEST_F(IsNullPredicateTests, IsNull) {
#define CHECK_NULL_FOR_TYPE(NullVal, NonNullVal)                                    \
  {                                                                                 \
    EXPECT_TRUE(noisepage::execution::sql::IsNullPredicate::IsNull(NullVal));       \
    EXPECT_FALSE(noisepage::execution::sql::IsNullPredicate::IsNotNull(NullVal));   \
    EXPECT_FALSE(noisepage::execution::sql::IsNullPredicate::IsNull(NonNullVal));   \
    EXPECT_TRUE(noisepage::execution::sql::IsNullPredicate::IsNotNull(NonNullVal)); \
  }

  CHECK_NULL_FOR_TYPE(BoolVal::Null(), BoolVal(false));
  CHECK_NULL_FOR_TYPE(Integer::Null(), Integer(44));
  CHECK_NULL_FOR_TYPE(Real::Null(), Real(44.0));
  CHECK_NULL_FOR_TYPE(StringVal::Null(), StringVal("44"));
  CHECK_NULL_FOR_TYPE(DateVal::Null(), DateVal(sql::Date::FromYMD(2010, 10, 10)));
  // CHECK_IS_NOT_NULL_FOR_TYPE(TimestampVal::Null(), sql::Timestamp::FromString("2010-10-10"));
}

}  // namespace noisepage::execution::sql::test
