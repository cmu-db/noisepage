#include "type/type_util.h"
#include "type/type_id.h"

#include "gtest/gtest.h"

namespace terrier::type {

// NOLINTNEXTLINE
TEST(TypeUtilTests, InvalidTypeTest) {
  // Just check to make sure that we throw an exception if we give GetTypeSize
  // an invalid TypeId that it handles it correctly
  EXPECT_THROW(TypeUtil::GetTypeSize(TypeId::INVALID), std::runtime_error);
}

}  // namespace terrier::type
