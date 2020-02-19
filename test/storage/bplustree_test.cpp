#include "test_util/test_harness.h"

namespace terrier::storage::index {

struct BPlusTreeTests : public TerrierTest {};

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, EmptyTest) { EXPECT_TRUE(true); }
}  // namespace terrier::storage::index
