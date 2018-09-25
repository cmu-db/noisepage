#include <functional>

#include "bwtree/bwtree.h"
#include "util/test_harness.h"
namespace terrier {
using TEST_TYPE = char;
using BwTree = ::wangziqi2013::bwtree::BwTree<TEST_TYPE, TEST_TYPE, std::less<>, std::equal_to<>, std::hash<TEST_TYPE>,
                                              std::equal_to<>, std::hash<TEST_TYPE>>;

struct BwTreeTests : public TerrierTest {
  BwTree tree_;
};

// NOLINTNEXTLINE
TEST_F(BwTreeTests, BasicSelect) {
  bool ret = tree_.Insert(1, 1, true);
  EXPECT_TRUE(ret);
}
}  // namespace terrier
