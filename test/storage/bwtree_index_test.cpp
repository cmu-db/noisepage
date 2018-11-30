#include "storage/index/bwtree_index.h"
#include <functional>
#include "util/test_harness.h"

namespace terrier {

struct BwTreeIndexTests : public ::terrier::TerrierTest {};

// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, BuilderTest) {
  storage::index::BwTreeIndex<int64_t, std::less<int64_t>, std::equal_to<int64_t>, std::hash<int64_t>>::Builder builder;
  auto bwtree = builder.Build();
  delete bwtree;
}
}  // namespace terrier
