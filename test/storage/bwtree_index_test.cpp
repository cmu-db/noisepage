#include "storage/index/bwtree_index.h"
#include <functional>
#include "util/test_harness.h"

namespace terrier {

struct BwTreeIndexTests : public ::terrier::TerrierTest {};

// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, BuilderTest) {
  storage::index::BwTreeIndex<int64_t, std::less<>, std::equal_to<>, std::hash<int64_t>>::Builder builder;
  auto *bwtree = builder.Build();

  UNUSED_ATTRIBUTE auto key1 = storage::index::CompactIntsKey<1>();
  UNUSED_ATTRIBUTE auto key2 = storage::index::CompactIntsKey<2>();
  UNUSED_ATTRIBUTE auto key3 = storage::index::CompactIntsKey<3>();
  UNUSED_ATTRIBUTE auto key4 = storage::index::CompactIntsKey<4>();

  UNUSED_ATTRIBUTE auto equality = storage::index::CompactIntsEqualityChecker<1>();
  UNUSED_ATTRIBUTE auto hasher = storage::index::CompactIntsHasher<1>();
  UNUSED_ATTRIBUTE auto comparator = storage::index::CompactIntsComparator<1>();

  delete bwtree;
}
}  // namespace terrier
