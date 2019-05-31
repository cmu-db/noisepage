#include "execution/tpl_test.h"  // NOLINT

#include "execution/util/region.h"

namespace tpl::util::test {

class RegionTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(RegionTest, Simple) {
  util::Region r("test");

  uint32_t alloc_size = 7;

  uint32_t allocated = 0;
  for (size_t alignment = 1; alignment < 64; alignment *= 2, alloc_size *= 2) {
    void *ptr = r.Allocate(alloc_size, alignment);
    EXPECT_NE(nullptr, ptr);

    EXPECT_EQ(MathUtil::AlignAddress(reinterpret_cast<uintptr_t>(ptr), alignment), reinterpret_cast<uintptr_t>(ptr));

    allocated += alloc_size;
    EXPECT_EQ(allocated, r.allocated());
  }
}

}  // namespace tpl::util::test
