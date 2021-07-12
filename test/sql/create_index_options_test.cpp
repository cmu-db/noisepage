
#include "execution/compiler/output_checker.h"
#include "gtest/gtest.h"
#include "spdlog/fmt/fmt.h"
#include "storage/index/bplustree_index.h"
#include "storage/index/index.h"
#include "test_util/end_to_end_test.h"
#include "test_util/test_harness.h"

namespace noisepage::test {

class CreateIndexOptionsTest : public EndToEndTest {
 public:
  void SetUp() override {
    EndToEndTest::SetUp();
    auto exec_ctx = MakeExecCtx();
    GenerateTestTables(exec_ctx.get());
  }
};

// NOLINTNEXTLINE
TEST_F(CreateIndexOptionsTest, BPlusTreeOptions) {
  RunQuery("CREATE INDEX test_2_idx_upper ON test_2 (col1) WITH (BPLUSTREE_INNER_NODE_UPPER_THRESHOLD = 256)");
  RunQuery("CREATE INDEX test_2_idx_lower ON test_2 (col1) WITH (BPLUSTREE_INNER_NODE_LOWER_THRESHOLD = 4)");
  RunQuery(
      "CREATE INDEX test_2_idx_both ON test_2 (col1) WITH (BPLUSTREE_INNER_NODE_UPPER_THRESHOLD = 256, "
      "BPLUSTREE_INNER_NODE_LOWER_THRESHOLD = 4)");

  auto test_2_idx_upper = accessor_->GetIndex(accessor_->GetIndexOid("test_2_idx_upper"));
  auto test_2_idx_lower = accessor_->GetIndex(accessor_->GetIndexOid("test_2_idx_lower"));
  auto test_2_idx_both = accessor_->GetIndex(accessor_->GetIndexOid("test_2_idx_both"));
  ASSERT_TRUE(test_2_idx_upper);
  ASSERT_TRUE(test_2_idx_lower);
  ASSERT_TRUE(test_2_idx_both);

  ASSERT_EQ(test_2_idx_upper->Type(), storage::index::IndexType::BPLUSTREE);
  ASSERT_EQ(test_2_idx_lower->Type(), storage::index::IndexType::BPLUSTREE);
  ASSERT_EQ(test_2_idx_both->Type(), storage::index::IndexType::BPLUSTREE);

  auto test_2_idx_upper_bpt_index =
      test_2_idx_upper.CastManagedPointerTo<storage::index::BPlusTreeIndex<storage::index::CompactIntsKey<8>>>();
  auto test_2_idx_lower_bpt_index =
      test_2_idx_lower.CastManagedPointerTo<storage::index::BPlusTreeIndex<storage::index::CompactIntsKey<8>>>();
  auto test_2_idx_both_bpt_index =
      test_2_idx_both.CastManagedPointerTo<storage::index::BPlusTreeIndex<storage::index::CompactIntsKey<8>>>();

  ASSERT_EQ(test_2_idx_upper_bpt_index->GetInnerNodeSizeUpperThreshold(), 256);
  ASSERT_EQ(test_2_idx_lower_bpt_index->GetInnerNodeSizeLowerThreshold(), 4);
  ASSERT_EQ(test_2_idx_both_bpt_index->GetInnerNodeSizeUpperThreshold(), 256);
  ASSERT_EQ(test_2_idx_both_bpt_index->GetInnerNodeSizeLowerThreshold(), 4);
}

}  // namespace noisepage::test
