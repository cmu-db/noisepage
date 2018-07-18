#include <random>
#include <unordered_set>
#include "gtest/gtest.h"
#include "common/test_util.h"
#include "storage/block_store.h"

namespace terrier {
// Fake class to keep the memory utilization down if we don't care about the
// actual content (maybe we don't want to actually allocate 1mb for a fake page)
template<typename T>
class FakeObjectPool : public ObjectPool<T> {
 public:
  explicit FakeObjectPool(uint32_t reuse_limit)
      : ObjectPool<T>(0), fake_pool_(reuse_limit) {}

  ~FakeObjectPool() = default;

  T *Get() override {
    return reinterpret_cast<T *>(fake_pool_.Get());
  }

  void Release(T *obj) override {
    fake_pool_.Release(reinterpret_cast<char *>(obj));
  }
 private:
  ObjectPool<char> fake_pool_;
};

// Tests that a block can be retrieved after calling new block and that
// the retrieved block matches the new block. Then, test that block can
// be erased and that a subsequent lookup on that block throws an error
TEST(BlockStoreTests, SimpleCorrectnessTest) {
  const uint64_t reuse_limit = 100;
  FakeObjectPool<RawBlock> pool(reuse_limit);
  storage::BlockStore store(pool);
  auto new_block = store.NewBlock();
  EXPECT_EQ(new_block.second, store.RetrieveBlock(new_block.first));
  store.UnsafeDeallocate(new_block.first);
  EXPECT_EQ(nullptr, store.RetrieveBlock(new_block.first));
}

void TestUniqueness() {
  // This should have no bearing on the correctness of test
  const uint64_t reuse_limit = 100;
  FakeObjectPool<RawBlock> pool(reuse_limit);
  storage::BlockStore store(pool);
  const uint32_t num_threads = 8;
  std::vector<std::vector<block_id_t>> block_ids(num_threads);
  std::vector<std::vector<RawBlock *>> blocks(num_threads);
  const uint32_t num_allocation = 100;

  auto workload = [&](const uint32_t thread_id) {
    // Randomly generate a sequence of new and use
    std::default_random_engine generator;
    uint32_t blocks_allocated = 0;
    auto allocate = [&] {
      auto pair = store.NewBlock();
      block_ids[thread_id].push_back(pair.first);
      blocks[thread_id].push_back(pair.second);
      blocks_allocated++;
    };
    auto use = [&] {
      auto &ids = block_ids[thread_id];
      auto elem = testutil::UniformRandomElement(ids, generator);
      // Check that the same memory location is being returned.
      EXPECT_EQ(store.RetrieveBlock(*elem),
                *(blocks[thread_id].begin() + (elem - ids.begin())));
    };

    while (blocks_allocated < num_allocation)
      testutil::InvokeWorkloadWithDistribution({allocate, use},
                                               {0.5, 0.5},
                                               generator);
  };

  testutil::RunThreadsUntilFinish(num_threads, workload);

  // Check global uniqueness of the block ids and ptrs.
  std::unordered_set<block_id_t> ids;
  std::unordered_set<RawBlock *> block_ptrs;
  for (uint32_t tid = 0; tid < num_threads; tid++) {
    ids.insert(block_ids[tid].begin(), block_ids[tid].end());
    block_ptrs.insert(blocks[tid].begin(), blocks[tid].end());
  }
  EXPECT_EQ(ids.size(), num_threads * num_allocation);
  EXPECT_EQ(block_ptrs.size(), num_threads * num_allocation);
}

// Tests that multiple threads allocating and accessing blocks will get
// back unique block_id -> RawBlock *pairs.
TEST(BlockStoreTests, ConcurrentUniquenessTest) {
  // Independent trials
  const uint32_t num_reps = 50;
  for (uint32_t rep = 0; rep < num_reps; rep++)
    TestUniqueness();
}
}

