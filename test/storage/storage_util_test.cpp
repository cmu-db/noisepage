#include <unordered_map>
#include <utility>
#include <vector>
#include "common/object_pool.h"
#include "storage/data_table.h"
#include "storage/storage_util.h"
#include "util/storage_test_util.h"
#include "storage/storage_defs.h"

namespace terrier {

class StorageUtilTestObject {
 public:
  ~StorageUtilTestObject() {
    for (auto entry : loose_pointers_) {
      delete[] entry;
    }
  }

  std::vector<byte *> loose_pointers_;
};

struct StorageUtilTests : public ::testing::Test {
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};
  std::uniform_int_distribution<uint64_t> timestamp_dist_{0, ULONG_MAX};

  storage::RawBlock *raw_block_ = nullptr;
  storage::BlockStore block_store_{1};

 protected:
  void SetUp() override {
    raw_block_ = block_store_.Get();
  }

  void TearDown() override {
    block_store_.Release(raw_block_);
  }
};


// Write a value to a position, read from the same position and compare results. Repeats for num_iterations.
// NOLINTNEXTLINE
TEST_F(StorageUtilTests, ReadWriteBytes) {
  uint32_t num_iterations = 500;
  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    StorageUtilTestObject test_obj;
    // generate a random val
    std::vector<uint8_t> valid_sizes{1,2,4,8};
    std::uniform_int_distribution<uint8_t> idx(0, static_cast<uint8_t>(valid_sizes.size() - 1));
    uint8_t attr_size = valid_sizes[idx(generator_)];
    uint64_t val = 0;
    StorageTestUtil::FillWithRandomBytes(attr_size, reinterpret_cast<byte *>(&val), &generator_);

    // Write and read again to see if we get the same value;
    byte pos[8];
    storage::StorageUtil::WriteBytes(attr_size, val, pos);
    EXPECT_EQ(val, storage::StorageUtil::ReadBytes(attr_size, pos));
  }
}
}
