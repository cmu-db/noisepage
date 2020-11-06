#include <utility>

#include "execution/sql/memory_pool.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::sql::test {

class MemoryPoolTest : public TplTest {};

struct SimpleObj {
  uint32_t a_, b_, c_, d_;
};

struct ComplexObj {
  MemoryPool *memory_;
  MemPoolPtr<SimpleObj> nested_;
  ComplexObj(MemoryPool *m, MemPoolPtr<SimpleObj> n) : memory_(m), nested_(std::move(n)) {}
  ~ComplexObj() { memory_->DeleteObject(std::move(nested_)); }
};

// NOLINTNEXTLINE
TEST_F(MemoryPoolTest, PoolPointers) {
  MemoryPool pool(nullptr);

  // Empty pointer
  MemPoolPtr<SimpleObj> obj1, obj2;
  EXPECT_EQ(obj1, obj2);
  EXPECT_EQ(nullptr, obj1);
  EXPECT_EQ(obj1, nullptr);
  EXPECT_EQ(nullptr, obj1.Get());

  // Non-empty pointers
  obj1 = pool.MakeObject<SimpleObj>();
  EXPECT_NE(obj1, obj2);
  EXPECT_NE(nullptr, obj1);
  EXPECT_NE(obj1, nullptr);
  EXPECT_NE(nullptr, obj1.Get());

  obj1->a_ = 10;
  EXPECT_EQ(10u, obj1->a_);

  pool.DeleteObject(std::move(obj1));
}

// NOLINTNEXTLINE
TEST_F(MemoryPoolTest, ComplexPointers) {
  MemoryPool pool(nullptr);

  MemPoolPtr<ComplexObj> obj1, obj2;
  EXPECT_EQ(obj1, obj2);

  obj1 = pool.MakeObject<ComplexObj>(&pool, pool.MakeObject<SimpleObj>());
  EXPECT_NE(nullptr, obj1);
  EXPECT_NE(obj1, nullptr);

  obj1->nested_->a_ = 1;
  EXPECT_EQ(1u, obj1->nested_->a_);

  pool.DeleteObject(std::move(obj1));
}

}  // namespace noisepage::execution::sql::test
