#include <utility>

#include "execution/sql/memory_pool.h"
#include "execution/tpl_test.h"

namespace terrier::execution::sql::test {

class MemoryPoolTest : public TplTest {};

struct SimpleObj {
  uint32_t a, b, c, d;
};

struct ComplexObj {
  MemoryPool *memory;
  MemPoolPtr<SimpleObj> nested;
  ComplexObj(MemoryPool *m, MemPoolPtr<SimpleObj> n) : memory(m), nested(std::move(n)) {}
  ~ComplexObj() { memory->DeleteObject(std::move(nested)); }
};

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

  obj1->a = 10;
  EXPECT_EQ(10u, obj1->a);

  pool.DeleteObject(std::move(obj1));
}

TEST_F(MemoryPoolTest, ComplexPointers) {
  MemoryPool pool(nullptr);

  MemPoolPtr<ComplexObj> obj1, obj2;
  EXPECT_EQ(obj1, obj2);

  obj1 = pool.MakeObject<ComplexObj>(&pool, pool.MakeObject<SimpleObj>());
  EXPECT_NE(nullptr, obj1);
  EXPECT_NE(obj1, nullptr);

  obj1->nested->a = 1;
  EXPECT_EQ(1u, obj1->nested->a);

  pool.DeleteObject(std::move(obj1));
}

}  // namespace terrier::execution::sql::test
