#include <algorithm>
#include <random>
#include <vector>

#include "catalog/schema.h"
#include "execution/sql/operators/comparison_operators.h"
#include "execution/sql/sorter.h"
#include "execution/sql/vector_projection.h"
#include "execution/sql/vector_projection_iterator.h"
#include "execution/sql_test.h"

namespace noisepage::execution::sql::test {

namespace {

/**
 * The input tuples. All sorter instances use this tuple as input and output.
 */
struct Tuple {
  int64_t key_;
  int64_t attributes_;
};

/**
 * Insert a number of input tuples into the given sorter instance.
 * @param sorter The sorter to insert into.
 * @param num_tuples The number of tuple to insert.
 */
void PopulateSorter(Sorter *sorter, uint32_t num_tuples = common::Constants::K_DEFAULT_VECTOR_SIZE + 7) {
  std::random_device r;
  for (uint32_t i = 0; i < num_tuples; i++) {
    auto tuple = reinterpret_cast<Tuple *>(sorter->AllocInputTuple());
    tuple->key_ = r() % num_tuples;
    tuple->attributes_ = r() % 10;
  }
}

/**
 * Compare two tuples.
 * @param lhs The first tuple.
 * @param rhs The second tuple.
 * @return < 0 if lhs < rhs, 0 if lhs = rhs, > 0 if lhs > rhs.
 */
int32_t CompareTuple(const Tuple &lhs, const Tuple &rhs) { return lhs.key_ - rhs.key_; }

/**
 * Convert row-oriented data in the rows vector to columnar format in the given
 * vector projection.
 * @param rows The array of row-oriented tuple data.
 * @param num_rows The number of rows in the array.
 * @param iter The output vector projection iterator.
 */
void Transpose(const byte *rows[], uint64_t num_rows, VectorProjectionIterator *iter) {
  for (uint64_t i = 0; i < num_rows; i++) {
    auto tuple = reinterpret_cast<const Tuple *>(rows[i]);
    iter->SetValue<int64_t, false>(0, tuple->key_, false);
    iter->SetValue<int64_t, false>(1, tuple->attributes_, false);
    iter->Advance();
  }
}

/**
 * Check if the given vector is sorted.
 * @tparam T The primitive/native type of the element the vector contains.
 * @param vec The vector to check is sorted.
 * @return True if sorted; false otherwise.
 */
template <class T>
bool IsSorted(const Vector &vec) {
  const auto data = reinterpret_cast<const T *>(vec.GetData());
  for (uint64_t i = 1; i < vec.GetCount(); i++) {
    bool left_null = vec.GetNullMask()[i - 1];
    bool right_null = vec.GetNullMask()[i];
    if (left_null != right_null) {
      return false;
    }
    if (left_null) {
      continue;
    }
    if (!LessThanEqual<T>::Apply(data[i - 1], data[i])) {
      return false;
    }
  }
  return true;
}

}  // namespace

class SorterVectorIteratorTest : public SqlBasedTest {
 public:
  SorterVectorIteratorTest()
      : schema_({
            {"key", type::TypeId::BIGINT, false, parser::ConstantValueExpression(type::TypeId::BIGINT)},
            {"attributes", type::TypeId::BIGINT, true, parser::ConstantValueExpression(type::TypeId::BIGINT)},
        }) {}

 protected:
  std::vector<const catalog::Schema::Column *> RowMeta() const {
    std::vector<const catalog::Schema::Column *> cols;
    for (const auto &col : schema_.GetColumns()) {
      cols.push_back(&col);
    }
    return cols;
  }

 private:
  catalog::Schema schema_;
};

// NOLINTNEXTLINE
TEST_F(SorterVectorIteratorTest, EmptyIterator) {
  auto exec_ctx = MakeExecCtx();
  const auto compare = [](const void *lhs, const void *rhs) {
    return CompareTuple(*reinterpret_cast<const Tuple *>(lhs), *reinterpret_cast<const Tuple *>(rhs));
  };
  Sorter sorter(exec_ctx.get(), compare, sizeof(Tuple));

  for (SorterVectorIterator iter(sorter, RowMeta(), Transpose); iter.HasNext(); iter.Next(Transpose)) {
    FAIL() << "Iteration should not occur on empty sorter instance";
  }
}

// NOLINTNEXTLINE
TEST_F(SorterVectorIteratorTest, Iterate) {
  // To ensure at least two vector's worth of data
  auto exec_ctx = MakeExecCtx();
  const uint32_t num_elems = common::Constants::K_DEFAULT_VECTOR_SIZE + 29;

  const auto compare = [](const void *lhs, const void *rhs) {
    return CompareTuple(*reinterpret_cast<const Tuple *>(lhs), *reinterpret_cast<const Tuple *>(rhs));
  };
  Sorter sorter(exec_ctx.get(), compare, sizeof(Tuple));
  PopulateSorter(&sorter, num_elems);
  sorter.Sort();

  uint32_t num_found = 0;
  for (SorterVectorIterator iter(sorter, RowMeta(), Transpose); iter.HasNext(); iter.Next(Transpose)) {
    auto *vpi = iter.GetVectorProjectionIterator();

    // VPI shouldn't be filtered
    EXPECT_FALSE(vpi->IsFiltered());
    EXPECT_GT(vpi->GetTotalTupleCount(), 0u);

    // VPI should have two columns: key and attributes
    EXPECT_EQ(2u, vpi->GetVectorProjection()->GetColumnCount());

    // Neither vector should have NULLs
    EXPECT_FALSE(vpi->GetVectorProjection()->GetColumn(0)->GetNullMask().Any());
    EXPECT_FALSE(vpi->GetVectorProjection()->GetColumn(1)->GetNullMask().Any());

    // Verify sorted
    const auto *key_vector = vpi->GetVectorProjection()->GetColumn(0);
    const auto *key_data = reinterpret_cast<const decltype(Tuple::key_) *>(key_vector->GetData());
    EXPECT_TRUE(std::is_sorted(key_data, key_data + key_vector->GetCount()));

    // Count
    num_found += vpi->GetSelectedTupleCount();
  }

  EXPECT_EQ(num_elems, num_found);
}

}  // namespace noisepage::execution::sql::test
