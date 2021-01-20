#include <chrono>  // NOLINT
#include <vector>

#include "common/settings.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/execution_settings.h"
#include "execution/sql/filter_manager.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/sql/vector_filter_executor.h"
#include "execution/sql_test.h"
#include "gmock/gmock.h"

namespace noisepage::execution::sql::test {

class FilterManagerTest : public SqlBasedTest {};

enum Col : uint8_t { A = 0, B = 1, C = 2, D = 3 };

using namespace std::chrono_literals;  // NOLINT

// NOLINTNEXTLINE
TEST_F(FilterManagerTest, ConjunctionTest) {
  auto exec_ctx = MakeExecCtx();
  // Create a filter that implements: colA < 500 AND colB < 9
  FilterManager filter{exec_ctx->GetExecutionSettings()};
  filter.StartNewClause();
  filter.InsertClauseTerms({[](auto exec_ctx, auto vp, auto tids, auto ctx) {
                              VectorFilterExecutor::SelectLessThanVal(
                                  reinterpret_cast<exec::ExecutionContext *>(exec_ctx)->GetExecutionSettings(), vp,
                                  Col::A, GenericValue::CreateInteger(500), tids);
                            },
                            [](auto exec_ctx, auto vp, auto tids, auto ctx) {
                              VectorFilterExecutor::SelectLessThanVal(
                                  reinterpret_cast<exec::ExecutionContext *>(exec_ctx)->GetExecutionSettings(), vp,
                                  Col::B, GenericValue::CreateInteger(9), tids);
                            }});

  // Create some random data.
  VectorProjection vp;
  vp.Initialize({TypeId::Integer, TypeId::Integer});
  vp.Reset(common::Constants::K_DEFAULT_VECTOR_SIZE);
  VectorOps::Generate(vp.GetColumn(Col::A), 0, 1);
  VectorOps::Generate(vp.GetColumn(Col::B), 0, 1);

  // Run the filters
  VectorProjectionIterator vpi(&vp);
  filter.RunFilters(exec_ctx.get(), &vpi);

  // Check
  EXPECT_TRUE(vpi.IsFiltered());
  EXPECT_FALSE(vpi.IsEmpty());
  vpi.ForEach([&]() {
    auto cola = *vpi.GetValue<int32_t, false>(Col::A, nullptr);
    auto colb = *vpi.GetValue<int32_t, false>(Col::B, nullptr);
    EXPECT_TRUE(cola < 500 && colb < 9);
  });
}

// NOLINTNEXTLINE
TEST_F(FilterManagerTest, DisjunctionTest) {
  auto exec_ctx = MakeExecCtx();
  // Create a filter that implements: colA < 500 OR colB < 9
  FilterManager filter{exec_ctx->GetExecutionSettings()};
  filter.StartNewClause();
  filter.InsertClauseTerm([](auto exec_ctx, auto vp, auto tids, auto ctx) {
    VectorFilterExecutor::SelectLessThanVal(
        reinterpret_cast<exec::ExecutionContext *>(exec_ctx)->GetExecutionSettings(), vp, Col::A,
        GenericValue::CreateInteger(500), tids);
  });
  filter.StartNewClause();
  filter.InsertClauseTerm([](auto exec_ctx, auto vp, auto tids, auto ctx) {
    VectorFilterExecutor::SelectLessThanVal(
        reinterpret_cast<exec::ExecutionContext *>(exec_ctx)->GetExecutionSettings(), vp, Col::B,
        GenericValue::CreateInteger(9), tids);
  });

  // Create some random data.
  VectorProjection vp;
  vp.Initialize({TypeId::Integer, TypeId::Integer});
  vp.Reset(common::Constants::K_DEFAULT_VECTOR_SIZE);
  VectorOps::Generate(vp.GetColumn(Col::A), 0, 1);
  VectorOps::Generate(vp.GetColumn(Col::B), 0, 1);

  // Run the filters
  VectorProjectionIterator vpi(&vp);
  filter.RunFilters(exec_ctx.get(), &vpi);

  // Check
  EXPECT_TRUE(vpi.IsFiltered());
  EXPECT_FALSE(vpi.IsEmpty());
  vpi.ForEach([&]() {
    auto cola = *vpi.GetValue<int32_t, false>(Col::A, nullptr);
    auto colb = *vpi.GetValue<int32_t, false>(Col::B, nullptr);
    EXPECT_TRUE(cola < 500 || colb < 9);
  });
}

// NOLINTNEXTLINE
TEST_F(FilterManagerTest, MixedTaatVaatFilterTest) {
  auto exec_ctx = MakeExecCtx();
  // Create a filter that implements: colA < 500 AND colB < 9
  // The filter on column colB is implemented using a tuple-at-a-time filter.
  // Thus, the filter is a mixed VaaT and TaaT filter.
  FilterManager filter{exec_ctx->GetExecutionSettings()};
  filter.StartNewClause();
  filter.InsertClauseTerms({[](auto exec_ctx, auto vp, auto tids, auto ctx) {
                              VectorFilterExecutor::SelectLessThanVal(
                                  reinterpret_cast<exec::ExecutionContext *>(exec_ctx)->GetExecutionSettings(), vp,
                                  Col::A, GenericValue::CreateInteger(500), tids);
                            },
                            [](auto exec_ctx, auto vp, auto tids, auto ctx) {
                              VectorProjectionIterator iter(vp, tids);
                              iter.RunFilter([&]() { return *iter.GetValue<int32_t, false>(Col::B, nullptr) < 9; });
                            }});

  // Create some random data.
  VectorProjection vp;
  vp.Initialize({TypeId::Integer, TypeId::Integer});
  vp.Reset(common::Constants::K_DEFAULT_VECTOR_SIZE);
  VectorOps::Generate(vp.GetColumn(Col::A), 0, 1);
  VectorOps::Generate(vp.GetColumn(Col::B), 0, 1);

  // Run the filters
  VectorProjectionIterator vpi(&vp);
  filter.RunFilters(exec_ctx.get(), &vpi);

  // Check
  EXPECT_TRUE(vpi.IsFiltered());
  EXPECT_FALSE(vpi.IsEmpty());
  vpi.ForEach([&]() {
    auto cola = *vpi.GetValue<int32_t, false>(Col::A, nullptr);
    auto colb = *vpi.GetValue<int32_t, false>(Col::B, nullptr);
    EXPECT_TRUE(cola < 500 && colb < 9);
  });
}

// NOLINTNEXTLINE
TEST_F(FilterManagerTest, AdaptiveCheckTest) {
  auto exec_ctx = MakeExecCtx();
  uint32_t iter = 0;

  // Create a filter that implements: colA < 500 AND colB < 7
  FilterManager filter(exec_ctx->GetExecutionSettings(), true, &iter);
  filter.StartNewClause();
  filter.InsertClauseTerms(
      {[](auto exec_ctx, auto vp, auto tids, auto ctx) {
         auto *r = reinterpret_cast<uint32_t *>(ctx);
         if (*r < 1000) std::this_thread::sleep_for(500us);  // Fake a sleep.
         const auto val = GenericValue::CreateInteger(500);
         VectorFilterExecutor::SelectLessThanVal(
             reinterpret_cast<exec::ExecutionContext *>(exec_ctx)->GetExecutionSettings(), vp, Col::A, val, tids);
       },
       [](auto exec_ctx, auto vp, auto tids, auto ctx) {
         auto *r = reinterpret_cast<uint32_t *>(ctx);
         if (*r >= 1000) std::this_thread::sleep_for(500us);  // Fake a sleep.
         const auto val = GenericValue::CreateInteger(7);
         VectorFilterExecutor::SelectLessThanVal(
             reinterpret_cast<exec::ExecutionContext *>(exec_ctx)->GetExecutionSettings(), vp, Col::B, val, tids);
       }});

  // Create some random data.
  VectorProjection vp;
  vp.Initialize({TypeId::Integer, TypeId::Integer});
  vp.Reset(common::Constants::K_DEFAULT_VECTOR_SIZE);
  VectorOps::Generate(vp.GetColumn(Col::A), 0, 1);
  VectorOps::Generate(vp.GetColumn(Col::B), 0, 1);

  for (uint32_t run = 0; run < 2; run++) {
    for (uint32_t i = 0; i < 1000; i++, iter++) {
      // Remove any lingering filter.
      vp.Reset(common::Constants::K_DEFAULT_VECTOR_SIZE);
      // Create an iterator and filter it.
      VectorProjectionIterator vpi(&vp);
      filter.RunFilters(exec_ctx.get(), &vpi);
    }

    // After a while, at least one re sampling should have occurred. At that time,
    // the manager should have realized that the second filter is more selective
    // and runs faster. When we switch to the second run, the second filter is
    // hobbled and the order should reverse back.
    EXPECT_EQ(1, filter.GetClauseCount());
    const auto clause = filter.GetOptimalClauseOrder()[0];
    EXPECT_GT(clause->GetResampleCount(), 1);
    if (run == 0) {
      EXPECT_THAT(clause->GetOptimalTermOrder(), ::testing::ElementsAre(1, 0));
    } else {
      EXPECT_THAT(clause->GetOptimalTermOrder(), ::testing::ElementsAre(0, 1));
    }
  }
}

}  // namespace noisepage::execution::sql::test
