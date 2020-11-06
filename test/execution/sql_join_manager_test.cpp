#include <random>
#include <vector>

#include "common/hash_util.h"
#include "execution/sql/join_hash_table.h"
#include "execution/sql/join_manager.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "execution/sql_test.h"

namespace noisepage::execution::sql::test {

enum Col : uint8_t { A = 0, B = 1, C = 2, D = 3 };

struct JoinRow {
  int32_t key_;
  int32_t val_;
};

struct QueryState {
  std::unique_ptr<JoinManager> jm_;
  std::unique_ptr<JoinHashTable> jht1_;
  std::unique_ptr<JoinHashTable> jht2_;
};

class JoinManagerTest : public SqlBasedTest {};

// Build a join hash table.
void BuildHT(JoinHashTable *jht, bool is_a_key, uint32_t a_max, uint32_t b_max) {
  const auto bound = is_a_key ? a_max : b_max;
  for (uint32_t i = 0; i < bound; i++) {
    auto cola = i % a_max;
    auto colb = i % b_max;
    auto hash_val = common::HashUtil::Hash(is_a_key ? cola : colb);
    auto join_row = reinterpret_cast<JoinRow *>(jht->AllocInputTuple(hash_val));
    join_row->key_ = is_a_key ? cola : colb;
    join_row->val_ = colb;
  }
  jht->Build();
}

// NOLINTNEXTLINE
TEST_F(JoinManagerTest, TwoWayJoin) {
  auto exec_ctx = MakeExecCtx();
  auto exec_settings = exec_ctx->GetExecutionSettings();
  QueryState query_state;

  query_state.jht1_ = std::make_unique<JoinHashTable>(exec_settings, exec_ctx.get(), sizeof(JoinRow), false);
  query_state.jht2_ = std::make_unique<JoinHashTable>(exec_settings, exec_ctx.get(), sizeof(JoinRow), false);
  query_state.jm_ = std::make_unique<JoinManager>(exec_settings, &query_state);
  // The first join.
  query_state.jm_->InsertJoinStep(*query_state.jht1_, {0}, [](auto exec_ctx, auto vp, auto tids, auto ctx) {
    auto *query_state = reinterpret_cast<QueryState *>(ctx);
    query_state->jm_->PrepareSingleJoin(vp, tids, 0);
  });
  // The second join.
  query_state.jm_->InsertJoinStep(*query_state.jht2_, {1}, [](auto exec_ctx, auto vp, auto tids, auto ctx) {
    auto *query_state = reinterpret_cast<QueryState *>(ctx);
    query_state->jm_->PrepareSingleJoin(vp, tids, 1);
  });

  // Build table.
  BuildHT(query_state.jht1_.get(), true, 1000, 10);
  BuildHT(query_state.jht2_.get(), false, 10, 10);

  // Input to probe.
  VectorProjection vec_proj;
  vec_proj.Initialize({TypeId::Integer, TypeId::Integer});
  vec_proj.Reset(common::Constants::K_DEFAULT_VECTOR_SIZE);
  VectorOps::Generate(vec_proj.GetColumn(0), 0, 1);
  VectorOps::Generate(vec_proj.GetColumn(1), 0, 1);

  for (uint32_t i = 0; i < 1000; i++) {
    // Run join.
    vec_proj.Reset(common::Constants::K_DEFAULT_VECTOR_SIZE);
    VectorProjectionIterator vpi(&vec_proj);
    query_state.jm_->SetInputBatch(exec_ctx.get(), &vpi);

    uint32_t count = 0;
#if 1
    while (query_state.jm_->Next()) {
      const HashTableEntry **matches[2];
      query_state.jm_->GetOutputBatch(matches);
      count += vpi.GetSelectedTupleCount();
    }
#else
    vpi.ForEach([&]() {
      const HashTableEntry *matches[2] = {nullptr};
      query_state.jm_->GetOutputBatch(&vpi, matches);
      for (auto iter1 = matches[0]; iter1 != nullptr; iter1 = iter1->next) {
        auto *jr1 = iter1->PayloadAs<JoinRow>();
        if (jr1->cola == *vpi.GetValue<int32_t, false>(0, nullptr)) {
          for (auto iter2 = matches[1]; iter2 != nullptr; iter2 = iter2->next) {
            auto *jr2 = iter2->PayloadAs<JoinRow>();
            if (jr2->colb == *vpi.GetValue<int32_t, false>(1, nullptr)) {
              count++;
            }
          }
        }
      }
    });
#endif
    EXPECT_EQ(10u, count);
  }
}

}  // namespace noisepage::execution::sql::test
