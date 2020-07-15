#include <random>
#include <vector>

#include "common/hash_util.h"
#include "execution/sql/join_hash_table.h"
#include "execution/sql/join_manager.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "execution/sql_test.h"

namespace terrier::execution::sql::test {

enum Col : uint8_t { A = 0, B = 1, C = 2, D = 3 };

struct JoinRow {
  int32_t key;
  int32_t val;
};

struct QueryState {
  std::unique_ptr<JoinManager> jm;
  std::unique_ptr<JoinHashTable> jht1;
  std::unique_ptr<JoinHashTable> jht2;
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
    join_row->key = is_a_key ? cola : colb;
    join_row->val = colb;
  }
  jht->Build();
}

TEST_F(JoinManagerTest, TwoWayJoin) {
  MemoryPool mem_pool(nullptr);
  QueryState query_state;

  auto exec_ctx = MakeExecCtx();
  auto exec_settings = exec_ctx->GetExecutionSettings();

  query_state.jht1 = std::make_unique<JoinHashTable>(exec_settings, &mem_pool, sizeof(JoinRow), false);
  query_state.jht2 = std::make_unique<JoinHashTable>(exec_settings, &mem_pool, sizeof(JoinRow), false);
  query_state.jm = std::make_unique<JoinManager>(exec_settings, &query_state);
  // The first join.
  query_state.jm->InsertJoinStep(*query_state.jht1, {0}, [](auto exec_ctx, auto vp, auto tids, auto ctx) {
    auto *query_state = reinterpret_cast<QueryState *>(ctx);
    query_state->jm->PrepareSingleJoin(vp, tids, 0);
  });
  // The second join.
  query_state.jm->InsertJoinStep(*query_state.jht2, {1}, [](auto exec_ctx, auto vp, auto tids, auto ctx) {
    auto *query_state = reinterpret_cast<QueryState *>(ctx);
    query_state->jm->PrepareSingleJoin(vp, tids, 1);
  });

  // Build table.
  BuildHT(query_state.jht1.get(), true, 1000, 10);
  BuildHT(query_state.jht2.get(), false, 10, 10);

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
    query_state.jm->SetInputBatch(exec_ctx.get(), &vpi);

    uint32_t count = 0;
#if 1
    while (query_state.jm->Next()) {
      const HashTableEntry **matches[2];
      query_state.jm->GetOutputBatch(matches);
      count += vpi.GetSelectedTupleCount();
    }
#else
    vpi.ForEach([&]() {
      const HashTableEntry *matches[2] = {nullptr};
      query_state.jm->GetOutputBatch(&vpi, matches);
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

}  // namespace terrier::execution::sql::test
