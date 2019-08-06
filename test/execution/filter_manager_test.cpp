#include <chrono>  // NOLINT
#include <limits>
#include <memory>
#include <numeric>
#include <random>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "execution/sql_test.h"  // NOLINT

#include "catalog/catalog.h"
#include "execution/sql/filter_manager.h"
#include "execution/sql/table_vector_iterator.h"
#include "type/type_id.h"

namespace terrier::sql::test {

class FilterManagerTest : public SqlBasedTest {
  void SetUp() override {
    // Create the test tables
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();
    sql::TableGenerator table_generator{exec_ctx_.get(), BlockStore(), NSOid()};
    table_generator.GenerateTestTables();
  }

 protected:
  /**
   * Execution context to use for the test
   */
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};

enum Col : u8 { A = 0, B = 1, C = 2, D = 3 };

u32 TaaT_Lt_500(ProjectedColumnsIterator *pci) {
  pci->RunFilter([pci]() -> bool {
    auto cola = *pci->Get<i32, false>(Col::A, nullptr);
    return cola < 500;
  });
  return pci->num_selected();
}

u32 Hobbled_TaaT_Lt_500(ProjectedColumnsIterator *pci) {
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  return TaaT_Lt_500(pci);
}

u32 Vectorized_Lt_500(ProjectedColumnsIterator *pci) {
  ProjectedColumnsIterator::FilterVal param{.i = 500};
  return pci->FilterColByVal<std::less>(Col::A, terrier::type::TypeId ::INTEGER, param);
}

// NOLINTNEXTLINE
TEST_F(FilterManagerTest, SimpleFilterManagerTest) {
  FilterManager filter(bandit::Policy::Kind::FixedAction);
  filter.StartNewClause();
  filter.InsertClauseFlavor(TaaT_Lt_500);
  filter.InsertClauseFlavor(Vectorized_Lt_500);
  filter.Finalize();
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  TableVectorIterator tvi(!table_oid, exec_ctx_.get());
  for (tvi.Init(); tvi.Advance();) {
    auto *pci = tvi.projected_columns_iterator();

    // Run the filters
    filter.RunFilters(pci);

    // Check
    pci->ForEach([pci]() {
      auto cola = *pci->Get<i32, false>(Col::A, nullptr);
      EXPECT_LT(cola, 500);
    });
  }
}

// NOLINTNEXTLINE
TEST_F(FilterManagerTest, AdaptiveFilterManagerTest) {
  FilterManager filter(bandit::Policy::Kind::EpsilonGreedy);
  filter.StartNewClause();
  filter.InsertClauseFlavor(Hobbled_TaaT_Lt_500);
  filter.InsertClauseFlavor(Vectorized_Lt_500);
  filter.Finalize();
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  TableVectorIterator tvi(!table_oid, exec_ctx_.get());
  for (tvi.Init(); tvi.Advance();) {
    auto *pci = tvi.projected_columns_iterator();

    // Run the filters
    filter.RunFilters(pci);

    // Check
    pci->ForEach([pci]() {
      auto cola = *pci->Get<i32, false>(Col::A, nullptr);
      EXPECT_LT(cola, 500);
    });
  }

  // The vectorized filter better be the optimal!
  EXPECT_EQ(1u, filter.GetOptimalFlavorForClause(0));
}

}  // namespace terrier::sql::test
