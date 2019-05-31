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

namespace tpl::sql::test {

class FilterManagerTest : public SqlBasedTest {};

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
  auto exec = ExecutionStructures::Instance();
  auto txn = exec->GetTxnManager()->BeginTransaction();
  auto test_db_ns = exec->GetTestDBAndNS();
  auto catalog_table = exec->GetCatalog()->GetUserTable(txn, test_db_ns.first, test_db_ns.second, "test_1");
  TableVectorIterator tvi(!test_db_ns.first, !test_db_ns.second, !catalog_table->Oid(), txn);
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
  exec->GetTxnManager()->Commit(txn, [](void *) {}, nullptr);
}

// NOLINTNEXTLINE
TEST_F(FilterManagerTest, AdaptiveFilterManagerTest) {
  FilterManager filter(bandit::Policy::Kind::EpsilonGreedy);
  filter.StartNewClause();
  filter.InsertClauseFlavor(Hobbled_TaaT_Lt_500);
  filter.InsertClauseFlavor(Vectorized_Lt_500);
  filter.Finalize();
  auto exec = ExecutionStructures::Instance();
  auto txn = exec->GetTxnManager()->BeginTransaction();
  auto test_db_ns = exec->GetTestDBAndNS();
  auto catalog_table = exec->GetCatalog()->GetUserTable(txn, test_db_ns.first, test_db_ns.second, "test_1");
  TableVectorIterator tvi(!test_db_ns.first, !test_db_ns.second, !catalog_table->Oid(), txn);
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
  exec->GetTxnManager()->Commit(txn, [](void *) {}, nullptr);
}

}  // namespace tpl::sql::test
