#include <chrono>  // NOLINT
#include <limits>
#include <memory>
#include <numeric>
#include <random>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "execution/sql_test.h"  // NOLINT

#include "execution/sql/catalog.h"
#include "execution/sql/filter_manager.h"
#include "execution/sql/table_vector_iterator.h"

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
  return pci->FilterColByVal<std::less>(Col::A, param);
}

TEST_F(FilterManagerTest, SimpleFilterManagerTest) {
  FilterManager filter(bandit::Policy::FixedAction);
  filter.StartNewClause();
  filter.InsertClauseFlavor(TaaT_Lt_500);
  filter.InsertClauseFlavor(Vectorized_Lt_500);
  filter.Finalize();

  TableVectorIterator tvi(static_cast<u16>(TableId::Test1));
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

TEST_F(FilterManagerTest, AdaptiveFilterManagerTest) {
  FilterManager filter(bandit::Policy::EpsilonGreedy);
  filter.StartNewClause();
  filter.InsertClauseFlavor(Hobbled_TaaT_Lt_500);
  filter.InsertClauseFlavor(Vectorized_Lt_500);
  filter.Finalize();

  TableVectorIterator tvi(static_cast<u16>(TableId::Test1));
  for (tvi.Init(); tvi.Advance();) {
    auto *pci = tvi.vector_projection_iterator();

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

}  // namespace tpl::sql::test
