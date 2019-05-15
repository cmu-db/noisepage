#include <random>
#include <vector>
#include "common/macros.h"
#include "common/scoped_timer.h"
#include "common/worker_pool.h"
#include "storage/garbage_collector.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_manager.h"
#include "util/tpcc/builder.h"
#include "util/tpcc/database.h"
#include "util/tpcc/delivery.h"
#include "util/tpcc/loader.h"
#include "util/tpcc/new_order.h"
#include "util/tpcc/order_status.h"
#include "util/tpcc/payment.h"
#include "util/tpcc/stock_level.h"
#include "util/tpcc/worker.h"
#include "util/tpcc/workload.h"

namespace terrier::storage::index {

class TPCCTests : public TerrierTest {
 public:
  void SetUp() override { TerrierTest::SetUp(); }

  void TearDown() override { TerrierTest::TearDown(); }
};

// NOLINTNEXTLINE
TEST_F(TPCCTests, TPCCTest) {}

}  // namespace terrier::storage::index
