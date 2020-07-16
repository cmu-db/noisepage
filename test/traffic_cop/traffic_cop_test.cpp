#include "traffic_cop/traffic_cop.h"

#include <memory>
#include <pqxx/pqxx>  // NOLINT
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/settings.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "network/connection_handle_factory.h"
#include "network/terrier_server.h"
#include "storage/garbage_collector.h"
#include "test_util/manual_packet_util.h"
#include "test_util/test_harness.h"
#include "traffic_cop/traffic_cop_defs.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"

namespace terrier::trafficcop {

class TrafficCopTests : public TerrierTest {
 protected:
  void SetUp() override {}
};

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, BasicTest) {
  try {
    pqxx::connection connection(fmt::format("host=127.0.0.1 port={0} user={1} sslmode=disable application_name=psql",
                                            15721, catalog::DEFAULT_DATABASE));

    pqxx::work txn1(connection);
    txn1.exec("CREATE INDEX field1_index on USERTABLE (FIELD1);");

    txn1.commit();
  } catch (const std::exception &e) {
    EXPECT_TRUE(false);
  }
}

}  // namespace terrier::trafficcop
