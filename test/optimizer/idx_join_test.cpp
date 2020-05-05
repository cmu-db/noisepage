#include <memory>
#include <stack>
#include <utility>
#include <vector>

#include "main/db_main.h"
#include "network/connection_context.h"
#include "network/postgres/statement.h"
#include "traffic_cop/traffic_cop_defs.h"
#include "optimizer/binding.h"
#include "optimizer/optimizer_context.h"
#include "optimizer/optimizer_defs.h"
#include "optimizer/optimizer_task.h"
#include "optimizer/optimizer_task_pool.h"
#include "optimizer/pattern.h"

#include "test_util/test_harness.h"

namespace terrier::optimizer {

struct IdxJoinTest : public TerrierTest {
  void ExecuteSQL(std::string sql, network::QueryType qtype) {
    tcop->BeginTransaction(common::ManagedPointer(&context_));
    auto parse = tcop->ParseQuery(sql, common::ManagedPointer(&context_));
    auto stmt = network::Statement(std::move(parse));
    auto result = tcop->BindQuery(common::ManagedPointer(&context_), common::ManagedPointer(&stmt), common::ManagedPointer(&params));
    TERRIER_ASSERT(result.type_ == trafficcop::ResultType::COMPLETE, "Bind should have succeeded");

    auto plan = tcop->OptimizeBoundQuery(common::ManagedPointer(&context_), stmt.ParseResult());
    result = tcop->ExecuteCreateStatement(common::ManagedPointer(&context_), common::ManagedPointer(plan), qtype);
    TERRIER_ASSERT(result.type_ == trafficcop::ResultType::COMPLETE, "Execute should have succeeded");

    tcop->EndTransaction(common::ManagedPointer(&context_), network::QueryType::QUERY_COMMIT);
  }

  void SetUp() override { 
    TerrierTest::SetUp();

    std::unordered_map<settings::Param, settings::ParamInfo> param_map;
    settings::SettingsManager::ConstructParamMap(param_map);

    db_main_ = terrier::DBMain::Builder()
      .SetUseGC(true)
      .SetSettingsParameterMap(std::move(param_map))
      .SetUseSettingsManager(true)
      .SetUseCatalog(true)
      .SetUseStatsStorage(true)
      .SetUseTrafficCop(true)
      .SetUseExecution(true)
      .Build();

    std::vector<type::TransientValue> params;
    auto tcop = db_main_->GetTrafficCop();
    auto oids = tcop->CreateTempNamespace(network::connection_id_t(0), "terrier");
    context_.SetDatabaseName("terrier");
    context_.SetDatabaseOid(oids.first);
    context_.SetTempNamespaceOid(oids.second);

    ExecuteSQL("CREATE TABLE foo (col1 INT, col2 INT, col3 INT);", network::QueryType::QUERY_CREATE_TABLE);
    ExecuteSQL("CREATE INDEX foo_idx (col1, col2, col3);", network::QueryType::QUERY_CREATE_INDEX);
  }

  void TearDown() override { TerrierTest::TearDown(); }

  network::ConnectionContext context_;
  std::unique_ptr<DBMain> db_main_;
};

// NOLINTNEXTLINE
TEST_F(IdxJoinTest, SimpleIdxJoinTest) {
  auto sql = "SELECT foo.col1 FROM foo, foo as b WHERE foo.col1 = b.col1";
}

}  // namespace terrier::optimizer
