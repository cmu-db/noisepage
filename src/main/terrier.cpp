#include <gflags/gflags.h>

#include <memory>
#include <unordered_map>
#include <utility>

#include "execution/sql/ddl_executors.h"
#include "loggers/loggers_util.h"
#include "main/db_main.h"
#include "planner/plannodes/create_table_plan_node.h"
#include "settings/settings_manager.h"
#include "type/transient_value_factory.h"

int main(int argc, char *argv[]) {
  // initialize loggers
  // Parse Setting Values
  ::google::SetUsageMessage("Usage Info: \n");
  ::google::ParseCommandLineFlags(&argc, &argv, true);

  terrier::LoggersUtil::Initialize();

  // initialize stat registry
  auto main_stat_reg =
      std::make_unique<terrier::common::StatisticsRegistry>();  // TODO(Matt): do we still want this thing?

  std::unordered_map<terrier::settings::Param, terrier::settings::ParamInfo> param_map;
  terrier::settings::SettingsManager::ConstructParamMap(param_map);

  auto db_main = terrier::DBMain::Builder()
                     .SetSettingsParameterMap(std::move(param_map))
                     .SetUseSettingsManager(true)
                     .SetUseMetrics(true)
                     .SetUseMetricsThread(true)
                     .SetUseLogging(true)
                     .SetUseGC(true)
                     .SetUseCatalog(true)
                     .SetUseGCThread(true)
                     .SetUseStatsStorage(true)
                     .SetUseTrafficCop(true)
                     .SetUseNetwork(true)
                     .Build();

  auto *txn = db_main->GetTransactionLayer()->GetTransactionManager()->BeginTransaction();
  const auto db = db_main->GetCatalogLayer()->GetCatalog()->GetDatabaseOid(terrier::common::ManagedPointer(txn),
                                                                           terrier::catalog::DEFAULT_DATABASE);
  db_main->GetTransactionLayer()->GetTransactionManager()->Commit(
      txn, terrier::transaction::TransactionUtil::EmptyCallback, nullptr);

  auto col1 = terrier::catalog::Schema::Column(
      "col1", terrier::type::TypeId::INTEGER, true,
      terrier::parser::ConstantValueExpression(
          terrier::type::TransientValueFactory::GetNull(terrier::type::TypeId::INTEGER)));
  auto col2 = terrier::catalog::Schema::Column(
      "col2", terrier::type::TypeId::VARCHAR, 25, true,
      terrier::parser::ConstantValueExpression(
          terrier::type::TransientValueFactory::GetNull(terrier::type::TypeId::VARCHAR)));
  auto table_schema =
      std::make_unique<terrier::catalog::Schema>(std::vector<terrier::catalog::Schema::Column>{col1, col2});

  txn = db_main->GetTransactionLayer()->GetTransactionManager()->BeginTransaction();
  auto accessor = db_main->GetCatalogLayer()->GetCatalog()->GetAccessor(terrier::common::ManagedPointer(txn), db);

  terrier::planner::CreateTablePlanNode::Builder builder;
  auto create_table_node = builder.SetNamespaceOid(accessor->GetDefaultNamespace())
                               .SetTableSchema(std::move(table_schema))
                               .SetTableName("foo")
                               .SetBlockStore(db_main->GetStorageLayer()->GetBlockStore())
                               .Build();
  bool result UNUSED_ATTRIBUTE = terrier::execution::sql::DDLExecutors::CreateTableExecutor(
      terrier::common::ManagedPointer<terrier::planner::CreateTablePlanNode>(create_table_node),
      terrier::common::ManagedPointer<terrier::catalog::CatalogAccessor>(accessor), db);

  TERRIER_ASSERT(true, "Failed to create table foo.");

  db_main->GetTransactionLayer()->GetTransactionManager()->Commit(
      txn, terrier::transaction::TransactionUtil::EmptyCallback, nullptr);

  db_main->Run();

  terrier::LoggersUtil::ShutDown();
}
