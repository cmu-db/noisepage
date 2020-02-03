#include "test_util/tpcc/workload_cached.h"

#include <random>

#include "execution/exec/execution_context.h"
#include "execution/execution_util.h"
// #include "execution/table_generator/table_generator.h"
#include "main/db_main.h"
#include "test_util/tpcc/builder.h"
#include "test_util/tpcc/database.h"
#include "test_util/tpcc/loader.h"
#include "test_util/tpcc/worker.h"

#include <string>
#include <fstream>
#include <map>

#include "parser/expression/derived_value_expression.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "test_util/test_harness.h"
#include "test_util/tpcc/plan_generator.h"

namespace terrier::tpcc {

    WorkloadCached::WorkloadCached(common::ManagedPointer<DBMain> db_main, const std::string &table_root,
                       const std::vector<std::string> &txn_names, int8_t num_threads) {
        // cache db main and members
        db_main_ = db_main;
        txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
        block_store_ = db_main_->GetStorageLayer()->GetBlockStore();
        catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
        txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();


        // Create database catalog and namespace
        Builder tpcc_builder{block_store_, catalog_, txn_manager_};

        // build the TPCC database using HashMaps where possible
        tpcc_db_ = tpcc_builder.Build(storage::index::IndexType::HASHMAP);

        std::cout << "after build, before populate" << std::endl;

        common::WorkerPool thread_pool{static_cast<uint32_t>(num_threads), {}};

        // prepare the workers
        std::vector<Worker> workers;
        workers.reserve(num_threads);
        workers.clear();
        for (int8_t i = 0; i < num_threads; i++) {
            workers.emplace_back(tpcc_db_);
        }

        // populate the tables and indexes
        Loader::PopulateDatabase(txn_manager_, tpcc_db_, &workers, &thread_pool);

        db_oid_ = tpcc_db_->db_oid_;
        std::cout << "after populate, before load queries" << std::endl;

        auto txn = txn_manager_->BeginTransaction();
        // db_oid_ = catalog_->CreateDatabase(common::ManagedPointer<transaction::TransactionContext>(txn), db_name, true);
        auto accessor = catalog_->GetAccessor(common::ManagedPointer<transaction::TransactionContext>(txn), db_oid_);

        std::cout << "got accessor" << std::endl;

        ns_oid_ = accessor->GetDefaultNamespace();

        std::cout << "got ns_oid" << std::endl;

        // Make the execution context
        execution::exec::ExecutionContext exec_ctx{db_oid_, common::ManagedPointer<transaction::TransactionContext>(txn),
                                                   nullptr, nullptr, common::ManagedPointer<catalog::CatalogAccessor>(accessor)};

        // compile the queries
        LoadTPCCQueries(&exec_ctx, table_root, txn_names);

        std::cout << "queries loaded" << std::endl;

        txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    }

    void WorkloadCached::LoadTPCCQueries(execution::exec::ExecutionContext *exec_ctx, const std::string &file_root,
            const std::vector<std::string> &txn_names) {

        std::string query;
        std::string tbl;

        PlanGenerator plan_generator(catalog_, txn_manager_, db_main_->GetStatsStorage(),
            static_cast<uint64_t>(db_main_->GetSettingsManager()->GetInt(settings::Param::task_execution_timeout)),
            tpcc_db_);

        for (auto &txn_name : txn_names) {
            auto curr = queries_.emplace(txn_name, std::vector<execution::ExecutableQuery> {});
            std::ifstream ifs_sql(file_root + txn_name + ".sql");
            std::ifstream ifs_tbl("tpcc_files/raw/" + txn_name + ".txt");
            while (getline(ifs_sql, query)) {
                getline(ifs_tbl, tbl);
                curr.first->second.emplace_back(
                        execution::ExecutableQuery(common::ManagedPointer<planner::AbstractPlanNode>(plan_generator.Generate(query, tbl)),
                                common::ManagedPointer<execution::exec::ExecutionContext>(exec_ctx)));
            }
            ifs_sql.close();
            ifs_tbl.close();
            txn_names_.push_back(txn_name);
        }
    }

    void WorkloadCached::Execute(int8_t worker_id, uint32_t num_precomputed_txns_per_worker, execution::vm::ExecutionMode mode) {
        // Shuffle the queries randomly for each thread
        auto num_queries = queries_.size();
        uint32_t index[num_queries];
        for (uint32_t i = 0; i < num_queries; ++i) index[i] = i;
        std::shuffle(&index[0], &index[num_queries], std::mt19937(worker_id));

        // Register to the metrics manager
        db_main_->GetMetricsManager()->RegisterThread();
        uint32_t counter = 0;
        for (uint32_t i = 0; i < num_precomputed_txns_per_worker; i++) {
            // Executing all the queries on by one in round robin
            auto txn = txn_manager_->BeginTransaction();
            auto accessor = catalog_->GetAccessor(common::ManagedPointer<transaction::TransactionContext>(txn), db_oid_);
            for (execution::ExecutableQuery &query : queries_.find(txn_names_[index[counter]])->second) {
                execution::exec::ExecutionContext exec_ctx{db_oid_,
                                                           common::ManagedPointer<transaction::TransactionContext>(txn),
                                                           query.GetPrinter(), query.GetOutputSchema(),
                                                           common::ManagedPointer<catalog::CatalogAccessor>(accessor)};
                query.Run(common::ManagedPointer<execution::exec::ExecutionContext>(&exec_ctx), mode);
            }
            counter = counter == num_queries - 1 ? 0:counter + 1 ;
            txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
        }

        // Unregister from the metrics manager
        db_main_->GetMetricsManager()->UnregisterThread();
    }

}  // namespace terrier::tpcc
