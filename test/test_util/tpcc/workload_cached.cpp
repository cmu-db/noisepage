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
#include "execution/execution_util.h"

namespace terrier::tpcc {

    WorkloadCached::WorkloadCached(common::ManagedPointer<DBMain> db_main, const std::string &table_root,
                       const std::vector<std::string> &txn_names, int8_t num_threads) {
        // cache db main and members
        db_main_ = db_main;
        txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
        block_store_ = db_main_->GetStorageLayer()->GetBlockStore();
        catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
        txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();

        // build the TPCC database using HashMaps where possible
        Builder tpcc_builder{block_store_, catalog_, txn_manager_};
        tpcc_db_ = tpcc_builder.Build(storage::index::IndexType::HASHMAP);
        db_oid_ = tpcc_db_->db_oid_;
/*
        // populate the tables and indexes
        // prepare the thread pool and workers
        common::WorkerPool thread_pool{static_cast<uint32_t>(num_threads), {}};
        std::vector<Worker> workers;
        workers.reserve(num_threads);
        workers.clear();
        for (int8_t i = 0; i < num_threads; i++) {
            workers.emplace_back(tpcc_db_);
        }
        Loader::PopulateDatabase(txn_manager_, tpcc_db_, &workers, &thread_pool);*/

        // compile the queries
        LoadTPCCQueries(table_root, txn_names);
    }

    void WorkloadCached::LoadTPCCQueries(const std::string &file_root, const std::vector<std::string> &txn_names) {
        std::string query;
        std::string tbl;

        PlanGenerator plan_generator(catalog_, txn_manager_, db_main_->GetStatsStorage(),
            static_cast<uint64_t>(db_main_->GetSettingsManager()->GetInt(settings::Param::task_execution_timeout)),
            tpcc_db_);

        for (auto &txn_name : txn_names) {
            // read queries and tbls from files
            auto curr = queries_.emplace(txn_name, std::vector<execution::ExecutableQuery> {});
            std::ifstream ifs_sql(file_root + txn_name + ".sql");
            std::ifstream ifs_tbl(file_root + txn_name + ".txt");

            while (getline(ifs_sql, query)) {
                getline(ifs_tbl, tbl);

                plan_generator.BeginTransaction();
                // generate plan node
                execution::exec::ExecutionContext exec_ctx{db_oid_, common::ManagedPointer<transaction::TransactionContext>(plan_generator.GetTxn()),
                                                           nullptr, nullptr, common::ManagedPointer<catalog::CatalogAccessor>(plan_generator.GetAccessor())};

                std::unique_ptr<planner::AbstractPlanNode> plan_node = plan_generator.Optimize(
                        query, plan_generator.GetTbl(tbl), plan_generator.GetStmt(query));

                std::cout << std::setw(4) << plan_node->ToJson() << std::endl;

                // generate executable query and emplace it into the vector; break down here
                curr.first->second.emplace_back(execution::ExecutableQuery{common::ManagedPointer<planner::AbstractPlanNode>(plan_node),
                                                                           common::ManagedPointer<execution::exec::ExecutionContext>(&exec_ctx)});
                plan_generator.EndTransaction(true);
            }
            ifs_sql.close();
            ifs_tbl.close();

            // record the name of transaction
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
