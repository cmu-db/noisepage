#pragma once

#include <utility>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "storage/storage_defs.h"
#include "execution/vm/module.h"

namespace terrier::execution::exec {
    class ExecutionContext;
}

namespace terrier::catalog {
    class Catalog;
}

namespace terrier::transaction {
    class TransactionManager;
}

namespace terrier::execution {
    class ExecutableQuery;
}

namespace terrier {
    class DBMain;
}

namespace terrier::common {
    class WorkerPool;
}

namespace terrier::tpcc {
    class Database;

/**
 * Class that can load the TPCC tables, compile the TPCC queries, and execute the TPCC workload
 */
    class WorkloadCached {
    public:
        WorkloadCached(common::ManagedPointer<DBMain> db_main, const std::string &table_root,
                 const std::vector<std::string> &txn_names, int8_t num_threads);

        /**
         * Function to invoke for a single worker thread to invoke the TPCH queries
         * @param worker_id 1-indexed thread id
         */
        void Execute(int8_t worker_id, uint32_t num_precomputed_txns_per_worker, execution::vm::ExecutionMode mode);

    private:
        void LoadTPCCQueries(execution::exec::ExecutionContext *exec_ctx, const std::string &file_root,
                const std::vector<std::string> &queries);

        common::ManagedPointer<DBMain> db_main_;
        common::ManagedPointer<storage::BlockStore> block_store_;
        common::ManagedPointer<catalog::Catalog> catalog_;
        common::ManagedPointer<transaction::TransactionManager> txn_manager_;
        catalog::db_oid_t db_oid_;
        catalog::namespace_oid_t ns_oid_;
        Database *tpcc_db_;

        std::map<std::string, std::vector<execution::ExecutableQuery> > queries_;
        std::vector<std::string> txn_names_;
    };

}  // namespace terrier::tpcc
