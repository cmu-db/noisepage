#pragma once

#include <memory>
#include <queue>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "main/db_main.h"
#include "optimizer/optimizer.h"
#include "parser/postgresparser.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "test_util/test_harness.h"

namespace terrier {

    namespace tpcc {
        class Database;
    };

    namespace execution {
        class ExecutableQuery;
    };

    // Modified from TpccPlanTest
    class PlanGenerator {
    public:
        PlanGenerator(common::ManagedPointer<catalog::Catalog> catalog,
                                     common::ManagedPointer<transaction::TransactionManager> txn_manager,
                                     common::ManagedPointer<optimizer::StatsStorage> stats_storage,
                                     int64_t task_execution_timeout,
                                     tpcc::Database *tpcc_db);

        void BeginTransaction();
        void EndTransaction(bool commit);

        std::unique_ptr<planner::AbstractPlanNode> Optimize(const std::string &query, catalog::table_oid_t tbl_oid,
                                                            parser::StatementType stmt_type);

        transaction::TransactionContext *GetTxn() {
            return txn_;
        }

        catalog::CatalogAccessor *GetAccessor() {
            return accessor_;
        }

        /**
         * convert table name to table oid
         * @param tbl name of table
         * @return table oid
         */
        catalog::table_oid_t GetTbl(const std::string &tbl) {
            return tbl_map_.find(tbl)->second;
        }

        /**
         * convert statement name to statement type
         * @param stmt name of statement
         * @return statement type
         */
        parser::StatementType GetStmt(const std::string &stmt) {
            return stmt_map_.find(stmt[0])->second;
        }

        // Infrastucture
        common::ManagedPointer<catalog::Catalog> catalog_;
        common::ManagedPointer<transaction::TransactionManager> txn_manager_;
        common::ManagedPointer<optimizer::StatsStorage> stats_storage_;

        uint64_t task_execution_timeout_;

        // Optimizer transaction
        transaction::TransactionContext *txn_;
        catalog::CatalogAccessor *accessor_;

        tpcc::Database *tpcc_db_;
        catalog::db_oid_t db_;

        std::map <std::string, catalog::table_oid_t> tbl_map_;
        std::map <char, parser::StatementType> stmt_map_;
    };

}  // namespace terrier
