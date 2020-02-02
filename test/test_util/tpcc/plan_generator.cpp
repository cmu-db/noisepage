#include "test_util/tpcc/plan_generator.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "binder/bind_node_visitor.h"
#include "catalog/catalog_accessor.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/optimizer.h"
#include "optimizer/properties.h"
#include "optimizer/property_set.h"
#include "optimizer/query_to_operator_transformer.h"
#include "optimizer/statistics/stats_storage.h"
#include "parser/postgresparser.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/insert_plan_node.h"
#include "test_util/test_harness.h"
#include "test_util/tpcc/builder.h"
#include "transaction/transaction_manager.h"

namespace terrier {

    PlanGenerator::PlanGenerator(common::ManagedPointer<catalog::Catalog> catalog,
                                 common::ManagedPointer<transaction::TransactionManager> txn_manager,
                                 common::ManagedPointer<optimizer::StatsStorage> stats_storage,
                                 int64_t task_execution_timeout,
                                 tpcc::Database *tpcc_db) :
            catalog_ (catalog), txn_manager_(txn_manager), stats_storage_(stats_storage),
            task_execution_timeout_(task_execution_timeout), tpcc_db_(tpcc_db) {
        db_ = tpcc_db_->db_oid_;
        tbl_map_["tbl_item_"] = tpcc_db_->item_table_oid_;
        tbl_map_["tbl_warehouse_"] = tpcc_db_->warehouse_table_oid_;
        tbl_map_["tbl_stock_"] = tpcc_db_->stock_table_oid_;
        tbl_map_["tbl_district_"] = tpcc_db_->district_table_oid_;
        tbl_map_["tbl_customer_"] = tpcc_db_->customer_table_oid_;
        tbl_map_["tbl_history_"] = tpcc_db_->history_table_oid_;
        tbl_map_["tbl_new_order_"] = tpcc_db_->new_order_table_oid_;
        tbl_map_["tbl_order_"] = tpcc_db_->order_table_oid_;
        tbl_map_["tbl_order_line_"] = tpcc_db_->order_line_table_oid_;

        stmt_map_['S'] = parser::StatementType::SELECT;
        stmt_map_['I'] = parser::StatementType::INSERT;
        stmt_map_['D'] = parser::StatementType::DELETE;
        stmt_map_['U'] = parser::StatementType::UPDATE;
    }

    void PlanGenerator::BeginTransaction() {
        txn_ = txn_manager_->BeginTransaction();
        accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_).release();
    }

    void PlanGenerator::EndTransaction(bool commit) {
        delete accessor_;
        if (commit)
            txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
        else
            txn_manager_->Abort(txn_);
    }

    std::unique_ptr<planner::AbstractPlanNode> PlanGenerator::Optimize(const std::string &query,
                                                                      catalog::table_oid_t tbl_oid,
                                                                      parser::StatementType stmt_type) {
        auto stmt_list = parser::PostgresParser::BuildParseTree(query);

        // Bind + Transform
        auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn_), db_);
        auto *binder = new binder::BindNodeVisitor(common::ManagedPointer(accessor), "tpcc");
        binder->BindNameToNode(stmt_list.GetStatement(0), &stmt_list);
        auto *transformer = new optimizer::QueryToOperatorTransformer(common::ManagedPointer(accessor));
        auto plan = transformer->ConvertToOpExpression(stmt_list.GetStatement(0), &stmt_list);
        delete binder;
        delete transformer;

        auto optimizer = new optimizer::Optimizer(std::make_unique<optimizer::TrivialCostModel>(), task_execution_timeout_);
        std::unique_ptr<planner::AbstractPlanNode> out_plan;
        if (stmt_type == parser::StatementType::SELECT) {
            auto sel_stmt = stmt_list.GetStatement(0).CastManagedPointerTo<parser::SelectStatement>();

            // Output
            auto output = sel_stmt->GetSelectColumns();

            // Property Sort
            auto property_set = new optimizer::PropertySet();
            std::vector<optimizer::OrderByOrderingType> sort_dirs;
            std::vector<common::ManagedPointer<parser::AbstractExpression>> sort_exprs;
            if (sel_stmt->GetSelectOrderBy()) {
                auto order_by = sel_stmt->GetSelectOrderBy();
                auto types = order_by->GetOrderByTypes();
                auto exprs = order_by->GetOrderByExpressions();
                for (size_t idx = 0; idx < order_by->GetOrderByExpressionsSize(); idx++) {
                    sort_exprs.emplace_back(exprs[idx]);
                    sort_dirs.push_back(types[idx] == parser::OrderType::kOrderAsc ? optimizer::OrderByOrderingType::ASC
                                                                                   : optimizer::OrderByOrderingType::DESC);
                }

                auto sort_prop = new optimizer::PropertySort(sort_exprs, sort_dirs);
                property_set->AddProperty(sort_prop);
            }

            auto query_info = optimizer::QueryInfo(parser::StatementType::SELECT, std::move(output), property_set);
            out_plan = optimizer->BuildPlanTree(txn_, accessor_, stats_storage_.Get(), query_info, std::move(plan));
            delete property_set;
        } else if (stmt_type == parser::StatementType::INSERT) {
            auto ins_stmt = stmt_list.GetStatement(0).CastManagedPointerTo<parser::InsertStatement>();

            auto &schema = accessor_->GetSchema(tbl_oid);
            std::vector<catalog::col_oid_t> col_oids;
            for (auto &col : *ins_stmt->GetInsertColumns()) {
                col_oids.push_back(schema.GetColumn(col).Oid());
            }

            auto property_set = new optimizer::PropertySet();
            auto query_info = optimizer::QueryInfo(stmt_type, {}, property_set);
            out_plan = optimizer->BuildPlanTree(txn_, accessor_, stats_storage_.Get(), query_info, std::move(plan));
            delete property_set;

            EXPECT_EQ(out_plan->GetPlanNodeType(), planner::PlanNodeType::INSERT);
            auto insert = reinterpret_cast<planner::InsertPlanNode *>(out_plan.get());
            EXPECT_EQ(insert->GetDatabaseOid(), db_);
            EXPECT_EQ(insert->GetNamespaceOid(), accessor_->GetDefaultNamespace());
            EXPECT_EQ(insert->GetTableOid(), tbl_oid);
            EXPECT_EQ(insert->GetParameterInfo(), col_oids);
            EXPECT_EQ(insert->GetBulkInsertCount(), 1);

            auto values = *(ins_stmt->GetValues());
            EXPECT_EQ(values.size(), 1);
            EXPECT_EQ(insert->GetValues(0).size(), values[0].size());
            for (size_t idx = 0; idx < ins_stmt->GetValues()->size(); idx++) {
                EXPECT_EQ(*(values[0][idx].Get()), *(insert->GetValues(0)[idx].Get()));
            }
        } else {
            auto property_set = new optimizer::PropertySet();
            auto query_info = optimizer::QueryInfo(stmt_type, {}, property_set);
            out_plan = optimizer->BuildPlanTree(txn_, accessor_, stats_storage_.Get(), query_info, std::move(plan));
            delete property_set;
        }

        delete optimizer;
        return out_plan;
    }
}  // namespace terrier
