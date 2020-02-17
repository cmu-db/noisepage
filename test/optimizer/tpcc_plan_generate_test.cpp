#include <string>
#include <fstream>
#include <map>

#include "parser/expression/derived_value_expression.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "test_util/test_harness.h"
#include "test_util/tpcc/tpcc_plan_test.h"

namespace terrier {

  struct TpccPlanGenerateTests : public TpccPlanTest {};

// NOLINTNEXTLINE
  TEST_F(TpccPlanGenerateTests, GenerateAll) {
    const int test_cnt = 8;
    std::string names[test_cnt];
    names[0] = "delivery";
    names[1] = "index_scan";
    names[2] = "neworder";
    names[3] = "orderstatus";
    names[4] = "payment";
    names[5] = "seq_scan";
    names[6] = "stocklevel";
    names[7] = "temp";
    std::string *names_use;
    names_use = &names[0];
    int test_cnt_use = 7;

    std::map <std::string, catalog::table_oid_t> tbl_map;
    tbl_map["tbl_item_"] = tbl_item_;
    tbl_map["tbl_warehouse_"] = tbl_warehouse_;
    tbl_map["tbl_stock_"] = tbl_stock_;
    tbl_map["tbl_district_"] = tbl_district_;
    tbl_map["tbl_customer_"] = tbl_customer_;
    tbl_map["tbl_history_"] = tbl_history_;
    tbl_map["tbl_new_order_"] = tbl_new_order_;
    tbl_map["tbl_order_"] = tbl_order_;
    tbl_map["tbl_order_line_"] = tbl_order_line_;

    std::map <char, parser::StatementType> stmt_map;
    stmt_map['S'] = parser::StatementType::SELECT;
    stmt_map['I'] = parser::StatementType::INSERT;
    stmt_map['D'] = parser::StatementType::DELETE;
    stmt_map['U'] = parser::StatementType::UPDATE;

    const std::string tpcc_file_root = "../tpcc_files/";

    std::string query;
    std::string tbl;
    std::ofstream rec(tpcc_file_root + "started_list.txt");

    for (int i = 0; i < test_cnt_use; i++) {
      std::ifstream ifs_sql(tpcc_file_root + "raw/" + names_use[i] + ".sql");
      std::ifstream ifs_tbl(tpcc_file_root + "raw/" + names_use[i] + ".txt");
      int query_cnt = 0;
      rec << names_use[i] << std::endl;
      while (getline(ifs_sql, query)) {
        getline(ifs_tbl, tbl);
        rec << query << std::endl;
        BeginTransaction();
        auto plan = Optimize(query, tbl_map.find(tbl)->second, stmt_map.find(query[0])->second);

        std::ofstream ofs(tpcc_file_root + "json/" + names_use[i] + "/" + std::to_string(query_cnt) + ".json");
        ofs << std::setw(4) << plan->ToJson() << std::endl;
        ofs.close();
        EndTransaction(true);
        query_cnt++;
      }
      ifs_sql.close();
      ifs_tbl.close();
    }
    rec.close();
  }

}