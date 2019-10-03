#include <string>
#include <unordered_map>
#include <vector>

#include "planner/plannodes/output_schema.h"

namespace terrier::execution::exec {
/**
 * Used by the tpl executable to retrieve hard-coded test output schemas.
 */
class SampleOutput {
 public:
  /**
   * Constructor
   */
  SampleOutput() = default;

  /**
   * Initialize test output schemas
   */
  void InitTestOutput() {
    // Sample output formats
    catalog::col_oid_t col_oid{0};
    planner::OutputSchema::Column int_col{"dummy", type::TypeId::INTEGER, true, col_oid};
    planner::OutputSchema::Column real_col{"dummy", type::TypeId::DECIMAL, true, col_oid};
    planner::OutputSchema::Column date_col{"dummy", type::TypeId::DATE, true, col_oid};
    planner::OutputSchema::Column string_col{"dummy", type::TypeId::VARCHAR, true, col_oid};

    // Create schemas with up to 10 integer columns.
    for (uint32_t i = 0; i < 10; i++) {
      std::vector<planner::OutputSchema::Column> cols(i + 1, int_col);
      schemas_.emplace("schema" + std::to_string(i + 1), planner::OutputSchema(cols));
    }

    // Create a schema that has all types
    {
      std::vector<planner::OutputSchema::Column> cols;
      cols.emplace_back(int_col);
      cols.emplace_back(real_col);
      cols.emplace_back(date_col);
      cols.emplace_back(string_col);
      schemas_.emplace("all_types", planner::OutputSchema(cols));
    }

    InitTPCHOutput();
  }

  /**
   * @param name name of schema
   * @return the schema if it exists; an exception otherwise
   */
  const planner::OutputSchema *GetSchema(const std::string &name) { return &schemas_.at(name); }

 private:
  void InitTPCHOutput() {
    catalog::col_oid_t col_oid{0};
    planner::OutputSchema::Column int_col{"dummy", type::TypeId::INTEGER, true, col_oid};
    planner::OutputSchema::Column real_col{"dummy", type::TypeId::DECIMAL, true, col_oid};
    planner::OutputSchema::Column date_col{"dummy", type::TypeId::DATE, true, col_oid};
    planner::OutputSchema::Column string_col{"dummy", type::TypeId::VARCHAR, true, col_oid};
    // Q1 (two strings, 7 reals, 1 int)
    {
      std::vector<planner::OutputSchema::Column> cols{};
      for (uint32_t i = 0; i < uint32_t(2); i++) {
        cols.emplace_back(string_col);
      }
      for (uint32_t i = 0; i < uint32_t(7); i++) {
        cols.emplace_back(real_col);
      }
      cols.emplace_back(int_col);
      schemas_.emplace("tpch_q1", planner::OutputSchema(cols));
    }

    // Q6 (one real)
    {
      std::vector<planner::OutputSchema::Column> cols{real_col};
      schemas_.emplace("tpch_q6", planner::OutputSchema(cols));
    }
    // Q4 (one string, one int)
    {
      std::vector<planner::OutputSchema::Column> cols{string_col, int_col};
      schemas_.emplace("tpch_q4", planner::OutputSchema(cols));
    }

    // Q5 (one string, one real)
    {
      std::vector<planner::OutputSchema::Column> cols{string_col, real_col};
      schemas_.emplace("tpch_q5", planner::OutputSchema(cols));
    }
  }

  std::unordered_map<std::string, planner::OutputSchema> schemas_;
};
}  // namespace terrier::execution::exec
