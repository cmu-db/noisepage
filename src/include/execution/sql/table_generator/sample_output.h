#include <string>
#include <unordered_map>
#include <vector>

#include "planner/plannodes/output_schema.h"

namespace tpl::exec {
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
    terrier::catalog::col_oid_t col_oid{0};
    terrier::planner::OutputSchema::Column int_col{"dummy", terrier::type::TypeId::INTEGER, true, col_oid};
    // Create schemas with up to 10 integer columns.
    for (int i = 0; i < 10; i++) {
      std::vector<terrier::planner::OutputSchema::Column> cols;
      for (int j = 0; j < i + 1; j++) {
        cols.emplace_back(int_col);
      }
      schemas_.emplace("schema" + std::to_string(i + 1), terrier::planner::OutputSchema(cols));
    }
    InitTPCHOutput();
  }

  /**
   * @param name name of schema
   * @return the schema if it exists; an exception otherwise
   */
  const terrier::planner::OutputSchema *GetSchema(const std::string &name) { return &schemas_.at(name); }

 private:
  void InitTPCHOutput() {
    terrier::catalog::col_oid_t col_oid{0};
    terrier::planner::OutputSchema::Column int_col{"dummy", terrier::type::TypeId::INTEGER, true, col_oid};
    terrier::planner::OutputSchema::Column real_col{"dummy", terrier::type::TypeId::DECIMAL, true, col_oid};
    terrier::planner::OutputSchema::Column date_col{"dummy", terrier::type::TypeId::DATE, true, col_oid};
    terrier::planner::OutputSchema::Column string_col{"dummy", terrier::type::TypeId::VARCHAR, true, col_oid};
    // Q1 (two strings, 7 reals, 1 int)
    {
      std::vector<terrier::planner::OutputSchema::Column> cols{};
      for (u32 i = 0; i < u32(2); i++) {
        cols.emplace_back(string_col);
      }
      for (u32 i = 0; i < u32(7); i++) {
        cols.emplace_back(real_col);
      }
      cols.emplace_back(int_col);
      schemas_.emplace("tpch_q1", terrier::planner::OutputSchema(cols));
    }

    // TODO(Amadou): Fix the type of these others queries
    // Q6 (one Integer)
    {
      std::vector<terrier::planner::OutputSchema::Column> cols{int_col};
      schemas_.emplace("tpch_q2", terrier::planner::OutputSchema(cols));
    }

    // Q4 (two Integers)
    {
      std::vector<terrier::planner::OutputSchema::Column> cols{};
      for (u32 i = 0; i < u32(2); i++) {
        cols.emplace_back(int_col);
      }
      schemas_.emplace("tpch_q4", terrier::planner::OutputSchema(cols));
    }

    // Q5 (two Integers)
    {
      std::vector<terrier::planner::OutputSchema::Column> cols{};
      for (u32 i = 0; i < u32(2); i++) {
        cols.emplace_back(int_col);
      }
      schemas_.emplace("tpch_q5", terrier::planner::OutputSchema(cols));
    }
  }

  std::unordered_map<std::string, terrier::planner::OutputSchema> schemas_;
};
}  // namespace tpl::exec
