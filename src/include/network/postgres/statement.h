#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
#include "network/postgres/statement.h"
#include "parser/postgresparser.h"
#include "traffic_cop/traffic_cop_util.h"
#include "type/type_id.h"

namespace terrier::network {

/**
 * Statement is a postgres concept (see the Extended Query documentation:
 * https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)
 * It encapsulates a parsed statement, the parameter types (if any). It represents a statement ready to be turned into a
 * Portal with a Bind message.
 */
class Statement {
 public:
  /**
   * Constructor that doesn't have parameter types, i.e. Simple Query protocol
   * @param parse_result unbound output from postgresparser
   */
  explicit Statement(std::unique_ptr<parser::ParseResult> &&parse_result) : Statement(std::move(parse_result), {}) {}

  /**
   * Constructor that does have parameter types, i.e. Extended Query protocol
   * @param parse_result unbound output from postgresparser
   * @param param_types types of the values to be bound
   */
  Statement(std::unique_ptr<parser::ParseResult> &&parse_result, std::vector<type::TypeId> &&param_types)
      : parse_result_(std::move(parse_result)), param_types_(std::move(param_types)) {
    if (Valid()) {
      TERRIER_ASSERT(parse_result_->GetStatements().size() <= 1, "We currently expect one statement per string.");
      if (!Empty()) {
        root_statement_ = parse_result_->GetStatement(0);
        type_ = trafficcop::TrafficCopUtil::QueryTypeForStatement(root_statement_);
      }
    }
  }

  /**
   * @return true if parser succeeded and this statement is usable
   */
  bool Valid() const { return parse_result_ != nullptr; }

  /**
   * @return true if the statement is empty
   */
  bool Empty() const {
    TERRIER_ASSERT(Valid(), "Attempting to check emptiness without a valid parsed result.");
    return parse_result_->Empty();
  }

  /**
   * @return managed pointer to the output of the parser for this statement
   */
  common::ManagedPointer<parser::ParseResult> ParseResult() const {
    TERRIER_ASSERT(Valid(), "Attempting to get parse results without a valid parsed result.");
    return common::ManagedPointer(parse_result_);
  }

  /**
   * @return managed pointer to the  root statement of the ParseResult. Just shorthand for ParseResult->GetStatement(0)
   */
  common::ManagedPointer<parser::SQLStatement> RootStatement() const {
    TERRIER_ASSERT(Valid(), "Attempting to get root statement without a valid parsed result.");
    return common::ManagedPointer(root_statement_);
  }

  /**
   * @return vector of the statements parameters (if any)
   */
  const std::vector<type::TypeId> &ParamTypes() const { return param_types_; }

  /**
   * @return QueryType of the root statement of the ParseResult
   */
  QueryType GetQueryType() const { return type_; }

 private:
  const std::unique_ptr<parser::ParseResult> parse_result_ = nullptr;
  const std::vector<type::TypeId> param_types_;
  common::ManagedPointer<parser::SQLStatement> root_statement_ = nullptr;
  enum QueryType type_ = QueryType::QUERY_INVALID;
};

}  // namespace terrier::network
