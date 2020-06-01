#include "network/postgres/statement.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "parser/postgresparser.h"
#include "traffic_cop/traffic_cop_util.h"
#include "type/type_id.h"

namespace terrier::network {

Statement::Statement(std::string &&name, std::string &&query_text, std::unique_ptr<parser::ParseResult> &&parse_result,
                     std::vector<type::TypeId> &&param_types)
    : name_(std::move(name)),
      query_text_(std::move(query_text)),
      query_hash_(common::HashUtil::Hash(query_text_)),
      parse_result_(std::move(parse_result)),
      param_types_(std::move(param_types)) {
  if (Valid()) {
    TERRIER_ASSERT(parse_result_->GetStatements().size() <= 1, "We currently expect one statement per string.");
    if (!Empty()) {
      root_statement_ = parse_result_->GetStatement(0);
      type_ = trafficcop::TrafficCopUtil::QueryTypeForStatement(root_statement_);
    }
  }
}
}  // namespace terrier::network
