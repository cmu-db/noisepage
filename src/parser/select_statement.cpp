#include "parser/select_statement.h"
#include <memory>
#include <vector>

namespace terrier::parser {

nlohmann::json SelectStatement::ToJson() const {
  nlohmann::json j = SQLStatement::ToJson();
  j["select"] = select_;
  j["select_distinct"] = select_distinct_;
  j["from"] = from_;
  j["where"] = where_;
  j["group_by"] = group_by_;
  j["order_by"] = order_by_;
  j["limit"] = limit_;
  j["union_select"] = union_select_;
  return j;
}

void SelectStatement::FromJson(const nlohmann::json &j) {
  SQLStatement::FromJson(j);

  // Deserialize select
  auto select_expressions = j.at("select").get<std::vector<nlohmann::json>>();
  for (const auto &expr : select_expressions) {
    select_.emplace_back(DeserializeExpression(expr));
  }
  // Deserialize select distinct
  select_distinct_ = j.at("select_distinct").get<bool>();

  // Deserialize from
  if (!j.at("from").is_null()) {
    from_ = std::make_shared<TableRef>();
    from_->FromJson(j.at("from"));
  }

  // Deserialize where
  if (!j.at("where").is_null()) {
    where_ = DeserializeExpression(j.at("where"));
  }

  // Deserialize group by
  if (!j.at("group_by").is_null()) {
    group_by_ = std::make_shared<GroupByDescription>();
    group_by_->FromJson(j.at("group_by"));
  }

  // Deserialize order by
  if (!j.at("order_by").is_null()) {
    order_by_ = std::make_shared<OrderByDescription>();
    order_by_->FromJson(j.at("order_by"));
  }

  // Deserialize limit
  if (!j.at("limit").is_null()) {
    limit_ = std::make_shared<LimitDescription>();
    limit_->FromJson(j.at("limit"));
  }

  // Deserialize select
  if (!j.at("union_select").is_null()) {
    union_select_ = std::make_shared<parser::SelectStatement>();
    union_select_->FromJson(j.at("union_select"));
  }
}

}  // namespace terrier::parser
