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
    from_ = common::ManagedPointer<TableRef>();
    from_->FromJson(j.at("from"));
  }

  // Deserialize where
  if (!j.at("where").is_null()) {
    // TODO(WAN): lifetime
    where_ = common::ManagedPointer(DeserializeExpression(j.at("where")));
  }

  // Deserialize group by
  if (!j.at("group_by").is_null()) {
    // TODO(WAN): lifetime
    group_by_ = common::ManagedPointer(new GroupByDescription());
    group_by_->FromJson(j.at("group_by"));
  }

  // Deserialize order by
  if (!j.at("order_by").is_null()) {
    // TODO(WAN): lifetime
    order_by_ = common::ManagedPointer(new OrderByDescription());
    order_by_->FromJson(j.at("order_by"));
  }

  // Deserialize limit
  if (!j.at("limit").is_null()) {
    // TODO(WAN): lifetime
    limit_ = common::ManagedPointer(new LimitDescription());
    limit_->FromJson(j.at("limit"));
  }

  // Deserialize select
  if (!j.at("union_select").is_null()) {
    // TODO(WAN): lifetime
    union_select_ = common::ManagedPointer(new SelectStatement());
    union_select_->FromJson(j.at("union_select"));
  }
}

}  // namespace terrier::parser
