#include "parser/select_statement.h"
#include <memory>
#include <vector>

namespace terrier::parser {

nlohmann::json SelectStatement::ToJson() const {
  nlohmann::json j = SQLStatement::ToJson();
  std::vector<nlohmann::json> select_json;
  select_json.reserve(select_.size());
  for (const auto &expr : select_) {
    select_json.emplace_back(expr->ToJson());
  }
  j["select"] = select_json;
  j["select_distinct"] = select_distinct_;
  j["from"] = from_ == nullptr ? nlohmann::json(nullptr) : from_->ToJson();
  j["where"] = where_ == nullptr ? nlohmann::json(nullptr) : where_->ToJson();
  j["group_by"] = group_by_ == nullptr ? nlohmann::json(nullptr) : group_by_->ToJson();
  j["order_by"] = order_by_ == nullptr ? nlohmann::json(nullptr) : order_by_->ToJson();
  j["limit"] = limit_ == nullptr ? nlohmann::json(nullptr) : limit_->ToJson();
  j["union_select"] = union_select_ == nullptr ? nlohmann::json(nullptr) : union_select_->ToJson();
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
    from_ = std::make_unique<TableRef>();
    from_->FromJson(j.at("from"));
  }

  // Deserialize where
  if (!j.at("where").is_null()) {
    where_ = DeserializeExpression(j.at("where"));
  }

  // Deserialize group by
  if (!j.at("group_by").is_null()) {
    group_by_ = std::make_unique<GroupByDescription>();
    group_by_->FromJson(j.at("group_by"));
  }

  // Deserialize order by
  if (!j.at("order_by").is_null()) {
    order_by_ = std::make_unique<OrderByDescription>();
    order_by_->FromJson(j.at("order_by"));
  }

  // Deserialize limit
  if (!j.at("limit").is_null()) {
    limit_ = std::make_unique<LimitDescription>();
    limit_->FromJson(j.at("limit"));
  }

  // Deserialize select
  if (!j.at("union_select").is_null()) {
    union_select_ = std::make_unique<parser::SelectStatement>();
    union_select_->FromJson(j.at("union_select"));
  }
}

std::unique_ptr<SelectStatement> SelectStatement::Copy() {
  auto select = std::make_unique<SelectStatement>(select_, select_distinct_, from_->Copy(), where_,
                                                  group_by_ == nullptr ? nullptr : group_by_->Copy(),
                                                  order_by_ == nullptr ? nullptr : order_by_->Copy(),
                                                  limit_ == nullptr ? nullptr : limit_->Copy());
  select->SetUnionSelect(union_select_->Copy());
  return select;
}

}  // namespace terrier::parser
