#include "parser/select_statement.h"

#include <memory>
#include <utility>
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

std::vector<std::unique_ptr<AbstractExpression>> SelectStatement::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<AbstractExpression>> exprs;

  auto e1 = SQLStatement::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));

  // Deserialize select
  auto select_expressions = j.at("select").get<std::vector<nlohmann::json>>();
  for (const auto &expr : select_expressions) {
    auto deserialized = DeserializeExpression(expr);
    select_.emplace_back(common::ManagedPointer(deserialized.result_));
    exprs.emplace_back(std::move(deserialized.result_));
    exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                 std::make_move_iterator(deserialized.non_owned_exprs_.end()));
  }
  // Deserialize select distinct
  select_distinct_ = j.at("select_distinct").get<bool>();

  // Deserialize from
  if (!j.at("from").is_null()) {
    from_ = std::make_unique<TableRef>();
    auto e1 = from_->FromJson(j.at("from"));
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  }

  // Deserialize where
  if (!j.at("where").is_null()) {
    auto deserialized = DeserializeExpression(j.at("where"));
    where_ = common::ManagedPointer(deserialized.result_);
    exprs.emplace_back(std::move(deserialized.result_));
    exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                 std::make_move_iterator(deserialized.non_owned_exprs_.end()));
  }

  // Deserialize group by
  if (!j.at("group_by").is_null()) {
    group_by_ = std::make_unique<GroupByDescription>();
    auto e1 = group_by_->FromJson(j.at("group_by"));
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  }

  // Deserialize order by
  if (!j.at("order_by").is_null()) {
    order_by_ = std::make_unique<OrderByDescription>();
    auto e1 = order_by_->FromJson(j.at("order_by"));
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  }

  // Deserialize limit
  if (!j.at("limit").is_null()) {
    limit_ = std::make_unique<LimitDescription>();
    auto e1 = limit_->FromJson(j.at("limit"));
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  }

  // Deserialize select
  if (!j.at("union_select").is_null()) {
    union_select_ = std::make_unique<parser::SelectStatement>();
    auto e1 = union_select_->FromJson(j.at("union_select"));
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  }

  return exprs;
}

std::unique_ptr<SelectStatement> SelectStatement::Copy() {
  auto select = std::make_unique<SelectStatement>(
      select_, select_distinct_, from_->Copy(), where_, group_by_ == nullptr ? nullptr : group_by_->Copy(),
      order_by_ == nullptr ? nullptr : order_by_->Copy(), limit_ == nullptr ? nullptr : limit_->Copy(),
      with_table_ == nullptr ? nullptr : with_table_->Copy());
  if (union_select_ != nullptr) {
    auto union_copy = std::make_unique<SelectStatement>(
        union_select_->select_, union_select_->select_distinct_, union_select_->from_->Copy(), union_select_->where_,
        union_select_->group_by_ == nullptr ? nullptr : union_select_->group_by_->Copy(),
        union_select_->order_by_ == nullptr ? nullptr : union_select_->order_by_->Copy(),
        union_select_->limit_ == nullptr ? nullptr : union_select_->limit_->Copy(), union_select_->with_table_->Copy());
    select->SetUnionSelect(std::move(union_copy));
  }
  return select;
}

common::hash_t SelectStatement::Hash() const {
  common::hash_t hash = common::HashUtil::Hash(GetType());
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(select_distinct_));
  if (union_select_ != nullptr) hash = common::HashUtil::CombineHashes(hash, union_select_->Hash());
  if (limit_ != nullptr) hash = common::HashUtil::CombineHashes(hash, limit_->Hash());
  if (order_by_ != nullptr) hash = common::HashUtil::CombineHashes(hash, order_by_->Hash());
  if (group_by_ != nullptr) hash = common::HashUtil::CombineHashes(hash, group_by_->Hash());
  if (where_ != nullptr) hash = common::HashUtil::CombineHashes(hash, where_->Hash());
  if (from_ != nullptr) hash = common::HashUtil::CombineHashes(hash, from_->Hash());
  for (const auto &expr : select_) {
    hash = common::HashUtil::CombineHashes(hash, expr->Hash());
  }
  return hash;
}

bool SelectStatement::operator==(const SelectStatement &rhs) const {
  if (this->GetType() != rhs.GetType()) return false;
  if (select_.size() != rhs.select_.size()) return false;
  for (size_t i = 0; i < select_.size(); i++)
    if (*(select_[i]) != *(rhs.select_[i])) return false;
  if (select_distinct_ != rhs.select_distinct_) return false;

  if (from_ != nullptr && rhs.from_ == nullptr) return false;
  if (from_ == nullptr && rhs.from_ != nullptr) return false;
  if (from_ != nullptr && rhs.from_ != nullptr && *(from_) != *(rhs.from_)) return false;

  if (where_ != nullptr && rhs.where_ == nullptr) return false;
  if (where_ == nullptr && rhs.where_ != nullptr) return false;
  if (where_ != nullptr && rhs.where_ != nullptr && *(where_) != *(rhs.where_)) return false;

  if (group_by_ != nullptr && rhs.group_by_ == nullptr) return false;
  if (group_by_ == nullptr && rhs.group_by_ != nullptr) return false;
  if (group_by_ != nullptr && rhs.group_by_ != nullptr && *(group_by_) != *(rhs.group_by_)) return false;

  if (order_by_ != nullptr && rhs.order_by_ == nullptr) return false;
  if (order_by_ == nullptr && rhs.order_by_ != nullptr) return false;
  if (order_by_ != nullptr && rhs.order_by_ != nullptr && *(order_by_) != *(rhs.order_by_)) return false;

  if (limit_ != nullptr && rhs.limit_ == nullptr) return false;
  if (limit_ == nullptr && rhs.limit_ != nullptr) return false;
  if (limit_ != nullptr && rhs.limit_ != nullptr && *(limit_) != *(rhs.limit_)) return false;

  if (union_select_ != nullptr && rhs.union_select_ == nullptr) return false;
  if (union_select_ == nullptr && rhs.union_select_ != nullptr) return false;
  if (union_select_ == nullptr && rhs.union_select_ == nullptr) return true;
  return *(union_select_) == *(rhs.union_select_);
}

}  // namespace terrier::parser
