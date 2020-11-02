#include "parser/expression/case_expression.h"

#include "binder/sql_node_visitor.h"
#include "common/hash_util.h"
#include "common/json.h"

namespace noisepage::parser {

nlohmann::json CaseExpression::WhenClause::ToJson() const {
  nlohmann::json j;
  j["condition"] = condition_->ToJson();
  j["then"] = then_->ToJson();
  return j;
}

std::vector<std::unique_ptr<AbstractExpression>> CaseExpression::WhenClause::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<AbstractExpression>> exprs;
  auto deserialized_cond = DeserializeExpression(j.at("condition"));
  condition_ = std::move(deserialized_cond.result_);
  exprs.insert(exprs.end(), std::make_move_iterator(deserialized_cond.non_owned_exprs_.begin()),
               std::make_move_iterator(deserialized_cond.non_owned_exprs_.end()));
  auto deserialized_then = DeserializeExpression(j.at("then"));
  then_ = std::move(deserialized_then.result_);
  exprs.insert(exprs.end(), std::make_move_iterator(deserialized_then.non_owned_exprs_.begin()),
               std::make_move_iterator(deserialized_then.non_owned_exprs_.end()));
  return exprs;
}

common::hash_t CaseExpression::WhenClause::Hash() const {
  common::hash_t hash = condition_->Hash();
  hash = common::HashUtil::CombineHashes(hash, condition_->Hash());
  hash = common::HashUtil::CombineHashes(hash, then_->Hash());
  return hash;
}

common::hash_t CaseExpression::Hash() const {
  common::hash_t hash = AbstractExpression::Hash();
  for (auto &clause : when_clauses_) {
    hash = common::HashUtil::CombineHashes(hash, clause.Hash());
  }
  if (default_expr_ != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, default_expr_->Hash());
  }
  return hash;
}

bool CaseExpression::operator==(const AbstractExpression &rhs) const {
  if (!AbstractExpression::operator==(rhs)) return false;
  auto const &other = dynamic_cast<const CaseExpression &>(rhs);
  auto clause_size = GetWhenClauseSize();
  if (clause_size != other.GetWhenClauseSize()) return false;

  for (size_t i = 0; i < clause_size; i++)
    if (when_clauses_[i] != other.when_clauses_[i]) return false;

  auto default_exp = GetDefaultClause();
  auto other_default_exp = other.GetDefaultClause();
  if (default_exp == nullptr && other_default_exp == nullptr) return true;
  if (default_exp == nullptr || other_default_exp == nullptr) return false;
  return (*default_exp == *other_default_exp);
}

std::unique_ptr<AbstractExpression> CaseExpression::Copy() const {
  std::vector<WhenClause> clauses;
  for (const auto &clause : when_clauses_) {
    clauses.emplace_back(WhenClause{clause.condition_->Copy(), clause.then_->Copy()});
  }
  auto expr = std::make_unique<CaseExpression>(GetReturnValueType(), std::move(clauses), default_expr_->Copy());
  expr->SetMutableStateForCopy(*this);
  return expr;
}

nlohmann::json CaseExpression::ToJson() const {
  nlohmann::json j = AbstractExpression::ToJson();
  std::vector<nlohmann::json> when_clauses_json;
  for (const auto &when_clause : when_clauses_) {
    when_clauses_json.push_back(when_clause.ToJson());
  }
  j["when_clauses"] = when_clauses_json;
  j["default_expr"] = default_expr_->ToJson();
  return j;
}

std::vector<std::unique_ptr<AbstractExpression>> CaseExpression::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<AbstractExpression>> exprs;
  auto e1 = AbstractExpression::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  when_clauses_ = j.at("when_clauses").get<std::vector<WhenClause>>();
  auto e2 = DeserializeExpression(j.at("default_expr"));
  default_expr_ = std::move(e2.result_);
  exprs.insert(exprs.end(), std::make_move_iterator(e2.non_owned_exprs_.begin()),
               std::make_move_iterator(e2.non_owned_exprs_.end()));
  return exprs;
}

void CaseExpression::Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) {
  v->Visit(common::ManagedPointer(this));
}

DEFINE_JSON_BODY_DECLARATIONS(CaseExpression::WhenClause);
DEFINE_JSON_BODY_DECLARATIONS(CaseExpression);

}  // namespace noisepage::parser
