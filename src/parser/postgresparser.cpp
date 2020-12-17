#include "parser/postgresparser.h"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/error/exception.h"
#include "execution/sql/value_util.h"
#include "libpg_query/pg_list.h"
#include "libpg_query/pg_query.h"
#include "loggers/parser_logger.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/case_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/default_value_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/parameter_value_expression.h"
#include "parser/expression/star_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/expression/table_star_expression.h"
#include "parser/expression/type_cast_expression.h"
#include "parser/pg_trigger.h"
#include "parser/statements.h"

/**
 * Log information about the error, then throw an exception
 * FN_NAME - name of current function
 * TYPE_MSG - message about the type in error
 * ARG - to print, i.e. unknown or unsupported type
 */
#define PARSER_LOG_AND_THROW(FN_NAME, TYPE_MSG, ARG)           \
  PARSER_LOG_DEBUG(#FN_NAME #TYPE_MSG " {} unsupported", ARG); \
  throw PARSER_EXCEPTION(#FN_NAME ":" #TYPE_MSG " unsupported")

namespace noisepage::parser {

std::unique_ptr<parser::ParseResult> PostgresParser::BuildParseTree(const std::string &query_string) {
  auto text = query_string.c_str();
  auto ctx = pg_query_parse_init();
  auto result = pg_query_parse(text);

  // Parse the query string with the Postgres parser.
  if (result.error != nullptr) {
    PARSER_LOG_DEBUG("BuildParseTree error: msg {}, curpos {}", result.error->message, result.error->cursorpos);

    ParserException exception(std::string(result.error->message), __FILE__, __LINE__, result.error->cursorpos);

    pg_query_parse_finish(ctx);
    pg_query_free_parse_result(result);
    throw exception;
  }

  // Transform the Postgres parse tree to a Terrier representation.
  auto parse_result = std::make_unique<ParseResult>();
  try {
    ListTransform(parse_result.get(), result.tree);
  } catch (const Exception &e) {
    pg_query_parse_finish(ctx);
    pg_query_free_parse_result(result);
    PARSER_LOG_DEBUG("BuildParseTree: caught {} {} {} {}", e.GetType(), e.GetFile(), e.GetLine(), e.what());
    throw;
  }

  pg_query_parse_finish(ctx);
  pg_query_free_parse_result(result);
  return parse_result;
}

void PostgresParser::ListTransform(ParseResult *parse_result, List *root) {
  if (root != nullptr) {
    for (auto cell = root->head; cell != nullptr; cell = cell->next) {
      auto node = static_cast<Node *>(cell->data.ptr_value);
      parse_result->AddStatement(NodeTransform(parse_result, node));
    }
  }
}

std::unique_ptr<SQLStatement> PostgresParser::NodeTransform(ParseResult *parse_result, Node *node) {
  // TODO(WAN): Document what input is parsed to nullptr
  if (node == nullptr) {
    return nullptr;
  }

  std::unique_ptr<SQLStatement> result;
  switch (node->type) {
    case T_CopyStmt: {
      result = CopyTransform(parse_result, reinterpret_cast<CopyStmt *>(node));
      break;
    }
    case T_CreateStmt: {
      result = CreateTransform(parse_result, reinterpret_cast<CreateStmt *>(node));
      break;
    }
    case T_CreatedbStmt: {
      result = CreateDatabaseTransform(parse_result, reinterpret_cast<CreateDatabaseStmt *>(node));
      break;
    }
    case T_CreateFunctionStmt: {
      result = CreateFunctionTransform(parse_result, reinterpret_cast<CreateFunctionStmt *>(node));
      break;
    }
    case T_CreateSchemaStmt: {
      result = CreateSchemaTransform(parse_result, reinterpret_cast<CreateSchemaStmt *>(node));
      break;
    }
    case T_CreateTrigStmt: {
      result = CreateTriggerTransform(parse_result, reinterpret_cast<CreateTrigStmt *>(node));
      break;
    }
    case T_DropdbStmt: {
      result = DropDatabaseTransform(parse_result, reinterpret_cast<DropDatabaseStmt *>(node));
      break;
    }
    case T_DropStmt: {
      result = DropTransform(parse_result, reinterpret_cast<DropStmt *>(node));
      break;
    }
    case T_ExecuteStmt: {
      result = ExecuteTransform(parse_result, reinterpret_cast<ExecuteStmt *>(node));
      break;
    }
    case T_ExplainStmt: {
      result = ExplainTransform(parse_result, reinterpret_cast<ExplainStmt *>(node));
      break;
    }
    case T_IndexStmt: {
      result = CreateIndexTransform(parse_result, reinterpret_cast<IndexStmt *>(node));
      break;
    }
    case T_InsertStmt: {
      result = InsertTransform(parse_result, reinterpret_cast<InsertStmt *>(node));
      break;
    }
    case T_PrepareStmt: {
      result = PrepareTransform(parse_result, reinterpret_cast<PrepareStmt *>(node));
      break;
    }
    case T_SelectStmt: {
      result = SelectTransform(parse_result, reinterpret_cast<SelectStmt *>(node));
      break;
    }
    case T_VacuumStmt: {
      result = VacuumTransform(parse_result, reinterpret_cast<VacuumStmt *>(node));
      break;
    }
    case T_VariableSetStmt: {
      result = VariableSetTransform(parse_result, reinterpret_cast<VariableSetStmt *>(node));
      break;
    }
    case T_VariableShowStmt: {
      result = VariableShowTransform(parse_result, reinterpret_cast<VariableShowStmt *>(node));
      break;
    }
    case T_ViewStmt: {
      result = CreateViewTransform(parse_result, reinterpret_cast<ViewStmt *>(node));
      break;
    }
    case T_TruncateStmt: {
      result = TruncateTransform(parse_result, reinterpret_cast<TruncateStmt *>(node));
      break;
    }
    case T_TransactionStmt: {
      result = TransactionTransform(reinterpret_cast<TransactionStmt *>(node));
      break;
    }
    case T_UpdateStmt: {
      result = UpdateTransform(parse_result, reinterpret_cast<UpdateStmt *>(node));
      break;
    }
    case T_DeleteStmt: {
      result = DeleteTransform(parse_result, reinterpret_cast<DeleteStmt *>(node));
      break;
    }
    default: {
      PARSER_LOG_DEBUG("NodeTransform: statement type {} unsupported", node->type);
      throw PARSER_EXCEPTION("NodeTransform: unsupported statement type");
    }
  }
  return result;
}

std::unique_ptr<AbstractExpression> PostgresParser::ExprTransform(ParseResult *parse_result, Node *node, char *alias) {
  if (node == nullptr) {
    return nullptr;
  }

  std::unique_ptr<AbstractExpression> expr;
  switch (node->type) {
    case T_A_Const: {
      expr = ConstTransform(parse_result, reinterpret_cast<A_Const *>(node));
      break;
    }
    case T_A_Expr: {
      expr = AExprTransform(parse_result, reinterpret_cast<A_Expr *>(node));
      break;
    }
    case T_BoolExpr: {
      expr = BoolExprTransform(parse_result, reinterpret_cast<BoolExpr *>(node));
      break;
    }
    case T_CaseExpr: {
      expr = CaseExprTransform(parse_result, reinterpret_cast<CaseExpr *>(node));
      break;
    }
    case T_ColumnRef: {
      expr = ColumnRefTransform(parse_result, reinterpret_cast<ColumnRef *>(node), alias);
      break;
    }
    case T_FuncCall: {
      expr = FuncCallTransform(parse_result, reinterpret_cast<FuncCall *>(node));
      break;
    }
    case T_NullTest: {
      expr = NullTestTransform(parse_result, reinterpret_cast<NullTest *>(node));
      break;
    }
    case T_ParamRef: {
      expr = ParamRefTransform(parse_result, reinterpret_cast<ParamRef *>(node));
      break;
    }
    case T_SubLink: {
      expr = SubqueryExprTransform(parse_result, reinterpret_cast<SubLink *>(node));
      break;
    }
    case T_TypeCast: {
      expr = AExprTransform(parse_result, reinterpret_cast<A_Expr *>(node));
      break;
    }
    default: {
      PARSER_LOG_DEBUG("ExprTransform: type {} unsupported", node->type);
      throw PARSER_EXCEPTION("ExprTransform: unsupported type");
    }
  }
  if (alias != nullptr) {
    expr->SetAlias(alias);
  }
  return expr;
}

/**
 * DO NOT USE THIS UNLESS YOU MUST.
 * Converts the Postgres parser's expression into our own expression type.
 * @param parser_str string representation returned by postgres parser
 * @return expression type corresponding to the string
 */
ExpressionType PostgresParser::StringToExpressionType(const std::string &parser_str) {
  std::string str = parser_str;
  std::transform(str.begin(), str.end(), str.begin(), ::toupper);
  if (str == "OPERATOR_UNARY_MINUS") {
    return ExpressionType::OPERATOR_UNARY_MINUS;
  }
  if (str == "OPERATOR_PLUS" || str == "+") {
    return ExpressionType::OPERATOR_PLUS;
  }
  if (str == "OPERATOR_MINUS" || str == "-") {
    return ExpressionType::OPERATOR_MINUS;
  }
  if (str == "OPERATOR_MULTIPLY" || str == "*") {
    return ExpressionType::OPERATOR_MULTIPLY;
  }
  if (str == "OPERATOR_DIVIDE" || str == "/") {
    return ExpressionType::OPERATOR_DIVIDE;
  }
  if (str == "OPERATOR_CONCAT" || str == "||") {
    return ExpressionType::OPERATOR_CONCAT;
  }
  if (str == "OPERATOR_MOD" || str == "%") {
    return ExpressionType::OPERATOR_MOD;
  }
  if (str == "OPERATOR_NOT") {
    return ExpressionType::OPERATOR_NOT;
  }
  if (str == "OPERATOR_IS_NULL") {
    return ExpressionType::OPERATOR_IS_NULL;
  }
  if (str == "OPERATOR_EXISTS") {
    return ExpressionType::OPERATOR_EXISTS;
  }
  if (str == "COMPARE_EQUAL" || str == "=") {
    return ExpressionType::COMPARE_EQUAL;
  }
  if (str == "COMPARE_NOTEQUAL" || str == "!=" || str == "<>") {
    return ExpressionType::COMPARE_NOT_EQUAL;
  }
  if (str == "COMPARE_LESSTHAN" || str == "<") {
    return ExpressionType::COMPARE_LESS_THAN;
  }
  if (str == "COMPARE_GREATERTHAN" || str == ">") {
    return ExpressionType::COMPARE_GREATER_THAN;
  }
  if (str == "COMPARE_LESSTHANOREQUALTO" || str == "<=") {
    return ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO;
  }
  if (str == "COMPARE_GREATERTHANOREQUALTO" || str == ">=") {
    return ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO;
  }
  if (str == "COMPARE_LIKE" || str == "~~") {
    return ExpressionType::COMPARE_LIKE;
  }
  if (str == "COMPARE_NOTLIKE" || str == "!~~") {
    return ExpressionType::COMPARE_NOT_LIKE;
  }
  if (str == "COMPARE_IN") {
    return ExpressionType::COMPARE_IN;
  }
  if (str == "COMPARE_DISTINCT_FROM") {
    return ExpressionType::COMPARE_IS_DISTINCT_FROM;
  }
  if (str == "CONJUNCTION_AND") {
    return ExpressionType::CONJUNCTION_AND;
  }
  if (str == "CONJUNCTION_OR") {
    return ExpressionType::CONJUNCTION_OR;
  }
  if (str == "COLUMN_VALUE") {
    return ExpressionType::COLUMN_VALUE;
  }
  if (str == "VALUE_CONSTANT") {
    return ExpressionType::VALUE_CONSTANT;
  }
  if (str == "VALUE_PARAMETER") {
    return ExpressionType::VALUE_PARAMETER;
  }
  if (str == "VALUE_TUPLE") {
    return ExpressionType::VALUE_TUPLE;
  }
  if (str == "VALUE_TUPLE_ADDRESS") {
    return ExpressionType::VALUE_TUPLE_ADDRESS;
  }
  if (str == "VALUE_NULL") {
    return ExpressionType::VALUE_NULL;
  }
  if (str == "VALUE_VECTOR") {
    return ExpressionType::VALUE_VECTOR;
  }
  if (str == "VALUE_SCALAR") {
    return ExpressionType::VALUE_SCALAR;
  }
  if (str == "AGGREGATE_COUNT") {
    return ExpressionType::AGGREGATE_COUNT;
  }
  if (str == "AGGREGATE_SUM") {
    return ExpressionType::AGGREGATE_SUM;
  }
  if (str == "AGGREGATE_MIN") {
    return ExpressionType::AGGREGATE_MIN;
  }
  if (str == "AGGREGATE_MAX") {
    return ExpressionType::AGGREGATE_MAX;
  }
  if (str == "AGGREGATE_AVG") {
    return ExpressionType::AGGREGATE_AVG;
  }
  if (str == "FUNCTION") {
    return ExpressionType::FUNCTION;
  }
  if (str == "HASH_RANGE") {
    return ExpressionType::HASH_RANGE;
  }
  if (str == "OPERATOR_CASE_EXPR") {
    return ExpressionType::OPERATOR_CASE_EXPR;
  }
  if (str == "OPERATOR_NULLIF") {
    return ExpressionType::OPERATOR_NULL_IF;
  }
  if (str == "OPERATOR_COALESCE") {
    return ExpressionType::OPERATOR_COALESCE;
  }
  if (str == "ROW_SUBQUERY") {
    return ExpressionType::ROW_SUBQUERY;
  }
  if (str == "STAR") {
    return ExpressionType::STAR;
  }
  if (str == "TABLE_STAR") {
    return ExpressionType::TABLE_STAR;
  }
  if (str == "PLACEHOLDER") {
    return ExpressionType::PLACEHOLDER;
  }
  if (str == "COLUMN_REF") {
    return ExpressionType::COLUMN_REF;
  }
  if (str == "FUNCTION_REF") {
    return ExpressionType::FUNCTION_REF;
  }
  if (str == "TABLE_REF") {
    return ExpressionType::TABLE_REF;
  }

  PARSER_LOG_DEBUG("StringToExpressionType: type {} unsupported", str.c_str());
  throw PARSER_EXCEPTION("StringToExpressionType: unsupported type");
}

// Postgres.A_Expr -> noisepage.AbstractExpression
std::unique_ptr<AbstractExpression> PostgresParser::AExprTransform(ParseResult *parse_result, A_Expr *root) {
  // TODO(WAN): The original code wanted a function for transforming strings of operations to the relevant expression
  //  type, e.g. > to COMPARE_GREATERTHAN.
  if (root == nullptr) {
    return nullptr;
  }

  ExpressionType target_type;
  std::vector<std::unique_ptr<AbstractExpression>> children;

  if (root->kind_ == AEXPR_DISTINCT) {
    target_type = ExpressionType::COMPARE_IS_DISTINCT_FROM;
    children.emplace_back(ExprTransform(parse_result, root->lexpr_, nullptr));
    children.emplace_back(ExprTransform(parse_result, root->rexpr_, nullptr));
  } else if (root->kind_ == AEXPR_OP && root->type_ == T_TypeCast) {
    target_type = ExpressionType::OPERATOR_CAST;
  } else if (root->kind_ == AEXPR_IN) {
    // Expression "FOO in (X, Y, ..., Z)". By convention, FOO is the first child and the other children are the IN list.
    target_type = ExpressionType::COMPARE_IN;
    children.emplace_back(ExprTransform(parse_result, root->lexpr_, nullptr));
    auto *in_list = reinterpret_cast<List *>(root->rexpr_);
    for (auto cell = in_list->head; cell != nullptr; cell = cell->next) {
      auto node = static_cast<Node *>(cell->data.ptr_value);
      children.emplace_back(ExprTransform(parse_result, node, nullptr));
    }

    auto name = (reinterpret_cast<value *>(root->name_->head->data.ptr_value))->val_.str_;
    // Postgres distinguishes between IN "=" and NOT IN "<>" by the name of the expression. Rewrite NOT IN.
    if (std::strcmp(name, "<>") == 0) {
      auto in_expr = std::make_unique<ComparisonExpression>(target_type, std::move(children));
      std::vector<std::unique_ptr<AbstractExpression>> in_child;
      in_child.emplace_back(std::move(in_expr));
      return std::make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT, type::TypeId::INVALID,
                                                  std::move(in_child));
    }
  } else {
    auto name = (reinterpret_cast<value *>(root->name_->head->data.ptr_value))->val_.str_;
    target_type = StringToExpressionType(name);
    children.emplace_back(ExprTransform(parse_result, root->lexpr_, nullptr));
    children.emplace_back(ExprTransform(parse_result, root->rexpr_, nullptr));
  }

  switch (target_type) {
    case ExpressionType::OPERATOR_UNARY_MINUS:
    case ExpressionType::OPERATOR_PLUS:
    case ExpressionType::OPERATOR_MINUS:
    case ExpressionType::OPERATOR_MULTIPLY:
    case ExpressionType::OPERATOR_DIVIDE:
    case ExpressionType::OPERATOR_CONCAT:
    case ExpressionType::OPERATOR_MOD:
    case ExpressionType::OPERATOR_NOT:
    case ExpressionType::OPERATOR_IS_NULL:
    case ExpressionType::OPERATOR_IS_NOT_NULL:
    case ExpressionType::OPERATOR_EXISTS: {
      return std::make_unique<OperatorExpression>(target_type, type::TypeId::INVALID, std::move(children));
    }
    case ExpressionType::OPERATOR_CAST: {
      return TypeCastTransform(parse_result, reinterpret_cast<TypeCast *>(root));
    }
    case ExpressionType::COMPARE_EQUAL:
    case ExpressionType::COMPARE_NOT_EQUAL:
    case ExpressionType::COMPARE_LESS_THAN:
    case ExpressionType::COMPARE_GREATER_THAN:
    case ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
    case ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
    case ExpressionType::COMPARE_LIKE:
    case ExpressionType::COMPARE_NOT_LIKE:
    case ExpressionType::COMPARE_IN:
    case ExpressionType::COMPARE_IS_DISTINCT_FROM: {
      return std::make_unique<ComparisonExpression>(target_type, std::move(children));
    }
    default: {
      PARSER_LOG_DEBUG("AExprTransform: type {} unsupported", static_cast<int>(target_type));
      throw PARSER_EXCEPTION("AExprTransform: unsupported type");
    }
  }
}

// Postgres.BoolExpr -> noisepage.ConjunctionExpression
std::unique_ptr<AbstractExpression> PostgresParser::BoolExprTransform(ParseResult *parse_result, BoolExpr *root) {
  std::unique_ptr<AbstractExpression> result;
  std::vector<std::unique_ptr<AbstractExpression>> children;
  for (auto cell = root->args_->head; cell != nullptr; cell = cell->next) {
    auto node = reinterpret_cast<Node *>(cell->data.ptr_value);
    children.emplace_back(ExprTransform(parse_result, node, nullptr));
  }
  switch (root->boolop_) {
    case AND_EXPR: {
      result = std::make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(children));
      break;
    }
    case OR_EXPR: {
      result = std::make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(children));
      break;
    }
    case NOT_EXPR: {
      result = std::make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT, type::TypeId::INVALID,
                                                    std::move(children));
      break;
    }
    default: {
      PARSER_LOG_DEBUG("BoolExprTransform: type {} unsupported", root->boolop_);
      throw PARSER_EXCEPTION("BoolExprTransform: unsupported type");
    }
  }

  return result;
}

std::unique_ptr<AbstractExpression> PostgresParser::CaseExprTransform(ParseResult *parse_result, CaseExpr *root) {
  if (root == nullptr) {
    return nullptr;
  }

  auto arg_expr = ExprTransform(parse_result, reinterpret_cast<Node *>(root->arg_), nullptr);

  std::vector<CaseExpression::WhenClause> clauses;
  for (auto cell = root->args_->head; cell != nullptr; cell = cell->next) {
    auto w = reinterpret_cast<CaseWhen *>(cell->data.ptr_value);
    auto when_expr = ExprTransform(parse_result, reinterpret_cast<Node *>(w->expr_), nullptr);
    auto result_expr = ExprTransform(parse_result, reinterpret_cast<Node *>(w->result_), nullptr);

    if (arg_expr == nullptr) {
      auto when_clause = CaseExpression::WhenClause{std::move(when_expr), std::move(result_expr)};
      clauses.emplace_back(std::move(when_clause));
    } else {
      std::vector<std::unique_ptr<AbstractExpression>> children;
      children.emplace_back(arg_expr->Copy());
      children.emplace_back(std::move(when_expr));
      auto cmp_expr = std::make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(children));
      auto when_clause = CaseExpression::WhenClause{std::move(cmp_expr), std::move(result_expr)};
      clauses.emplace_back(std::move(when_clause));
    }
  }

  auto default_expr = ExprTransform(parse_result, reinterpret_cast<Node *>(root->defresult_), nullptr);
  auto ret_val_type = clauses[0].then_->GetReturnValueType();
  return std::make_unique<CaseExpression>(ret_val_type, std::move(clauses), std::move(default_expr));
}

// Postgres.ColumnRef -> noisepage.ColumnValueExpression | noisepage.TableStarExpression
std::unique_ptr<AbstractExpression> PostgresParser::ColumnRefTransform(ParseResult *parse_result, ColumnRef *root,
                                                                       char *alias) {
  std::unique_ptr<AbstractExpression> result;
  List *fields = root->fields_;
  auto node = reinterpret_cast<Node *>(fields->head->data.ptr_value);
  switch (node->type) {
    case T_String: {
      // TODO(WAN): verify the old system is doing the right thing
      std::string col_name;
      std::string table_name;
      bool all_columns = false;
      if (fields->length == 1) {
        col_name = reinterpret_cast<value *>(node)->val_.str_;
        table_name = "";
      } else {
        auto next_node = reinterpret_cast<Node *>(fields->head->next->data.ptr_value);
        if (next_node->type == T_A_Star) {
          all_columns = true;
        } else {
          col_name = reinterpret_cast<value *>(next_node)->val_.str_;
        }

        table_name = reinterpret_cast<value *>(node)->val_.str_;
      }

      if (all_columns)
        result = std::make_unique<TableStarExpression>(table_name);
      else if (alias != nullptr)
        result = std::make_unique<ColumnValueExpression>(table_name, col_name, std::string(alias));
      else
        result = std::make_unique<ColumnValueExpression>(table_name, col_name);
      break;
    }
    case T_A_Star: {
      result = std::make_unique<TableStarExpression>();
      break;
    }
    default: {
      PARSER_LOG_DEBUG("ColumnRefTransform: type {} unsupported", node->type);
      throw PARSER_EXCEPTION("ColumnRefTransform: unsupported type");
    }
  }

  return result;
}

// Postgres.A_Const -> noisepage.ConstantValueExpression
std::unique_ptr<AbstractExpression> PostgresParser::ConstTransform(ParseResult *parse_result, A_Const *root) {
  if (root == nullptr) {
    return nullptr;
  }
  return ValueTransform(parse_result, root->val_);
}

// Postgres.FuncCall -> noisepage.AbstractExpression
std::unique_ptr<AbstractExpression> PostgresParser::FuncCallTransform(ParseResult *parse_result, FuncCall *root) {
  // TODO(WAN): Check if we need to change the case of this.
  std::string func_name = reinterpret_cast<value *>(root->funcname_->head->data.ptr_value)->val_.str_;

  std::unique_ptr<AbstractExpression> result;
  if (!IsAggregateFunction(func_name)) {
    // normal functions (built-in functions or UDFs)
    func_name = (reinterpret_cast<value *>(root->funcname_->tail->data.ptr_value))->val_.str_;
    std::vector<std::unique_ptr<AbstractExpression>> children;

    if (root->args_ != nullptr) {
      for (auto cell = root->args_->head; cell != nullptr; cell = cell->next) {
        auto expr_node = reinterpret_cast<Node *>(cell->data.ptr_value);
        children.emplace_back(ExprTransform(parse_result, expr_node, nullptr));
      }
    }
    result = std::make_unique<FunctionExpression>(std::move(func_name), type::TypeId::INVALID, std::move(children));
  } else {
    // aggregate function
    auto agg_fun_type = StringToExpressionType("AGGREGATE_" + func_name);
    std::vector<std::unique_ptr<AbstractExpression>> children;
    if (root->agg_star_) {
      auto child = new StarExpression();
      children.emplace_back(child);
      result = std::make_unique<AggregateExpression>(agg_fun_type, std::move(children), root->agg_distinct_);
    } else if (root->args_->length < 2) {
      auto expr_node = reinterpret_cast<Node *>(root->args_->head->data.ptr_value);
      auto child = ExprTransform(parse_result, expr_node, nullptr);
      children.emplace_back(std::move(child));
      result = std::make_unique<AggregateExpression>(agg_fun_type, std::move(children), root->agg_distinct_);
    } else {
      PARSER_LOG_DEBUG("FuncCallTransform: Aggregation over multiple cols not supported");
      throw PARSER_EXCEPTION("FuncCallTransform: Aggregation over multiple cols not supported");
    }
  }
  return result;
}

// Postgres.NullTest -> noisepage.OperatorExpression
std::unique_ptr<AbstractExpression> PostgresParser::NullTestTransform(ParseResult *parse_result, NullTest *root) {
  if (root == nullptr) {
    return nullptr;
  }

  std::vector<std::unique_ptr<AbstractExpression>> children;

  switch (root->arg_->type_) {
    case T_ColumnRef: {
      auto arg_expr = ColumnRefTransform(parse_result, reinterpret_cast<ColumnRef *>(root->arg_), nullptr);
      children.emplace_back(std::move(arg_expr));
      break;
    }
    case T_A_Const: {
      auto arg_expr = ConstTransform(parse_result, reinterpret_cast<A_Const *>(root->arg_));
      children.emplace_back(std::move(arg_expr));
      break;
    }
    case T_A_Expr: {
      auto arg_expr = AExprTransform(parse_result, reinterpret_cast<A_Expr *>(root->arg_));
      children.emplace_back(std::move(arg_expr));
      break;
    }
    case T_ParamRef: {
      auto arg_expr = ParamRefTransform(parse_result, reinterpret_cast<ParamRef *>(root->arg_));
      children.emplace_back(std::move(arg_expr));
      break;
    }
    default: {
      PARSER_LOG_AND_THROW("NullTestTransform", "ArgExpr type", root->arg_->type_);
    }
  }

  ExpressionType type =
      root->nulltesttype_ == IS_NULL ? ExpressionType::OPERATOR_IS_NULL : ExpressionType::OPERATOR_IS_NOT_NULL;

  return std::make_unique<OperatorExpression>(type, type::TypeId::BOOLEAN, std::move(children));
}

// Postgres.ParamRef -> noisepage.ParameterValueExpression
std::unique_ptr<AbstractExpression> PostgresParser::ParamRefTransform(ParseResult *parse_result, ParamRef *root) {
  auto result = std::make_unique<ParameterValueExpression>(root->number_ - 1);
  return result;
}

// Postgres.SubLink -> noisepage.
std::unique_ptr<AbstractExpression> PostgresParser::SubqueryExprTransform(ParseResult *parse_result, SubLink *node) {
  if (node == nullptr) {
    return nullptr;
  }

  auto select_stmt = SelectTransform(parse_result, reinterpret_cast<SelectStmt *>(node->subselect_));
  auto subquery_expr = std::make_unique<SubqueryExpression>(std::move(select_stmt));
  std::vector<std::unique_ptr<AbstractExpression>> children;
  std::unique_ptr<AbstractExpression> result;

  switch (node->sub_link_type_) {
    case ANY_SUBLINK: {
      auto col_expr = ExprTransform(parse_result, node->testexpr_, nullptr);
      children.emplace_back(std::move(col_expr));
      children.emplace_back(std::move(subquery_expr));
      result = std::make_unique<ComparisonExpression>(ExpressionType::COMPARE_IN, std::move(children));
      break;
    }
    case EXISTS_SUBLINK: {
      children.emplace_back(std::move(subquery_expr));
      result = std::make_unique<OperatorExpression>(ExpressionType::OPERATOR_EXISTS, type::TypeId::BOOLEAN,
                                                    std::move(children));
      break;
    }
    case EXPR_SUBLINK: {
      result = std::move(subquery_expr);
      break;
    }
    default: {
      PARSER_LOG_AND_THROW("SubqueryExprTransform", "Sublink type", node->sub_link_type_);
    }
  }

  return result;
}

// Postgres.TypeCast -> noisepage.TypeCastExpression
std::unique_ptr<AbstractExpression> PostgresParser::TypeCastTransform(ParseResult *parse_result, TypeCast *root) {
  auto type_name = reinterpret_cast<value *>(root->type_name_->names_->tail->data.ptr_value)->val_.str_;
  auto type = ColumnDefinition::StrToValueType(type_name);
  std::vector<std::unique_ptr<AbstractExpression>> children;
  children.emplace_back(ExprTransform(parse_result, root->arg_, nullptr));
  auto result = std::make_unique<TypeCastExpression>(type, std::move(children));
  return result;
}

// Postgres.value -> noisepage.ConstantValueExpression
std::unique_ptr<AbstractExpression> PostgresParser::ValueTransform(ParseResult *parse_result, value val) {
  std::unique_ptr<AbstractExpression> result;
  switch (val.type_) {
    case T_Integer: {
      result =
          std::make_unique<ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(val.val_.ival_));
      break;
    }

    case T_String: {
      const auto string = std::string_view{val.val_.str_};
      auto string_val = execution::sql::ValueUtil::CreateStringVal(string);
      result = std::make_unique<ConstantValueExpression>(type::TypeId::VARCHAR, string_val.first,
                                                         std::move(string_val.second));

      break;
    }

    case T_Float: {
      // Per Postgres, T_Float just means that the string looks like a number.
      // T_Float is also used for oversized ints, e.g. BIGINT.
      // For this reason, a quick hack...
      // TODO(WAN): figure out how Postgres does it once we care about floating point
      if (std::strchr(val.val_.str_, '.') == nullptr) {
        result = std::make_unique<ConstantValueExpression>(type::TypeId::BIGINT,
                                                           execution::sql::Integer(std::stoll(val.val_.str_)));
      } else {
        result = std::make_unique<ConstantValueExpression>(type::TypeId::REAL,
                                                           execution::sql::Real(std::stod(val.val_.str_)));
      }
      break;
    }

    case T_Null: {
      result = std::make_unique<ConstantValueExpression>(type::TypeId::INVALID, execution::sql::Val(true));
      break;
    }
    default: {
      PARSER_LOG_AND_THROW("ValueTransform", "Value type", val.type_);
    }
  }
  return result;
}

std::unique_ptr<SelectStatement> PostgresParser::SelectTransform(ParseResult *parse_result, SelectStmt *root) {
  std::unique_ptr<SelectStatement> result;

  switch (root->op_) {
    case SETOP_NONE: {
      auto target = TargetTransform(parse_result, root->target_list_);
      auto from = FromTransform(parse_result, root);
      auto select_distinct = root->distinct_clause_ != nullptr;
      auto groupby = GroupByTransform(parse_result, root->group_clause_, root->having_clause_);
      auto orderby = OrderByTransform(parse_result, root->sort_clause_);
      auto where = WhereTransform(parse_result, root->where_clause_);

      int64_t limit = LimitDescription::NO_LIMIT;
      int64_t offset = LimitDescription::NO_OFFSET;
      if (root->limit_count_ != nullptr) {
        limit = reinterpret_cast<A_Const *>(root->limit_count_)->val_.val_.ival_;
        if (root->limit_offset_ != nullptr) {
          offset = reinterpret_cast<A_Const *>(root->limit_offset_)->val_.val_.ival_;
        }
      }
      auto limit_desc = std::make_unique<LimitDescription>(limit, offset);

      result = std::make_unique<SelectStatement>(std::move(target), select_distinct, std::move(from), where,
                                                 std::move(groupby), std::move(orderby), std::move(limit_desc));
      break;
    }
    case SETOP_UNION: {
      result = SelectTransform(parse_result, root->larg_);
      result->SetUnionSelect(SelectTransform(parse_result, root->rarg_));
      break;
    }
    default: {
      PARSER_LOG_AND_THROW("SelectTransform", "Select operation", root->type_);
    }
  }

  return result;
}

// Postgres.SelectStmt.targetList -> noisepage.SelectStatement.select_
std::vector<common::ManagedPointer<AbstractExpression>> PostgresParser::TargetTransform(ParseResult *parse_result,
                                                                                        List *root) {
  // Postgres parses 'SELECT;' to nullptr
  if (root == nullptr) {
    throw PARSER_EXCEPTION("TargetTransform: root==null.");
  }

  std::vector<common::ManagedPointer<AbstractExpression>> result;
  for (auto cell = root->head; cell != nullptr; cell = cell->next) {
    auto target = reinterpret_cast<ResTarget *>(cell->data.ptr_value);
    auto expr = ExprTransform(parse_result, target->val_, target->name_);
    auto expr_managed = common::ManagedPointer(expr);
    parse_result->AddExpression(std::move(expr));
    result.emplace_back(expr_managed);
  }
  return result;
}

// TODO(WAN): doesn't support select from multiple sources, nested queries, various joins
// Postgres.SelectStmt.fromClause -> noisepage.TableRef
std::unique_ptr<TableRef> PostgresParser::FromTransform(ParseResult *parse_result, SelectStmt *select_root) {
  // current code assumes SELECT from one source
  List *root = select_root->from_clause_;

  // Postgres parses 'SELECT;' to nullptr
  if (root == nullptr) {
    return nullptr;
  }

  // TODO(WAN): this codepath came from the old system. Can simplify?
  if (root->length > 1) {
    std::vector<std::unique_ptr<TableRef>> refs;
    for (auto cell = root->head; cell != nullptr; cell = cell->next) {
      auto node = reinterpret_cast<Node *>(cell->data.ptr_value);
      switch (node->type) {
        case T_RangeVar: {
          refs.emplace_back(RangeVarTransform(parse_result, reinterpret_cast<RangeVar *>(node)));
          break;
        }
        case T_RangeSubselect: {
          refs.emplace_back(RangeSubselectTransform(parse_result, reinterpret_cast<RangeSubselect *>(node)));
          break;
        }
        default: {
          PARSER_LOG_AND_THROW("FromTransform", "FromType", node->type);
        }
      }
    }
    auto result = TableRef::CreateTableRefByList(std::move(refs));
    return result;
  }

  std::unique_ptr<TableRef> result = nullptr;
  auto node = reinterpret_cast<Node *>(root->head->data.ptr_value);
  switch (node->type) {
    case T_RangeVar: {
      result = RangeVarTransform(parse_result, reinterpret_cast<RangeVar *>(node));
      break;
    }
    case T_JoinExpr: {
      auto join = JoinTransform(parse_result, reinterpret_cast<JoinExpr *>(node));
      if (join != nullptr) {
        result = TableRef::CreateTableRefByJoin(std::move(join));
      }
      break;
    }
    case T_RangeSubselect: {
      result = RangeSubselectTransform(parse_result, reinterpret_cast<RangeSubselect *>(node));
      break;
    }
    default: {
      PARSER_LOG_AND_THROW("FromTransform", "FromType", node->type);
    }
  }

  return result;
}

// Postgres.SelectStmt.groupClause -> noisepage.GroupByDescription
std::unique_ptr<GroupByDescription> PostgresParser::GroupByTransform(ParseResult *parse_result, List *group,
                                                                     Node *having_node) {
  if (group == nullptr && having_node == nullptr) {
    return nullptr;
  }

  std::vector<common::ManagedPointer<AbstractExpression>> columns;
  if (group != nullptr) {
    for (auto cell = group->head; cell != nullptr; cell = cell->next) {
      auto temp = reinterpret_cast<Node *>(cell->data.ptr_value);
      auto expr = ExprTransform(parse_result, temp, nullptr);
      auto expr_ptr = common::ManagedPointer(expr);
      parse_result->AddExpression(std::move(expr));
      columns.emplace_back(expr_ptr);
    }
  }

  // TODO(WAN): old system says, having clauses not implemented, depends on AExprTransform
  auto having = common::ManagedPointer<AbstractExpression>(nullptr);
  if (having_node != nullptr) {
    auto expr = ExprTransform(parse_result, having_node, nullptr);
    having = common::ManagedPointer(expr);
    parse_result->AddExpression(std::move(expr));
  }

  auto result = std::make_unique<GroupByDescription>(std::move(columns), having);
  return result;
}

// Postgres.SelectStmt.sortClause -> noisepage.OrderDescription
std::unique_ptr<OrderByDescription> PostgresParser::OrderByTransform(ParseResult *parse_result, List *order) {
  if (order == nullptr) {
    return nullptr;
  }

  std::vector<OrderType> types;
  std::vector<common::ManagedPointer<AbstractExpression>> exprs;

  for (auto cell = order->head; cell != nullptr; cell = cell->next) {
    auto temp = reinterpret_cast<Node *>(cell->data.ptr_value);
    switch (temp->type) {
      case T_SortBy: {
        auto sort = reinterpret_cast<SortBy *>(temp);

        switch (sort->sortby_dir_) {
          case SORTBY_DESC: {
            types.emplace_back(kOrderDesc);
            break;
          }
          case SORTBY_ASC:  // fall through
          case SORTBY_DEFAULT: {
            types.emplace_back(kOrderAsc);
            break;
          }
          default: {
            PARSER_LOG_AND_THROW("OrderByTransform", "Sortby type", sort->sortby_dir_);
          }
        }

        auto target = sort->node_;
        auto expr = ExprTransform(parse_result, target, nullptr);
        auto expr_ptr = common::ManagedPointer(expr);
        parse_result->AddExpression(std::move(expr));
        exprs.emplace_back(expr_ptr);
        break;
      }
      default: {
        PARSER_LOG_AND_THROW("OrderByTransform", "OrderBy type", temp->type);
      }
    }
  }

  auto result = std::make_unique<OrderByDescription>(std::move(types), std::move(exprs));
  return result;
}

// Postgres.SelectStmt.whereClause -> noisepage.AbstractExpression
common::ManagedPointer<AbstractExpression> PostgresParser::WhereTransform(ParseResult *parse_result, Node *root) {
  if (root == nullptr) {
    return nullptr;
  }
  auto expr = ExprTransform(parse_result, root, nullptr);
  auto result = common::ManagedPointer(expr);
  parse_result->AddExpression(std::move(expr));
  return result;
}

// Postgres.JoinExpr -> noisepage.JoinDefinition
std::unique_ptr<JoinDefinition> PostgresParser::JoinTransform(ParseResult *parse_result, JoinExpr *root) {
  // TODO(WAN): magic number 4?
  if ((root->jointype_ > 4) || (root->is_natural_)) {
    return nullptr;
  }

  JoinType type;
  switch (root->jointype_) {
    case JOIN_INNER: {
      type = JoinType::INNER;
      break;
    }
    case JOIN_LEFT: {
      type = JoinType::LEFT;
      break;
    }
    case JOIN_FULL: {
      type = JoinType::OUTER;
      break;
    }
    case JOIN_RIGHT: {
      type = JoinType::RIGHT;
      break;
    }
    case JOIN_SEMI: {
      type = JoinType::SEMI;
      break;
    }
    default: {
      PARSER_LOG_AND_THROW("JoinTransform", "JoinType", root->jointype_);
    }
  }

  std::unique_ptr<TableRef> left;
  switch (root->larg_->type) {
    case T_RangeVar: {
      left = RangeVarTransform(parse_result, reinterpret_cast<RangeVar *>(root->larg_));
      break;
    }
    case T_RangeSubselect: {
      left = RangeSubselectTransform(parse_result, reinterpret_cast<RangeSubselect *>(root->larg_));
      break;
    }
    case T_JoinExpr: {
      auto join = JoinTransform(parse_result, reinterpret_cast<JoinExpr *>(root->larg_));
      left = TableRef::CreateTableRefByJoin(std::move(join));
      break;
    }
    default: {
      PARSER_LOG_AND_THROW("JoinTransform", "JoinArgType", root->larg_->type);
    }
  }

  std::unique_ptr<TableRef> right;
  switch (root->rarg_->type) {
    case T_RangeVar: {
      right = RangeVarTransform(parse_result, reinterpret_cast<RangeVar *>(root->rarg_));
      break;
    }
    case T_RangeSubselect: {
      right = RangeSubselectTransform(parse_result, reinterpret_cast<RangeSubselect *>(root->rarg_));
      break;
    }
    case T_JoinExpr: {
      auto join = JoinTransform(parse_result, reinterpret_cast<JoinExpr *>(root->rarg_));
      right = TableRef::CreateTableRefByJoin(std::move(join));
      break;
    }
    default: {
      PARSER_LOG_AND_THROW("JoinTransform", "Right JoinArgType", root->rarg_->type);
    }
  }

  // TODO(WAN): We currently segfault on the following test case:
  // SELECT * FROM tab0 AS cor0 CROSS JOIN tab0 AS cor1 WHERE NULL IS NOT NULL;
  // We need to figure out how CROSS JOIN gets parsed.
  if (root->quals_ == nullptr) {
    PARSER_LOG_AND_THROW("JoinTransform", "root->quals", nullptr);
  }

  std::unique_ptr<AbstractExpression> expr;
  switch (root->quals_->type) {
    case T_A_Expr: {
      expr = AExprTransform(parse_result, reinterpret_cast<A_Expr *>(root->quals_));
      break;
    }
    case T_BoolExpr: {
      expr = BoolExprTransform(parse_result, reinterpret_cast<BoolExpr *>(root->quals_));
      break;
    }
    default: {
      PARSER_LOG_AND_THROW("JoinTransform", "Join condition type", root->quals_->type);
    }
  }

  auto condition = common::ManagedPointer(expr);
  parse_result->AddExpression(std::move(expr));

  auto result = std::make_unique<JoinDefinition>(type, std::move(left), std::move(right), condition);
  return result;
}

std::string PostgresParser::AliasTransform(Alias *root) {
  if (root == nullptr) {
    return "";
  }
  return root->aliasname_;
}

// Postgres.RangeVar -> noisepage.TableRef
std::unique_ptr<TableRef> PostgresParser::RangeVarTransform(ParseResult *parse_result, RangeVar *root) {
  auto table_name = root->relname_ == nullptr ? "" : root->relname_;
  auto schema_name = root->schemaname_ == nullptr ? "" : root->schemaname_;
  auto database_name = root->catalogname_ == nullptr ? "" : root->catalogname_;

  auto table_info = std::make_unique<TableInfo>(table_name, schema_name, database_name);
  auto alias = AliasTransform(root->alias_);
  auto result = TableRef::CreateTableRefByName(alias, std::move(table_info));
  return result;
}

// Postgres.RangeSubselect -> noisepage.TableRef
std::unique_ptr<TableRef> PostgresParser::RangeSubselectTransform(ParseResult *parse_result, RangeSubselect *root) {
  auto select = SelectTransform(parse_result, reinterpret_cast<SelectStmt *>(root->subquery_));
  if (select == nullptr) {
    return nullptr;
  }
  auto alias = AliasTransform(root->alias_);
  auto result = TableRef::CreateTableRefBySelect(alias, std::move(select));
  return result;
}

// Postgres.CopyStmt -> noisepage.CopyStatement
std::unique_ptr<CopyStatement> PostgresParser::CopyTransform(ParseResult *parse_result, CopyStmt *root) {
  static constexpr char k_delimiter_tok[] = "delimiter";
  static constexpr char k_format_tok[] = "format";
  static constexpr char k_quote_tok[] = "quote";
  static constexpr char k_escape_tok[] = "escape";

  std::unique_ptr<TableRef> table;
  std::unique_ptr<SelectStatement> select_stmt;
  if (root->relation_ != nullptr) {
    table = RangeVarTransform(parse_result, root->relation_);
  } else {
    select_stmt = SelectTransform(parse_result, reinterpret_cast<SelectStmt *>(root->query_));
  }

  auto file_path = root->filename_ != nullptr ? root->filename_ : "";
  auto is_from = root->is_from_;

  char delimiter = ',';
  ExternalFileFormat format = ExternalFileFormat::CSV;
  char quote = '"';
  char escape = '"';
  if (root->options_ != nullptr) {
    for (ListCell *cell = root->options_->head; cell != nullptr; cell = cell->next) {
      auto def_elem = reinterpret_cast<DefElem *>(cell->data.ptr_value);

      if (strncmp(def_elem->defname_, k_format_tok, sizeof(k_format_tok)) == 0) {
        auto format_cstr = reinterpret_cast<value *>(def_elem->arg_)->val_.str_;
        // lowercase
        if (strcmp(format_cstr, "csv") == 0) {
          format = ExternalFileFormat::CSV;
        } else if (strcmp(format_cstr, "binary") == 0) {
          format = ExternalFileFormat::BINARY;
        }
      }

      if (strncmp(def_elem->defname_, k_delimiter_tok, sizeof(k_delimiter_tok)) == 0) {
        delimiter = *(reinterpret_cast<value *>(def_elem->arg_)->val_.str_);
      }

      if (strncmp(def_elem->defname_, k_quote_tok, sizeof(k_quote_tok)) == 0) {
        quote = *(reinterpret_cast<value *>(def_elem->arg_)->val_.str_);
      }

      if (strncmp(def_elem->defname_, k_escape_tok, sizeof(k_escape_tok)) == 0) {
        escape = *(reinterpret_cast<value *>(def_elem->arg_)->val_.str_);
      }
    }
  }

  auto result = std::make_unique<CopyStatement>(std::move(table), std::move(select_stmt), file_path, format, is_from,
                                                delimiter, quote, escape);
  return result;
}

// Postgres.CreateStmt -> noisepage.CreateStatement
std::unique_ptr<SQLStatement> PostgresParser::CreateTransform(ParseResult *parse_result, CreateStmt *root) {
  RangeVar *relation = root->relation_;
  auto table_name = relation->relname_ != nullptr ? relation->relname_ : "";
  auto schema_name = relation->schemaname_ != nullptr ? relation->schemaname_ : "";
  auto database_name = relation->catalogname_ != nullptr ? relation->catalogname_ : "";
  std::unique_ptr<TableInfo> table_info = std::make_unique<TableInfo>(table_name, schema_name, database_name);

  std::unordered_set<std::string> primary_keys;

  std::vector<std::unique_ptr<ColumnDefinition>> columns;
  std::vector<std::unique_ptr<ColumnDefinition>> foreign_keys;

  for (auto cell = root->table_elts_->head; cell != nullptr; cell = cell->next) {
    auto node = reinterpret_cast<Node *>(cell->data.ptr_value);
    switch (node->type) {
      case T_ColumnDef: {
        auto res = ColumnDefTransform(parse_result, reinterpret_cast<ColumnDef *>(node));
        columns.emplace_back(std::move(res.col_));
        foreign_keys.insert(foreign_keys.end(), std::make_move_iterator(res.fks_.begin()),
                            std::make_move_iterator(res.fks_.end()));
        break;
      }
      case T_Constraint: {
        auto constraint = reinterpret_cast<Constraint *>(node);
        switch (constraint->contype_) {
          case CONSTR_PRIMARY: {
            for (auto key_cell = constraint->keys_->head; key_cell != nullptr; key_cell = key_cell->next) {
              primary_keys.emplace(reinterpret_cast<value *>(key_cell->data.ptr_value)->val_.str_);
            }
            break;
          }
          case CONSTR_FOREIGN: {
            std::vector<std::string> fk_sources;
            for (auto attr_cell = constraint->fk_attrs_->head; attr_cell != nullptr; attr_cell = attr_cell->next) {
              auto attr_val = reinterpret_cast<value *>(attr_cell->data.ptr_value);
              fk_sources.emplace_back(attr_val->val_.str_);
            }

            std::vector<std::string> fk_sinks;
            for (auto attr_cell = constraint->pk_attrs_->head; attr_cell != nullptr; attr_cell = attr_cell->next) {
              auto attr_val = reinterpret_cast<value *>(attr_cell->data.ptr_value);
              fk_sinks.emplace_back(attr_val->val_.str_);
            }

            auto fk_sink_table_name = constraint->pktable_->relname_;
            auto fk_delete_action = CharToActionType(constraint->fk_del_action_);
            auto fk_update_action = CharToActionType(constraint->fk_upd_action_);
            auto fk_match_type = CharToMatchType(constraint->fk_matchtype_);

            auto fk = std::make_unique<ColumnDefinition>(std::move(fk_sources), std::move(fk_sinks), fk_sink_table_name,
                                                         fk_delete_action, fk_update_action, fk_match_type);

            foreign_keys.emplace_back(std::move(fk));
            break;
          }
          default: {
            PARSER_LOG_DEBUG("CreateTransform: constraint of type {} not supported", constraint->contype_);
            throw NOT_IMPLEMENTED_EXCEPTION("CreateTransform error");
          }
        }
        break;
      }
      default: {
        PARSER_LOG_DEBUG("CreateTransform: tableElt type {} not supported", node->type);
        throw NOT_IMPLEMENTED_EXCEPTION("CreateTransform error");
      }
    }
  }

  // TODO(WAN): had to un-const is_primary, this is hacky, are we not guaranteed anything about order of nodes?
  for (auto &column : columns) {
    // skip foreign key constraint
    if (column->GetColumnName().empty()) {
      continue;
    }
    if (primary_keys.find(column->GetColumnName()) != primary_keys.end()) {
      column->SetPrimary(true);
    }
  }

  auto result = std::make_unique<CreateStatement>(std::move(table_info), CreateStatement::CreateType::kTable,
                                                  std::move(columns), std::move(foreign_keys));
  return result;
}

// Postgres.CreateDatabaseStmt -> noisepage.CreateStatement
std::unique_ptr<parser::SQLStatement> PostgresParser::CreateDatabaseTransform(ParseResult *parse_result,
                                                                              CreateDatabaseStmt *root) {
  auto table_info = std::make_unique<TableInfo>("", "", root->dbname_);
  std::vector<std::unique_ptr<ColumnDefinition>> columns;
  std::vector<std::unique_ptr<ColumnDefinition>> foreign_keys;
  auto result = std::make_unique<CreateStatement>(std::move(table_info), CreateStatement::kDatabase, std::move(columns),
                                                  std::move(foreign_keys));

  // TODO(WAN): per the old system, more options need to be converted
  // see postgresparser.h and the postgresql docs
  return result;
}

// Postgres.CreateFunctionStmt -> noisepage.CreateFunctionStatement
std::unique_ptr<SQLStatement> PostgresParser::CreateFunctionTransform(ParseResult *parse_result,
                                                                      CreateFunctionStmt *root) {
  bool replace = root->replace_;
  std::vector<std::unique_ptr<FuncParameter>> func_parameters;

  for (auto cell = root->parameters_->head; cell != nullptr; cell = cell->next) {
    auto node = reinterpret_cast<Node *>(cell->data.ptr_value);
    switch (node->type) {
      case T_FunctionParameter: {
        func_parameters.emplace_back(
            FunctionParameterTransform(parse_result, reinterpret_cast<FunctionParameter *>(node)));
        break;
      }
      default: {
        // TODO(WAN): previous code just ignored it, is this right?
        break;
      }
    }
  }

  auto return_type = ReturnTypeTransform(parse_result, reinterpret_cast<TypeName *>(root->return_type_));

  // TODO(WAN): assumption from old code, can only pass one function name for now
  std::string func_name = (reinterpret_cast<value *>(root->funcname_->tail->data.ptr_value)->val_.str_);

  std::vector<std::string> func_body;
  AsType as_type = AsType::INVALID;
  PLType pl_type = PLType::INVALID;

  for (auto cell = root->options_->head; cell != nullptr; cell = cell->next) {
    auto def_elem = reinterpret_cast<DefElem *>(cell->data.ptr_value);
    if (strcmp(def_elem->defname_, "as") == 0) {
      auto list_of_arg = reinterpret_cast<List *>(def_elem->arg_);

      for (auto cell2 = list_of_arg->head; cell2 != nullptr; cell2 = cell2->next) {
        std::string query_string = reinterpret_cast<value *>(cell2->data.ptr_value)->val_.str_;
        func_body.push_back(query_string);
      }

      if (func_body.size() > 1) {
        as_type = AsType::EXECUTABLE;
      } else {
        as_type = AsType::QUERY_STRING;
      }
    } else if (strcmp(def_elem->defname_, "language") == 0) {
      auto lang = reinterpret_cast<value *>(def_elem->arg_)->val_.str_;
      if (strcmp(lang, "plpgsql") == 0) {
        pl_type = PLType::PL_PGSQL;
      } else if (strcmp(lang, "c") == 0) {
        pl_type = PLType::PL_C;
      } else {
        PARSER_LOG_AND_THROW("CreateFunctionTransform", "PLType", lang);
      }
    }
  }

  auto result =
      std::make_unique<CreateFunctionStatement>(replace, std::move(func_name), std::move(func_body),
                                                std::move(return_type), std::move(func_parameters), pl_type, as_type);

  return result;
}

// Postgres.IndexStmt -> noisepage.CreateStatement
std::unique_ptr<SQLStatement> PostgresParser::CreateIndexTransform(ParseResult *parse_result, IndexStmt *root) {
  auto unique = root->unique_;

  NOISEPAGE_ASSERT(root->relation_->relname_ != nullptr, "It can't be empty. See postgres spec.");

  auto table_name = root->relation_->relname_;
  auto schema_name = root->relation_->schemaname_ == nullptr ? "" : root->relation_->schemaname_;
  auto database_name = root->relation_->catalogname_ == nullptr ? "" : root->relation_->catalogname_;
  auto table_info = std::make_unique<TableInfo>(table_name, schema_name, database_name);

  const bool no_name = root->idxname_ == nullptr;
  std::string index_name;
  if (!no_name) {
    index_name = root->idxname_;
  } else {
    index_name = table_name;
  }

  std::vector<IndexAttr> index_attrs;
  for (auto cell = root->index_params_->head; cell != nullptr; cell = cell->next) {
    auto *index_elem = reinterpret_cast<IndexElem *>(cell->data.ptr_value);
    if (index_elem->expr_ == nullptr) {
      index_attrs.emplace_back(index_elem->name_);
      if (no_name) {
        index_name += "_" + std::string(index_elem->name_);
      }
    } else {
      auto expr = ExprTransform(parse_result, index_elem->expr_, nullptr);
      auto expr_ptr = common::ManagedPointer(expr);
      parse_result->AddExpression(std::move(expr));
      index_attrs.emplace_back(expr_ptr);
    }
  }

  if (no_name) {
    index_name += "_idx";
  }

  char *access_method = root->access_method_;
  IndexType index_type;
  // TODO(WAN): do we need to do case conversion?
  if (strcmp(access_method, "invalid") == 0) {
    index_type = IndexType::INVALID;
  } else if ((strcmp(access_method, "btree") == 0) || (strcmp(access_method, "bwtree") == 0)) {
    index_type = IndexType::BWTREE;
  } else if (strcmp(access_method, "hash") == 0) {
    index_type = IndexType::HASH;
  } else {
    PARSER_LOG_DEBUG("CreateIndexTransform: IndexType {} not supported", access_method);
    throw NOT_IMPLEMENTED_EXCEPTION("CreateIndexTransform error");
  }

  return std::make_unique<CreateStatement>(std::move(table_info), index_type, unique, index_name,
                                           std::move(index_attrs));
}

// Postgres.CreateSchemaStmt -> noisepage.CreateStatement
std::unique_ptr<SQLStatement> PostgresParser::CreateSchemaTransform(ParseResult *parse_result, CreateSchemaStmt *root) {
  std::string schema_name;
  if (root->schemaname_ != nullptr) {
    schema_name = root->schemaname_;
  } else {
    NOISEPAGE_ASSERT(root->authrole_ != nullptr, "We need a schema name.");
    switch (root->authrole_->type) {
      case T_RoleSpec: {
        // TODO(WAN): old system said they didn't need the authrole.. not sure if that's true
        auto authrole = reinterpret_cast<RoleSpec *>(root->authrole_);
        schema_name = authrole->rolename_;
        break;
      }
      default: {
        PARSER_LOG_AND_THROW("CreateSchemaTransform", "AuthRole", root->authrole_->type);
      }
    }
  }

  auto table_info = std::make_unique<TableInfo>("", schema_name, "");
  auto if_not_exists = root->if_not_exists_;

  // TODO(WAN): the old system basically didn't implement any of this

  if (root->schema_elts_ != nullptr) {
    PARSER_LOG_DEBUG("CreateSchemaTransform schema_element unsupported");
    throw PARSER_EXCEPTION("CreateSchemaTransform schema_element unsupported");
  }

  auto result = std::make_unique<CreateStatement>(std::move(table_info), if_not_exists);
  return result;
}

// Postgres.CreateTrigStmt -> noisepage.CreateStatement
std::unique_ptr<SQLStatement> PostgresParser::CreateTriggerTransform(ParseResult *parse_result, CreateTrigStmt *root) {
  auto table_name = root->relation_->relname_ == nullptr ? "" : root->relation_->relname_;
  auto schema_name = root->relation_->schemaname_ == nullptr ? "" : root->relation_->schemaname_;
  auto database_name = root->relation_->catalogname_ == nullptr ? "" : root->relation_->catalogname_;
  auto table_info = std::make_unique<TableInfo>(table_name, schema_name, database_name);

  auto trigger_name = root->trigname_;

  std::vector<std::string> trigger_funcnames;
  if (root->funcname_ != nullptr) {
    for (auto cell = root->funcname_->head; cell != nullptr; cell = cell->next) {
      std::string name = reinterpret_cast<value *>(cell->data.ptr_value)->val_.str_;
      trigger_funcnames.emplace_back(name);
    }
  }

  std::vector<std::string> trigger_args;
  if (root->args_ != nullptr) {
    for (auto cell = root->args_->head; cell != nullptr; cell = cell->next) {
      std::string arg = (reinterpret_cast<value *>(cell->data.ptr_value))->val_.str_;
      trigger_args.push_back(arg);
    }
  }

  std::vector<std::string> trigger_columns;
  if (root->columns_ != nullptr) {
    for (auto cell = root->columns_->head; cell != nullptr; cell = cell->next) {
      std::string column = (reinterpret_cast<value *>(cell->data.ptr_value))->val_.str_;
      trigger_columns.push_back(column);
    }
  }

  auto trigger_when = WhenTransform(parse_result, root->when_clause_);
  auto trigger_when_ptr = common::ManagedPointer(trigger_when);
  parse_result->AddExpression(std::move(trigger_when));

  // TODO(WAN): what is this doing?
  int16_t trigger_type = 0;
  TRIGGER_CLEAR_TYPE(trigger_type);
  if (root->row_) {
    TRIGGER_SETT_ROW(trigger_type);
  }
  trigger_type |= root->timing_;
  trigger_type |= root->events_;

  auto result = std::make_unique<CreateStatement>(std::move(table_info), trigger_name, std::move(trigger_funcnames),
                                                  std::move(trigger_args), std::move(trigger_columns), trigger_when_ptr,
                                                  trigger_type);
  return result;
}

// Postgres.ViewStmt -> noisepage.CreateStatement
std::unique_ptr<SQLStatement> PostgresParser::CreateViewTransform(ParseResult *parse_result, ViewStmt *root) {
  auto view_name = root->view_->relname_;

  std::unique_ptr<SelectStatement> view_query;
  switch (root->query_->type) {
    case T_SelectStmt: {
      view_query = SelectTransform(parse_result, reinterpret_cast<SelectStmt *>(root->query_));
      break;
    }
    default: {
      PARSER_LOG_DEBUG("CREATE VIEW as query only supports SELECT");
      throw PARSER_EXCEPTION("CREATE VIEW as query only supports SELECT");
    }
  }

  auto result = std::make_unique<CreateStatement>(view_name, std::move(view_query));
  return result;
}

// Postgres.ColumnDef -> noisepage.ColumnDefinition
PostgresParser::ColumnDefTransResult PostgresParser::ColumnDefTransform(ParseResult *parse_result, ColumnDef *root) {
  auto type_name = root->type_name_;

  // handle varlen
  int32_t varlen = -1;
  if (type_name->typmods_ != nullptr) {
    auto node = reinterpret_cast<Node *>(type_name->typmods_->head->data.ptr_value);
    switch (node->type) {
      case T_A_Const: {
        auto node_type = reinterpret_cast<A_Const *>(node)->val_.type_;
        switch (node_type) {
          case T_Integer: {
            varlen = static_cast<int32_t>(reinterpret_cast<A_Const *>(node)->val_.val_.ival_);
            break;
          }
          default: {
            PARSER_LOG_AND_THROW("ColumnDefTransform", "typmods", node_type);
          }
        }
        break;
      }
      default: {
        PARSER_LOG_AND_THROW("ColumnDefTransform", "typmods", node->type);
      }
    }
  }

  auto datatype_name = reinterpret_cast<value *>(type_name->names_->tail->data.ptr_value)->val_.str_;
  auto datatype = ColumnDefinition::StrToDataType(datatype_name);

  std::vector<std::unique_ptr<ColumnDefinition>> foreign_keys;

  bool is_primary = false;
  bool is_not_null = false;
  bool is_unique = false;
  auto default_expr = common::ManagedPointer<AbstractExpression>(nullptr);
  auto check_expr = common::ManagedPointer<AbstractExpression>(nullptr);

  if (root->constraints_ != nullptr) {
    for (auto cell = root->constraints_->head; cell != nullptr; cell = cell->next) {
      auto constraint = reinterpret_cast<Constraint *>(cell->data.ptr_value);
      switch (constraint->contype_) {
        case CONSTR_PRIMARY: {
          is_primary = true;
          break;
        }
        case CONSTR_NOTNULL: {
          is_not_null = true;
          break;
        }
        case CONSTR_UNIQUE: {
          is_unique = true;
          break;
        }
        case CONSTR_FOREIGN: {
          std::vector<std::string> fk_sinks;
          std::vector<std::string> fk_sources;

          if (constraint->pk_attrs_ == nullptr) {
            throw NOT_IMPLEMENTED_EXCEPTION("Foreign key columns unspecified");
          }

          auto attr_cell = constraint->pk_attrs_->head;
          auto attr_val = reinterpret_cast<value *>(attr_cell->data.ptr_value);
          fk_sinks.emplace_back(attr_val->val_.str_);
          fk_sources.emplace_back(root->colname_);

          auto fk_sink_table_name = constraint->pktable_->relname_;
          auto fk_delete_action = CharToActionType(constraint->fk_del_action_);
          auto fk_update_action = CharToActionType(constraint->fk_upd_action_);
          auto fk_match_type = CharToMatchType(constraint->fk_matchtype_);

          auto coldef =
              std::make_unique<ColumnDefinition>(std::move(fk_sources), std::move(fk_sinks), fk_sink_table_name,
                                                 fk_delete_action, fk_update_action, fk_match_type);

          foreign_keys.emplace_back(std::move(coldef));
          break;
        }
        case CONSTR_DEFAULT: {
          auto expr = ExprTransform(parse_result, constraint->raw_expr_, nullptr);
          default_expr = common::ManagedPointer(expr);
          parse_result->AddExpression(std::move(expr));
          break;
        }
        case CONSTR_CHECK: {
          auto expr = ExprTransform(parse_result, constraint->raw_expr_, nullptr);
          check_expr = common::ManagedPointer(expr);
          parse_result->AddExpression(std::move(expr));
          break;
        }
        default: {
          PARSER_LOG_AND_THROW("ColumnDefTransform", "Constraint", constraint->contype_);
        }
      }
    }
  }

  auto name = root->colname_;
  auto result = std::make_unique<ColumnDefinition>(name, datatype, is_primary, is_not_null, is_unique, default_expr,
                                                   check_expr, varlen);

  return {std::move(result), std::move(foreign_keys)};
}

// Postgres.FunctionParameter -> noisepage.FuncParameter
std::unique_ptr<FuncParameter> PostgresParser::FunctionParameterTransform(ParseResult *parse_result,
                                                                          FunctionParameter *root) {
  // TODO(WAN): significant code duplication, refactor out char* -> DataType
  char *name = (reinterpret_cast<value *>(root->arg_type_->names_->tail->data.ptr_value)->val_.str_);
  parser::FuncParameter::DataType data_type;

  if ((strcmp(name, "int") == 0) || (strcmp(name, "int4") == 0)) {
    data_type = BaseFunctionParameter::DataType::INT;
  } else if (strcmp(name, "varchar") == 0) {
    data_type = BaseFunctionParameter::DataType::VARCHAR;
  } else if (strcmp(name, "int8") == 0) {
    data_type = BaseFunctionParameter::DataType::BIGINT;
  } else if (strcmp(name, "int2") == 0) {
    data_type = BaseFunctionParameter::DataType::SMALLINT;
  } else if ((strcmp(name, "double") == 0) || (strcmp(name, "float8") == 0)) {
    data_type = BaseFunctionParameter::DataType::DOUBLE;
  } else if ((strcmp(name, "real") == 0) || (strcmp(name, "float4") == 0)) {
    data_type = BaseFunctionParameter::DataType::FLOAT;
  } else if (strcmp(name, "text") == 0) {
    data_type = BaseFunctionParameter::DataType::TEXT;
  } else if (strcmp(name, "bpchar") == 0) {
    data_type = BaseFunctionParameter::DataType::CHAR;
  } else if (strcmp(name, "tinyint") == 0) {
    data_type = BaseFunctionParameter::DataType::TINYINT;
  } else if (strcmp(name, "bool") == 0) {
    data_type = BaseFunctionParameter::DataType::BOOL;
  } else {
    PARSER_LOG_AND_THROW("FunctionParameterTransform", "DataType", name);
  }

  auto param_name = root->name_ != nullptr ? root->name_ : "";
  auto result = std::make_unique<FuncParameter>(data_type, param_name);
  return result;
}

// Postgres.TypeName -> noisepage.ReturnType
std::unique_ptr<ReturnType> PostgresParser::ReturnTypeTransform(ParseResult *parse_result, TypeName *root) {
  char *name = (reinterpret_cast<value *>(root->names_->tail->data.ptr_value)->val_.str_);
  ReturnType::DataType data_type;

  if ((strcmp(name, "int") == 0) || (strcmp(name, "int4") == 0)) {
    data_type = BaseFunctionParameter::DataType::INT;
  } else if (strcmp(name, "varchar") == 0) {
    data_type = BaseFunctionParameter::DataType::VARCHAR;
  } else if (strcmp(name, "int8") == 0) {
    data_type = BaseFunctionParameter::DataType::BIGINT;
  } else if (strcmp(name, "int2") == 0) {
    data_type = BaseFunctionParameter::DataType::SMALLINT;
  } else if ((strcmp(name, "double") == 0) || (strcmp(name, "float8") == 0)) {
    data_type = BaseFunctionParameter::DataType::DOUBLE;
  } else if ((strcmp(name, "real") == 0) || (strcmp(name, "float4") == 0)) {
    data_type = BaseFunctionParameter::DataType::FLOAT;
  } else if (strcmp(name, "text") == 0) {
    data_type = BaseFunctionParameter::DataType::TEXT;
  } else if (strcmp(name, "bpchar") == 0) {
    data_type = BaseFunctionParameter::DataType::CHAR;
  } else if (strcmp(name, "tinyint") == 0) {
    data_type = BaseFunctionParameter::DataType::TINYINT;
  } else if (strcmp(name, "bool") == 0) {
    data_type = BaseFunctionParameter::DataType::BOOL;
  } else {
    PARSER_LOG_AND_THROW("ReturnTypeTransform", "ReturnType", name);
  }

  auto result = std::make_unique<ReturnType>(data_type);
  return result;
}

// Postgres.Node -> noisepage.AbstractExpression
std::unique_ptr<AbstractExpression> PostgresParser::WhenTransform(ParseResult *parse_result, Node *root) {
  if (root == nullptr) {
    return nullptr;
  }
  std::unique_ptr<AbstractExpression> result;
  switch (root->type) {
    case T_A_Expr: {
      result = AExprTransform(parse_result, reinterpret_cast<A_Expr *>(root));
      break;
    }
    case T_BoolExpr: {
      result = BoolExprTransform(parse_result, reinterpret_cast<BoolExpr *>(root));
      break;
    }
    default: {
      PARSER_LOG_AND_THROW("WhenTransform", "WHEN type", root->type);
    }
  }
  return result;
}

// Postgres.DeleteStmt -> noisepage.DeleteStatement
std::unique_ptr<DeleteStatement> PostgresParser::DeleteTransform(ParseResult *parse_result, DeleteStmt *root) {
  std::unique_ptr<DeleteStatement> result;
  auto table = RangeVarTransform(parse_result, root->relation_);
  auto where = WhereTransform(parse_result, root->where_clause_);
  result = std::make_unique<DeleteStatement>(std::move(table), where);
  return result;
}

// Postgres.DropStmt -> noisepage.DropStatement
std::unique_ptr<DropStatement> PostgresParser::DropTransform(ParseResult *parse_result, DropStmt *root) {
  switch (root->remove_type_) {
    case ObjectType::OBJECT_INDEX: {
      return DropIndexTransform(parse_result, root);
    }
    case ObjectType::OBJECT_SCHEMA: {
      return DropSchemaTransform(parse_result, root);
    }
    case ObjectType::OBJECT_TABLE: {
      return DropTableTransform(parse_result, root);
    }
    case ObjectType::OBJECT_TRIGGER: {
      return DropTriggerTransform(parse_result, root);
    }
    default: {
      PARSER_LOG_AND_THROW("DropTransform", "Drop ObjectType", root->remove_type_);
    }
  }
}

// Postgres.DropDatabaseStmt -> noisepage.DropStmt
std::unique_ptr<DropStatement> PostgresParser::DropDatabaseTransform(ParseResult *parse_result,
                                                                     DropDatabaseStmt *root) {
  auto table_info = std::make_unique<TableInfo>("", "", root->dbname_);
  auto if_exists = root->missing_ok_;

  auto result = std::make_unique<DropStatement>(std::move(table_info), DropStatement::DropType::kDatabase, if_exists);
  return result;
}

// Postgres.DropStmt -> noisepage.DropStatement
std::unique_ptr<DropStatement> PostgresParser::DropIndexTransform(ParseResult *parse_result, DropStmt *root) {
  // TODO(WAN): There are unimplemented DROP INDEX options.

  std::string schema_name;
  std::string index_name;
  auto list = reinterpret_cast<List *>(root->objects_->head->data.ptr_value);
  if (list->length == 2) {
    // List length is 2 when the schema length is specified, e.g. DROP INDEX/TABLE A.B.
    schema_name = reinterpret_cast<value *>(list->head->data.ptr_value)->val_.str_;
    index_name = reinterpret_cast<value *>(list->head->next->data.ptr_value)->val_.str_;
  } else {
    index_name = reinterpret_cast<value *>(list->head->data.ptr_value)->val_.str_;
  }

  auto table_info = std::make_unique<TableInfo>("", schema_name, "");
  auto result = std::make_unique<DropStatement>(std::move(table_info), index_name);
  return result;
}

std::unique_ptr<DropStatement> PostgresParser::DropSchemaTransform(ParseResult *parse_result, DropStmt *root) {
  auto if_exists = root->missing_ok_;
  auto cascade = root->behavior_ == DropBehavior::DROP_CASCADE;

  std::string schema_name;
  for (auto cell = root->objects_->head; cell != nullptr; cell = cell->next) {
    auto table_list = reinterpret_cast<List *>(cell->data.ptr_value);
    schema_name = reinterpret_cast<value *>(table_list->head->data.ptr_value)->val_.str_;
    break;
  }

  auto table_info = std::make_unique<TableInfo>("", schema_name, "");
  auto result = std::make_unique<DropStatement>(std::move(table_info), if_exists, cascade);
  return result;
}

std::unique_ptr<DropStatement> PostgresParser::DropTableTransform(ParseResult *parse_result, DropStmt *root) {
  auto if_exists = root->missing_ok_;

  std::string table_name;
  std::string schema_name;
  auto list = reinterpret_cast<List *>(root->objects_->head->data.ptr_value);
  if (list->length == 2) {
    // List length is 2 when the schema length is specified, e.g. DROP INDEX/TABLE A.B.
    schema_name = reinterpret_cast<value *>(list->head->data.ptr_value)->val_.str_;
    table_name = reinterpret_cast<value *>(list->head->next->data.ptr_value)->val_.str_;
  } else {
    table_name = reinterpret_cast<value *>(list->head->data.ptr_value)->val_.str_;
  }
  auto table_info = std::make_unique<TableInfo>(table_name, schema_name, "");

  auto result = std::make_unique<DropStatement>(std::move(table_info), DropStatement::DropType::kTable, if_exists);
  return result;
}

std::unique_ptr<DeleteStatement> PostgresParser::TruncateTransform(ParseResult *parse_result,
                                                                   TruncateStmt *truncate_stmt) {
  std::unique_ptr<DeleteStatement> result;

  auto cell = truncate_stmt->relations_->head;
  auto table_ref = RangeVarTransform(parse_result, reinterpret_cast<RangeVar *>(cell->data.ptr_value));
  result = std::make_unique<DeleteStatement>(std::move(table_ref));
  // TODO(WAN): This is almost certainly broken. Let's see who hits it first and figure out what's missing.
  return result;
}

std::unique_ptr<DropStatement> PostgresParser::DropTriggerTransform(ParseResult *parse_result, DropStmt *root) {
  auto list = reinterpret_cast<List *>(root->objects_->head->data.ptr_value);

  std::string trigger_name = reinterpret_cast<value *>(list->tail->data.ptr_value)->val_.str_;
  std::string table_name;
  std::string schema_name;

  // TODO(WAN): This is probably incomplete.
  if (list->length == 3) {
    schema_name = reinterpret_cast<value *>(list->head->data.ptr_value)->val_.str_;
    table_name = reinterpret_cast<value *>(list->head->next->data.ptr_value)->val_.str_;
  } else {
    table_name = reinterpret_cast<value *>(list->head->data.ptr_value)->val_.str_;
  }
  auto table_info = std::make_unique<TableInfo>(table_name, schema_name, "");

  auto result = std::make_unique<DropStatement>(std::move(table_info), DropStatement::DropType::kTrigger, trigger_name);
  return result;
}

std::unique_ptr<ExecuteStatement> PostgresParser::ExecuteTransform(ParseResult *parse_result, ExecuteStmt *root) {
  auto name = root->name_;
  auto params = ParamListTransform(parse_result, root->params_);
  auto result = std::make_unique<ExecuteStatement>(name, std::move(params));
  return result;
}

std::vector<common::ManagedPointer<AbstractExpression>> PostgresParser::ParamListTransform(ParseResult *parse_result,
                                                                                           List *root) {
  std::vector<common::ManagedPointer<AbstractExpression>> result;

  if (root == nullptr) {
    return result;
  }

  for (auto cell = root->head; cell != nullptr; cell = cell->next) {
    auto param = reinterpret_cast<Node *>(cell->data.ptr_value);
    switch (param->type) {
      case T_A_Const: {
        auto node = reinterpret_cast<A_Const *>(cell->data.ptr_value);
        auto expr = ConstTransform(parse_result, node);
        result.emplace_back(common::ManagedPointer(expr));
        parse_result->AddExpression(std::move(expr));
        break;
      }
      case T_A_Expr: {
        auto node = reinterpret_cast<A_Expr *>(cell->data.ptr_value);
        auto expr = AExprTransform(parse_result, node);
        result.emplace_back(common::ManagedPointer(expr));
        parse_result->AddExpression(std::move(expr));
        break;
      }
      case T_FuncCall: {
        auto node = reinterpret_cast<FuncCall *>(cell->data.ptr_value);
        auto expr = FuncCallTransform(parse_result, node);
        result.emplace_back(common::ManagedPointer(expr));
        parse_result->AddExpression(std::move(expr));
        break;
      }
      default: {
        PARSER_LOG_AND_THROW("ParamListTransform", "ExpressionType", param->type);
      }
    }
  }

  return result;
}

std::unique_ptr<ExplainStatement> PostgresParser::ExplainTransform(ParseResult *parse_result, ExplainStmt *root) {
  std::unique_ptr<ExplainStatement> result;
  auto query = NodeTransform(parse_result, root->query_);
  result = std::make_unique<ExplainStatement>(std::move(query));
  return result;
}

// Postgres.InsertStmt -> noisepage.InsertStatement
std::unique_ptr<InsertStatement> PostgresParser::InsertTransform(ParseResult *parse_result, InsertStmt *root) {
  NOISEPAGE_ASSERT(root->select_stmt_ != nullptr, "Selects from table or directly selects some values.");

  std::unique_ptr<InsertStatement> result;

  auto column_names = ColumnNameTransform(root->cols_);
  auto table_ref = RangeVarTransform(parse_result, root->relation_);
  auto select_stmt = reinterpret_cast<SelectStmt *>(root->select_stmt_);

  if (select_stmt->from_clause_ != nullptr) {
    // select from a table to insert
    auto select_trans = SelectTransform(parse_result, select_stmt);
    result = std::make_unique<InsertStatement>(std::move(column_names), std::move(table_ref), std::move(select_trans));
  } else {
    // directly insert some values
    NOISEPAGE_ASSERT(select_stmt->values_lists_ != nullptr, "Must have values to insert.");
    auto insert_values = ValueListsTransform(parse_result, select_stmt->values_lists_);
    result = std::make_unique<InsertStatement>(std::move(column_names), std::move(table_ref), std::move(insert_values));
  }

  return result;
}

// Postgres.List -> column names
std::unique_ptr<std::vector<std::string>> PostgresParser::ColumnNameTransform(List *root) {
  auto result = std::make_unique<std::vector<std::string>>();

  if (root == nullptr) {
    return result;
  }

  for (auto cell = root->head; cell != nullptr; cell = cell->next) {
    auto target = reinterpret_cast<ResTarget *>(cell->data.ptr_value);
    result->emplace_back(target->name_);
  }

  return result;
}

// Transforms value lists into noisepage equivalent. Nested vectors, because an InsertStmt may insert multiple tuples.
std::unique_ptr<std::vector<std::vector<common::ManagedPointer<AbstractExpression>>>>
PostgresParser::ValueListsTransform(ParseResult *parse_result, List *root) {
  auto result = std::make_unique<std::vector<std::vector<common::ManagedPointer<AbstractExpression>>>>();

  for (auto value_list = root->head; value_list != nullptr; value_list = value_list->next) {
    std::vector<common::ManagedPointer<AbstractExpression>> cur_result;

    auto target = reinterpret_cast<List *>(value_list->data.ptr_value);
    for (auto cell = target->head; cell != nullptr; cell = cell->next) {
      auto expr_pg = reinterpret_cast<Expr *>(cell->data.ptr_value);
      std::unique_ptr<AbstractExpression> expr;
      switch (expr_pg->type_) {
        case T_ParamRef: {
          expr = ParamRefTransform(parse_result, reinterpret_cast<ParamRef *>(expr_pg));
          break;
        }
        case T_A_Const: {
          expr = ConstTransform(parse_result, reinterpret_cast<A_Const *>(expr_pg));
          break;
        }
        case T_A_Expr: {
          expr = AExprTransform(parse_result, reinterpret_cast<A_Expr *>(expr_pg));
          break;
        }
        case T_TypeCast: {
          expr = TypeCastTransform(parse_result, reinterpret_cast<TypeCast *>(expr_pg));
          break;
        }
        case T_SetToDefault: {
          expr = std::make_unique<DefaultValueExpression>();
          break;
        }
        default: {
          PARSER_LOG_AND_THROW("ValueListsTransform", "Value type", expr_pg->type_);
        }
      }
      cur_result.emplace_back(common::ManagedPointer(expr));
      parse_result->AddExpression(std::move(expr));
    }
    result->emplace_back(std::move(cur_result));
  }

  return result;
}

std::unique_ptr<TransactionStatement> PostgresParser::TransactionTransform(TransactionStmt *transaction_stmt) {
  std::unique_ptr<TransactionStatement> result;

  switch (transaction_stmt->kind_) {
    case TRANS_STMT_BEGIN: {
      result = std::make_unique<TransactionStatement>(TransactionStatement::kBegin);
      break;
    }
    case TRANS_STMT_COMMIT: {
      result = std::make_unique<TransactionStatement>(TransactionStatement::kCommit);
      break;
    }
    case TRANS_STMT_ROLLBACK: {
      result = std::make_unique<TransactionStatement>(TransactionStatement::kRollback);
      break;
    }
    default: {
      PARSER_LOG_AND_THROW("TransactionTransform", "TRANSACTION statement type", transaction_stmt->kind_);
    }
  }
  return result;
}

// Postgres.List -> noisepage.UpdateClause
std::vector<std::unique_ptr<UpdateClause>> PostgresParser::UpdateTargetTransform(ParseResult *parse_result,
                                                                                 List *root) {
  std::vector<std::unique_ptr<UpdateClause>> result;
  for (auto cell = root->head; cell != nullptr; cell = cell->next) {
    auto target = reinterpret_cast<ResTarget *>(cell->data.ptr_value);
    auto column = target->name_;
    auto expr = ExprTransform(parse_result, target->val_, nullptr);
    auto expr_ptr = common::ManagedPointer(expr);
    parse_result->AddExpression(std::move(expr));
    result.emplace_back(std::make_unique<UpdateClause>(column, expr_ptr));
  }
  return result;
}

// Postgres.PrepareStmt -> noisepage.PrepareStatement
std::unique_ptr<PrepareStatement> PostgresParser::PrepareTransform(ParseResult *parse_result, PrepareStmt *root) {
  auto name = root->name_;
  auto query = NodeTransform(parse_result, root->query_);

  // TODO(WAN): This should probably be populated?
  std::vector<common::ManagedPointer<ParameterValueExpression>> placeholders;

  auto result = std::make_unique<PrepareStatement>(name, std::move(query), std::move(placeholders));
  return result;
}

// Postgres.VacuumStmt -> noisepage.AnalyzeStatement
std::unique_ptr<AnalyzeStatement> PostgresParser::VacuumTransform(ParseResult *parse_result, VacuumStmt *root) {
  std::unique_ptr<AnalyzeStatement> result;
  switch (root->options_) {
    case VACOPT_ANALYZE: {
      auto analyze_table = root->relation_ != nullptr ? RangeVarTransform(parse_result, root->relation_) : nullptr;
      auto analyze_columns = ColumnNameTransform(root->va_cols_);
      result = std::make_unique<AnalyzeStatement>(std::move(analyze_table), std::move(analyze_columns));
      break;
    }
    default: {
      PARSER_LOG_AND_THROW("VacuumTransform", "Vacuum", root->options_);
    }
  }

  return result;
}

// Postgres.UpdateStmt -> noisepage.UpdateStatement
std::unique_ptr<UpdateStatement> PostgresParser::UpdateTransform(ParseResult *parse_result, UpdateStmt *update_stmt) {
  std::unique_ptr<UpdateStatement> result;

  auto table = RangeVarTransform(parse_result, update_stmt->relation_);
  auto clauses = UpdateTargetTransform(parse_result, update_stmt->target_list_);
  auto where = WhereTransform(parse_result, update_stmt->where_clause_);

  result = std::make_unique<UpdateStatement>(std::move(table), std::move(clauses), where);
  return result;
}

// Postgres.VariableSetStmt -> noisepage.VariableSetStatement
std::unique_ptr<VariableSetStatement> PostgresParser::VariableSetTransform(ParseResult *parse_result,
                                                                           VariableSetStmt *root) {
  std::string name = root->name_;
  // TODO(WAN): This is an unfortunate hack around the way SET SESSION CHARACTERISTICS comes in.
  std::vector<common::ManagedPointer<AbstractExpression>> values;
  if (name == "SESSION CHARACTERISTICS") {
    auto list_cell = root->args_->head;
    NOISEPAGE_ASSERT(reinterpret_cast<Node *>(list_cell->data.ptr_value)->type == T_DefElem, "Expect a DefElem.");
    NOISEPAGE_ASSERT(list_cell->next == nullptr, "Expect only one argument.");
    auto def_cell = reinterpret_cast<DefElem *>(list_cell->data.ptr_value);
    name = def_cell->defname_;
    auto expr = ConstTransform(parse_result, reinterpret_cast<AConst *>(def_cell->arg_));
    values.emplace_back(common::ManagedPointer(expr));
    parse_result->AddExpression(std::move(expr));
  } else {
    values = ParamListTransform(parse_result, root->args_);
  }
  bool is_set_default = root->kind_ == VariableSetKind::VAR_SET_DEFAULT;
  auto result = std::make_unique<VariableSetStatement>(name, std::move(values), is_set_default);
  return result;
}

// Postgres.VariableShowStmt -> noisepage.VariableShowStatement
std::unique_ptr<VariableShowStatement> PostgresParser::VariableShowTransform(ParseResult *parse_result,
                                                                             VariableShowStmt *root) {
  std::string name = root->name_;
  auto result = std::make_unique<VariableShowStatement>(name);
  return result;
}

}  // namespace noisepage::parser
