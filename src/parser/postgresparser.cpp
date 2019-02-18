#include <algorithm>
#include <cstdio>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/exception.h"

#include "libpg_query/pg_list.h"
#include "libpg_query/pg_query.h"

#include "loggers/parser_logger.h"

#include "parser/expression/aggregate_expression.h"
#include "parser/expression/case_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/parameter_value_expression.h"
#include "parser/expression/star_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/expression/tuple_value_expression.h"
#include "parser/expression/type_cast_expression.h"
#include "parser/pg_trigger.h"
#include "parser/postgresparser.h"
#include "type/transient_value_factory.h"

/**
 * Log information about the error, then throw an exception
 * FN_NAME - name of current function
 * TYPE_MSG - message about the type in error
 * ARG - to print, i.e. unknown or unsupported type
 */
#define PARSER_LOG_AND_THROW(FN_NAME, TYPE_MSG, ARG)           \
  PARSER_LOG_DEBUG(#FN_NAME #TYPE_MSG " {} unsupported", ARG); \
  throw PARSER_EXCEPTION(#FN_NAME ":" #TYPE_MSG " unsupported")

namespace terrier {
namespace parser {

PostgresParser::PostgresParser() = default;

PostgresParser::~PostgresParser() = default;

std::vector<std::unique_ptr<SQLStatement>> PostgresParser::BuildParseTree(const std::string &query_string) {
  auto text = query_string.c_str();
  auto ctx = pg_query_parse_init();
  auto result = pg_query_parse(text);

  if (result.error != nullptr) {
    PARSER_LOG_DEBUG("BuildParseTree error: msg {}, curpos {}", result.error->message, result.error->cursorpos);
    pg_query_parse_finish(ctx);
    pg_query_free_parse_result(result);
    throw PARSER_EXCEPTION("BuildParseTree error");
  }

  std::vector<std::unique_ptr<SQLStatement>> transform_result;
  try {
    transform_result = ListTransform(result.tree);
  } catch (const Exception &e) {
    pg_query_parse_finish(ctx);
    pg_query_free_parse_result(result);
    PARSER_LOG_DEBUG("BuildParseTree: caught {} {} {} {}", e.get_type(), e.get_file(), e.get_line(), e.what());
    throw;
  }

  pg_query_parse_finish(ctx);
  pg_query_free_parse_result(result);
  return transform_result;
}

std::vector<std::unique_ptr<SQLStatement>> PostgresParser::ListTransform(List *root) {
  std::vector<std::unique_ptr<SQLStatement>> result;

  if (root != nullptr) {
    for (auto cell = root->head; cell != nullptr; cell = cell->next) {
      auto node = static_cast<Node *>(cell->data.ptr_value);
      result.emplace_back(NodeTransform(node));
    }
  }

  return result;
}

std::unique_ptr<SQLStatement> PostgresParser::NodeTransform(Node *node) {
  // is this a valid case or is it an error and should throw an exception?
  if (node == nullptr) {
    return nullptr;
  }

  std::unique_ptr<SQLStatement> result = nullptr;
  switch (node->type) {
    case T_CopyStmt: {
      result = CopyTransform(reinterpret_cast<CopyStmt *>(node));
      break;
    }
    case T_CreateStmt: {
      result = CreateTransform(reinterpret_cast<CreateStmt *>(node));
      break;
    }
    case T_CreatedbStmt: {
      result = CreateDatabaseTransform(reinterpret_cast<CreateDatabaseStmt *>(node));
      break;
    }
    case T_CreateFunctionStmt: {
      result = CreateFunctionTransform(reinterpret_cast<CreateFunctionStmt *>(node));
      break;
    }
    case T_CreateSchemaStmt: {
      result = CreateSchemaTransform(reinterpret_cast<CreateSchemaStmt *>(node));
      break;
    }
    case T_CreateTrigStmt: {
      result = CreateTriggerTransform(reinterpret_cast<CreateTrigStmt *>(node));
      break;
    }
    case T_DropdbStmt: {
      result = DropDatabaseTransform(reinterpret_cast<DropDatabaseStmt *>(node));
      break;
    }
    case T_DropStmt: {
      result = DropTransform(reinterpret_cast<DropStmt *>(node));
      break;
    }
    case T_ExecuteStmt: {
      result = ExecuteTransform(reinterpret_cast<ExecuteStmt *>(node));
      break;
    }
    case T_ExplainStmt: {
      result = ExplainTransform(reinterpret_cast<ExplainStmt *>(node));
      break;
    }
    case T_IndexStmt: {
      result = CreateIndexTransform(reinterpret_cast<IndexStmt *>(node));
      break;
    }
    case T_InsertStmt: {
      result = InsertTransform(reinterpret_cast<InsertStmt *>(node));
      break;
    }
    case T_PrepareStmt: {
      result = PrepareTransform(reinterpret_cast<PrepareStmt *>(node));
      break;
    }
    case T_SelectStmt: {
      result = SelectTransform(reinterpret_cast<SelectStmt *>(node));
      break;
    }
    case T_VacuumStmt: {
      result = VacuumTransform(reinterpret_cast<VacuumStmt *>(node));
      break;
    }
    case T_VariableSetStmt: {
      result = VariableSetTransform(reinterpret_cast<VariableSetStmt *>(node));
      break;
    }
    case T_ViewStmt: {
      result = CreateViewTransform(reinterpret_cast<ViewStmt *>(node));
      break;
    }
    case T_TruncateStmt: {
      result = TruncateTransform(reinterpret_cast<TruncateStmt *>(node));
      break;
    }
    case T_TransactionStmt: {
      result = TransactionTransform(reinterpret_cast<TransactionStmt *>(node));
      break;
    }
    case T_UpdateStmt: {
      result = UpdateTransform(reinterpret_cast<UpdateStmt *>(node));
      break;
    }
    case T_DeleteStmt: {
      result = DeleteTransform(reinterpret_cast<DeleteStmt *>(node));
      break;
    }
    default: {
      PARSER_LOG_DEBUG("NodeTransform: statement type {} unsupported", node->type);
      throw PARSER_EXCEPTION("NodeTransform: unsupported statement type");
    }
  }
  return result;
}

std::unique_ptr<AbstractExpression> PostgresParser::ExprTransform(Node *node) {
  if (node == nullptr) {
    return nullptr;
  }

  std::unique_ptr<AbstractExpression> expr = nullptr;
  switch (node->type) {
    case T_A_Const: {
      expr = ConstTransform(reinterpret_cast<A_Const *>(node));
      break;
    }
    case T_A_Expr: {
      expr = AExprTransform(reinterpret_cast<A_Expr *>(node));
      break;
    }
    case T_BoolExpr: {
      expr = BoolExprTransform(reinterpret_cast<BoolExpr *>(node));
      break;
    }
    case T_CaseExpr: {
      expr = CaseExprTransform(reinterpret_cast<CaseExpr *>(node));
      break;
    }
    case T_ColumnRef: {
      expr = ColumnRefTransform(reinterpret_cast<ColumnRef *>(node));
      break;
    }
    case T_FuncCall: {
      expr = FuncCallTransform(reinterpret_cast<FuncCall *>(node));
      break;
    }
    case T_NullTest: {
      expr = NullTestTransform(reinterpret_cast<NullTest *>(node));
      break;
    }
    case T_ParamRef: {
      expr = ParamRefTransform(reinterpret_cast<ParamRef *>(node));
      break;
    }
    case T_SubLink: {
      expr = SubqueryExprTransform(reinterpret_cast<SubLink *>(node));
      break;
    }
    case T_TypeCast: {
      expr = AExprTransform(reinterpret_cast<A_Expr *>(node));
      break;
    }
    default: {
      PARSER_LOG_DEBUG("ExprTransform: type {} unsupported", node->type);
      throw PARSER_EXCEPTION("ExprTransform: unsupported type");
    }
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
  if (str == "AGGREGATE_COUNT_STAR") {
    return ExpressionType::AGGREGATE_COUNT_STAR;
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
  if (str == "SELECT_SUBQUERY") {
    return ExpressionType::SELECT_SUBQUERY;
  }
  if (str == "STAR") {
    return ExpressionType::STAR;
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

// Postgres.A_Expr -> terrier.AbstractExpression
std::unique_ptr<AbstractExpression> PostgresParser::AExprTransform(A_Expr *root) {
  // TODO(WAN): the old system says, need a function to transform strings of ops to peloton exprtype
  // e.g. > to COMPARE_GREATERTHAN
  if (root == nullptr) {
    return nullptr;
  }

  ExpressionType target_type;
  std::vector<std::shared_ptr<AbstractExpression>> children;

  if (root->kind == AEXPR_DISTINCT) {
    target_type = ExpressionType::COMPARE_IS_DISTINCT_FROM;
    children.emplace_back(ExprTransform(root->lexpr));
    children.emplace_back(ExprTransform(root->rexpr));
  } else if (root->kind == AEXPR_OP && root->type == T_TypeCast) {
    target_type = ExpressionType::OPERATOR_CAST;
  } else {
    auto name = (reinterpret_cast<value *>(root->name->head->data.ptr_value))->val.str;
    target_type = StringToExpressionType(name);
    children.emplace_back(ExprTransform(root->lexpr));
    children.emplace_back(ExprTransform(root->rexpr));
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
      return TypeCastTransform(reinterpret_cast<TypeCast *>(root));
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

// Postgres.BoolExpr -> terrier.ConjunctionExpression
std::unique_ptr<AbstractExpression> PostgresParser::BoolExprTransform(BoolExpr *root) {
  std::unique_ptr<AbstractExpression> result;
  std::vector<std::shared_ptr<AbstractExpression>> children;
  for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
    auto node = reinterpret_cast<Node *>(cell->data.ptr_value);
    children.emplace_back(ExprTransform(node));
  }
  switch (root->boolop) {
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
      PARSER_LOG_DEBUG("BoolExprTransform: type {} unsupported", root->boolop);
      throw PARSER_EXCEPTION("BoolExprTransform: unsupported type");
    }
  }

  return result;
}

std::unique_ptr<AbstractExpression> PostgresParser::CaseExprTransform(CaseExpr *root) {
  if (root == nullptr) {
    return nullptr;
  }

  auto arg_expr = ExprTransform(reinterpret_cast<Node *>(root->arg));

  std::vector<CaseExpression::WhenClause> clauses;
  for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
    auto w = reinterpret_cast<CaseWhen *>(cell->data.ptr_value);
    auto when_expr = ExprTransform(reinterpret_cast<Node *>(w->expr));
    auto result_expr = ExprTransform(reinterpret_cast<Node *>(w->result));

    if (arg_expr == nullptr) {
      auto when_clause = CaseExpression::WhenClause{std::move(when_expr), std::move(result_expr)};
      clauses.emplace_back(when_clause);
    } else {
      std::vector<std::shared_ptr<AbstractExpression>> children;
      children.emplace_back(arg_expr->Copy());
      children.emplace_back(std::move(when_expr));
      auto cmp_expr = std::make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(children));
      auto when_clause = CaseExpression::WhenClause{std::move(cmp_expr), std::move(result_expr)};
      clauses.emplace_back(when_clause);
    }
  }

  auto default_expr = ExprTransform(reinterpret_cast<Node *>(root->defresult));
  auto ret_val_type = clauses[0].then->GetReturnValueType();

  auto result = std::make_unique<CaseExpression>(ret_val_type, std::move(clauses), std::move(default_expr));
  return result;
}

// Postgres.ColumnRef -> terrier.TupleValueExpression | terrier.StarExpression
std::unique_ptr<AbstractExpression> PostgresParser::ColumnRefTransform(ColumnRef *root) {
  std::unique_ptr<AbstractExpression> result;
  List *fields = root->fields;
  auto node = reinterpret_cast<Node *>(fields->head->data.ptr_value);
  switch (node->type) {
    case T_String: {
      // TODO(WAN): verify the old system is doing the right thing
      if (fields->length == 1) {
        auto col_name = reinterpret_cast<value *>(node)->val.str;
        result = std::make_unique<TupleValueExpression>(col_name, "");
      } else {
        auto next_node = reinterpret_cast<Node *>(fields->head->next->data.ptr_value);
        auto col_name = reinterpret_cast<value *>(next_node)->val.str;
        auto table_name = reinterpret_cast<value *>(node)->val.str;
        result = std::make_unique<TupleValueExpression>(col_name, table_name);
      }
      break;
    }
    case T_A_Star: {
      result = std::make_unique<StarExpression>();
      break;
    }
    default: {
      PARSER_LOG_DEBUG("ColumnRefTransform: type {} unsupported", node->type);
      throw PARSER_EXCEPTION("ColumnRefTransform: unsupported type");
    }
  }

  return result;
}

// Postgres.A_Const -> terrier.ConstantValueExpression
std::unique_ptr<AbstractExpression> PostgresParser::ConstTransform(A_Const *root) {
  if (root == nullptr) {
    return nullptr;
  }
  return ValueTransform(root->val);
}

// Postgres.FuncCall -> terrier.AbstractExpression
std::unique_ptr<AbstractExpression> PostgresParser::FuncCallTransform(FuncCall *root) {
  // TODO(WAN): change case?
  std::string func_name = reinterpret_cast<value *>(root->funcname->head->data.ptr_value)->val.str;

  std::unique_ptr<AbstractExpression> result;
  if (!IsAggregateFunction(func_name)) {
    // normal functions (built-in functions or UDFs)
    func_name = (reinterpret_cast<value *>(root->funcname->tail->data.ptr_value))->val.str;
    std::vector<std::shared_ptr<AbstractExpression>> children;

    if (root->args != nullptr) {
      for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
        auto expr_node = reinterpret_cast<Node *>(cell->data.ptr_value);
        children.emplace_back(ExprTransform(expr_node));
      }
    }
    result = std::make_unique<FunctionExpression>(func_name.c_str(), type::TypeId::INVALID, std::move(children));
  } else {
    // aggregate function
    auto agg_fun_type = StringToExpressionType("AGGREGATE_" + func_name);
    std::vector<std::shared_ptr<AbstractExpression>> children;
    if (root->agg_star) {
      auto child = std::make_unique<StarExpression>();
      children.emplace_back(std::move(child));
      result = std::make_unique<AggregateExpression>(agg_fun_type, std::move(children), root->agg_distinct);
    } else if (root->args->length < 2) {
      auto expr_node = reinterpret_cast<Node *>(root->args->head->data.ptr_value);
      auto child = ExprTransform(expr_node);
      children.emplace_back(std::move(child));
      result = std::make_unique<AggregateExpression>(agg_fun_type, std::move(children), root->agg_distinct);
    } else {
      PARSER_LOG_DEBUG("FuncCallTransform: Aggregation over multiple cols not supported");
      throw PARSER_EXCEPTION("FuncCallTransform: Aggregation over multiple cols not supported");
    }
  }
  return result;
}

// Postgres.NullTest -> terrier.OperatorExpression
std::unique_ptr<AbstractExpression> PostgresParser::NullTestTransform(NullTest *root) {
  if (root == nullptr) {
    return nullptr;
  }

  std::vector<std::shared_ptr<AbstractExpression>> children;

  switch (root->arg->type) {
    case T_ColumnRef: {
      auto arg_expr = ColumnRefTransform(reinterpret_cast<ColumnRef *>(root->arg));
      children.emplace_back(std::move(arg_expr));
      break;
    }
    case T_A_Const: {
      auto arg_expr = ConstTransform(reinterpret_cast<A_Const *>(root->arg));
      children.emplace_back(std::move(arg_expr));
      break;
    }
    case T_A_Expr: {
      auto arg_expr = AExprTransform(reinterpret_cast<A_Expr *>(root->arg));
      children.emplace_back(std::move(arg_expr));
      break;
    }
    case T_ParamRef: {
      auto arg_expr = ParamRefTransform(reinterpret_cast<ParamRef *>(root->arg));
      children.emplace_back(std::move(arg_expr));
      break;
    }
    default: { PARSER_LOG_AND_THROW("NullTestTransform", "ArgExpr type", root->arg->type); }
  }

  ExpressionType type =
      root->nulltesttype == IS_NULL ? ExpressionType::OPERATOR_IS_NULL : ExpressionType::OPERATOR_IS_NOT_NULL;

  auto result = std::make_unique<OperatorExpression>(type, type::TypeId::BOOLEAN, std::move(children));
  return result;
}

// Postgres.ParamRef -> terrier.ParameterValueExpression
std::unique_ptr<AbstractExpression> PostgresParser::ParamRefTransform(ParamRef *root) {
  auto result = std::make_unique<ParameterValueExpression>(root->number - 1);
  return result;
}

// Postgres.SubLink -> terrier.
std::unique_ptr<AbstractExpression> PostgresParser::SubqueryExprTransform(SubLink *node) {
  if (node == nullptr) {
    return nullptr;
  }

  auto select_stmt = SelectTransform(reinterpret_cast<SelectStmt *>(node->subselect));
  auto subquery_expr = std::make_unique<SubqueryExpression>(std::move(select_stmt));
  std::vector<std::shared_ptr<AbstractExpression>> children;

  std::unique_ptr<AbstractExpression> result;

  switch (node->subLinkType) {
    case ANY_SUBLINK: {
      auto col_expr = ExprTransform(node->testexpr);
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
    default: { PARSER_LOG_AND_THROW("SubqueryExprTransform", "Sublink type", node->subLinkType); }
  }

  return result;
}

// Postgres.TypeCast -> terrier.TypeCastExpression
std::unique_ptr<AbstractExpression> PostgresParser::TypeCastTransform(TypeCast *root) {
  auto type_name = reinterpret_cast<value *>(root->typeName->names->tail->data.ptr_value)->val.str;
  auto type = ColumnDefinition::StrToValueType(type_name);
  std::vector<std::shared_ptr<AbstractExpression>> children;
  children.emplace_back(ExprTransform(root->arg));
  auto result = std::make_unique<TypeCastExpression>(type, std::move(children));
  return result;
}

// Postgres.value -> terrier.ConstantValueExpression
std::unique_ptr<AbstractExpression> PostgresParser::ValueTransform(value val) {
  std::unique_ptr<AbstractExpression> result;
  switch (val.type) {
    case T_Integer: {
      auto v = type::TransientValueFactory::GetInteger(val.val.ival);
      result = std::make_unique<ConstantValueExpression>(v);
      break;
    }

    case T_String: {
      auto v = type::TransientValueFactory::GetVarChar(val.val.str);
      result = std::make_unique<ConstantValueExpression>(v);
      break;
    }

    case T_Float: {
      auto v = type::TransientValueFactory::GetDecimal(std::stod(val.val.str));
      result = std::make_unique<ConstantValueExpression>(v);
      break;
    }

    case T_Null: {
      auto v = type::TransientValueFactory::GetBoolean(false);
      v.SetNull(true);
      result = std::make_unique<ConstantValueExpression>(v);
      break;
    }

    default: { PARSER_LOG_AND_THROW("ValueTransform", "Value type", val.type); }
  }
  return result;
}

std::unique_ptr<SelectStatement> PostgresParser::SelectTransform(SelectStmt *root) {
  std::unique_ptr<SelectStatement> result;

  switch (root->op) {
    case SETOP_NONE: {
      auto target = TargetTransform(root->targetList);
      auto from = FromTransform(root);
      auto select_distinct = root->distinctClause != nullptr;
      auto groupby = GroupByTransform(root->groupClause, root->havingClause);
      auto orderby = OrderByTransform(root->sortClause);
      auto where = WhereTransform(root->whereClause);

      int64_t limit = LimitDescription::NO_LIMIT;
      int64_t offset = LimitDescription::NO_OFFSET;
      if (root->limitCount != nullptr) {
        limit = reinterpret_cast<A_Const *>(root->limitCount)->val.val.ival;
        if (root->limitOffset != nullptr) {
          offset = reinterpret_cast<A_Const *>(root->limitOffset)->val.val.ival;
        }
      }
      auto limit_desc = std::make_unique<LimitDescription>(limit, offset);

      result = std::make_unique<SelectStatement>(std::move(target), select_distinct, std::move(from), std::move(where),
                                                 std::move(groupby), std::move(orderby), std::move(limit_desc));
      break;
    }
    case SETOP_UNION: {
      result = SelectTransform(root->larg);
      result->SetUnionSelect(SelectTransform(root->rarg));
      break;
    }
    default: {
      // TODO(Wan): is Set the right message, or should it be Select?
      PARSER_LOG_AND_THROW("SelectTransform", "Set operation", root->type);
    }
  }

  return result;
}

// Postgres.SelectStmt.whereClause -> terrier.SelectStatement.select_
std::vector<std::shared_ptr<AbstractExpression>> PostgresParser::TargetTransform(List *root) {
  // Postgres parses 'SELECT;' to nullptr
  if (root == nullptr) {
    throw PARSER_EXCEPTION("TargetTransform: root==null.");
  }

  std::vector<std::shared_ptr<AbstractExpression>> result;
  for (auto cell = root->head; cell != nullptr; cell = cell->next) {
    auto target = reinterpret_cast<ResTarget *>(cell->data.ptr_value);
    /*
      TODO(WAN): the heck was this doing here?
      if (target->name != nullptr) {
        expr->alias = target->name;
      }
    */
    result.emplace_back(ExprTransform(target->val));
  }
  return result;
}

// TODO(WAN): doesn't support select from multiple sources, nested queries, various joins
// Postgres.SelectStmt.fromClause -> terrier.TableRef
std::unique_ptr<TableRef> PostgresParser::FromTransform(SelectStmt *select_root) {
  // current code assumes SELECT from one source
  List *root = select_root->fromClause;

  // Postgres parses 'SELECT;' to nullptr
  if (root == nullptr) {
    return nullptr;
  }

  // TODO(WAN): this codepath came from the old system. Can simplify?

  if (root->length > 1) {
    std::vector<std::shared_ptr<TableRef>> refs;
    for (auto cell = root->head; cell != nullptr; cell = cell->next) {
      auto node = reinterpret_cast<Node *>(cell->data.ptr_value);
      switch (node->type) {
        case T_RangeVar: {
          refs.emplace_back(RangeVarTransform(reinterpret_cast<RangeVar *>(node)));
          break;
        }
        case T_RangeSubselect: {
          refs.emplace_back(RangeSubselectTransform(reinterpret_cast<RangeSubselect *>(node)));
          break;
        }
        default: { PARSER_LOG_AND_THROW("FromTransform", "FromType", node->type); }
      }
    }
    auto result = TableRef::CreateTableRefByList(std::move(refs));
    return result;
  }

  std::unique_ptr<TableRef> result = nullptr;
  auto node = reinterpret_cast<Node *>(root->head->data.ptr_value);
  switch (node->type) {
    case T_RangeVar: {
      result = RangeVarTransform(reinterpret_cast<RangeVar *>(node));
      break;
    }
    case T_JoinExpr: {
      auto join = JoinTransform(reinterpret_cast<JoinExpr *>(node));
      if (join != nullptr) {
        result = TableRef::CreateTableRefByJoin(std::move(join));
      }
      break;
    }
    case T_RangeSubselect: {
      result = RangeSubselectTransform(reinterpret_cast<RangeSubselect *>(node));
      break;
    }
    default: { PARSER_LOG_AND_THROW("FromTransform", "FromType", node->type); }
  }

  return result;
}

// Postgres.SelectStmt.groupClause -> terrier.GroupByDescription
std::unique_ptr<GroupByDescription> PostgresParser::GroupByTransform(List *group, Node *having_node) {
  if (group == nullptr) {
    return nullptr;
  }

  std::vector<std::shared_ptr<AbstractExpression>> columns;
  for (auto cell = group->head; cell != nullptr; cell = cell->next) {
    auto temp = reinterpret_cast<Node *>(cell->data.ptr_value);
    columns.emplace_back(ExprTransform(temp));
  }

  // TODO(WAN): old system says, having clauses not implemented, depends on AExprTransform
  std::unique_ptr<AbstractExpression> having = nullptr;
  if (having_node != nullptr) {
    having = ExprTransform(having_node);
  }

  auto result = std::make_unique<GroupByDescription>(std::move(columns), std::move(having));
  return result;
}

// Postgres.SelectStmt.sortClause -> terrier.OrderDescription
std::unique_ptr<OrderByDescription> PostgresParser::OrderByTransform(List *order) {
  if (order == nullptr) {
    return nullptr;
  }

  std::vector<OrderType> types;
  std::vector<std::shared_ptr<AbstractExpression>> exprs;

  for (auto cell = order->head; cell != nullptr; cell = cell->next) {
    auto temp = reinterpret_cast<Node *>(cell->data.ptr_value);
    switch (temp->type) {
      case T_SortBy: {
        auto sort = reinterpret_cast<SortBy *>(temp);

        switch (sort->sortby_dir) {
          case SORTBY_DESC: {
            types.emplace_back(kOrderDesc);
            break;
          }
          case SORTBY_ASC:  // fall through
          case SORTBY_DEFAULT: {
            types.emplace_back(kOrderAsc);
            break;
          }
          default: { PARSER_LOG_AND_THROW("OrderByTransform", "Sortby type", sort->sortby_dir); }
        }

        auto target = sort->node;
        exprs.emplace_back(ExprTransform(target));
        break;
      }
      default: { PARSER_LOG_AND_THROW("OrderByTransform", "OrderBy type", temp->type); }
    }
  }

  auto result = std::make_unique<OrderByDescription>(std::move(types), std::move(exprs));
  return result;
}

// Postgres.SelectStmt.whereClause -> terrier.AbstractExpression
std::unique_ptr<AbstractExpression> PostgresParser::WhereTransform(Node *root) {
  if (root == nullptr) {
    return nullptr;
  }
  return ExprTransform(root);
}

// Postgres.JoinExpr -> terrier.JoinDefinition
std::unique_ptr<JoinDefinition> PostgresParser::JoinTransform(JoinExpr *root) {
  // TODO(WAN): magic number 4?
  if ((root->jointype > 4) || (root->isNatural)) {
    return nullptr;
  }

  JoinType type;
  switch (root->jointype) {
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
    default: { PARSER_LOG_AND_THROW("JoinTransform", "JoinType", root->jointype); }
  }

  std::unique_ptr<TableRef> left;
  switch (root->larg->type) {
    case T_RangeVar: {
      left = RangeVarTransform(reinterpret_cast<RangeVar *>(root->larg));
      break;
    }
    case T_RangeSubselect: {
      left = RangeSubselectTransform(reinterpret_cast<RangeSubselect *>(root->larg));
      break;
    }
    case T_JoinExpr: {
      auto join = JoinTransform(reinterpret_cast<JoinExpr *>(root->larg));
      left = TableRef::CreateTableRefByJoin(std::move(join));
      break;
    }
    default: { PARSER_LOG_AND_THROW("JoinTransform", "JoinArgType", root->larg->type); }
  }

  std::unique_ptr<TableRef> right;
  switch (root->rarg->type) {
    case T_RangeVar: {
      right = RangeVarTransform(reinterpret_cast<RangeVar *>(root->rarg));
      break;
    }
    case T_RangeSubselect: {
      right = RangeSubselectTransform(reinterpret_cast<RangeSubselect *>(root->rarg));
      break;
    }
    case T_JoinExpr: {
      auto join = JoinTransform(reinterpret_cast<JoinExpr *>(root->rarg));
      right = TableRef::CreateTableRefByJoin(std::move(join));
      break;
    }
    default: { PARSER_LOG_AND_THROW("JoinTransform", "Right JoinArgType", root->rarg->type); }
  }

  std::unique_ptr<AbstractExpression> condition;

  // TODO(WAN): quick fix to prevent segfaulting on the following test case
  // SELECT * FROM tab0 AS cor0 CROSS JOIN tab0 AS cor1 WHERE NULL IS NOT NULL;
  // we should figure out how to treat CROSS JOIN properly
  if (root->quals == nullptr) {
    PARSER_LOG_AND_THROW("JoinTransform", "root->quals", nullptr);
  }

  switch (root->quals->type) {
    case T_A_Expr: {
      condition = AExprTransform(reinterpret_cast<A_Expr *>(root->quals));
      break;
    }
    case T_BoolExpr: {
      condition = BoolExprTransform(reinterpret_cast<BoolExpr *>(root->quals));
      break;
    }
    default: { PARSER_LOG_AND_THROW("JoinTransform", "Join condition type", root->quals->type); }
  }

  auto result = std::make_unique<JoinDefinition>(type, std::move(left), std::move(right), std::move(condition));
  return result;
}

std::string PostgresParser::AliasTransform(Alias *root) {
  if (root == nullptr) {
    return "";
  }
  return root->aliasname;
}

// Postgres.RangeVar -> terrier.TableRef
std::unique_ptr<TableRef> PostgresParser::RangeVarTransform(RangeVar *root) {
  auto table_name = root->relname == nullptr ? "" : root->relname;
  auto schema_name = root->schemaname == nullptr ? "" : root->schemaname;
  auto database_name = root->catalogname == nullptr ? "" : root->catalogname;

  auto table_info = std::make_unique<TableInfo>(table_name, schema_name, database_name);
  auto alias = AliasTransform(root->alias);
  auto result = TableRef::CreateTableRefByName(alias, std::move(table_info));
  return result;
}

// Postgres.RangeSubselect -> terrier.TableRef
std::unique_ptr<TableRef> PostgresParser::RangeSubselectTransform(RangeSubselect *root) {
  auto select = SelectTransform(reinterpret_cast<SelectStmt *>(root->subquery));
  if (select == nullptr) {
    return nullptr;
  }
  auto alias = AliasTransform(root->alias);
  auto result = TableRef::CreateTableRefBySelect(alias, std::move(select));
  return result;
}

/*
std::unique_ptr<SQLStatement> PostgresParser::ExplainTransform(ExplainStmt *root) {
  auto real_sql_stmt = NodeTransform(root->query);
  auto result = std::make_unique<ExplainStatement>(std::move(real_sql_stmt));
  return result;
}
 */

std::unique_ptr<CopyStatement> PostgresParser::CopyTransform(CopyStmt *root) {
  static constexpr char kDelimiterTok[] = "delimiter";
  static constexpr char kFormatTok[] = "format";
  static constexpr char kQuoteTok[] = "quote";
  static constexpr char kEscapeTok[] = "escape";

  std::unique_ptr<TableRef> table;
  std::unique_ptr<SelectStatement> select_stmt;
  if (root->relation != nullptr) {
    table = RangeVarTransform(root->relation);
  } else {
    select_stmt = SelectTransform(reinterpret_cast<SelectStmt *>(root->query));
  }

  auto file_path = root->filename != nullptr ? root->filename : "";
  auto is_from = root->is_from;

  char delimiter = ',';
  ExternalFileFormat format = ExternalFileFormat::CSV;
  char quote = '"';
  char escape = '"';
  if (root->options != nullptr) {
    for (ListCell *cell = root->options->head; cell != nullptr; cell = cell->next) {
      auto def_elem = reinterpret_cast<DefElem *>(cell->data.ptr_value);

      if (strncmp(def_elem->defname, kFormatTok, sizeof(kFormatTok)) == 0) {
        auto format_cstr = reinterpret_cast<value *>(def_elem->arg)->val.str;
        // lowercase
        if (strcmp(format_cstr, "csv") == 0) {
          format = ExternalFileFormat::CSV;
        } else if (strcmp(format_cstr, "binary") == 0) {
          format = ExternalFileFormat::BINARY;
        }
      }

      if (strncmp(def_elem->defname, kDelimiterTok, sizeof(kDelimiterTok)) == 0) {
        delimiter = *(reinterpret_cast<value *>(def_elem->arg)->val.str);
      }

      if (strncmp(def_elem->defname, kQuoteTok, sizeof(kQuoteTok)) == 0) {
        quote = *(reinterpret_cast<value *>(def_elem->arg)->val.str);
      }

      if (strncmp(def_elem->defname, kEscapeTok, sizeof(kEscapeTok)) == 0) {
        escape = *(reinterpret_cast<value *>(def_elem->arg)->val.str);
      }
    }
  }

  auto result = std::make_unique<CopyStatement>(std::move(table), std::move(select_stmt), file_path, format, is_from,
                                                delimiter, quote, escape);
  return result;
}

// Postgres.CreateStmt -> terrier.CreateStatement
std::unique_ptr<SQLStatement> PostgresParser::CreateTransform(CreateStmt *root) {
  RangeVar *relation = root->relation;
  auto table_name = relation->relname != nullptr ? relation->relname : "";
  auto schema_name = relation->schemaname != nullptr ? relation->schemaname : "";
  auto database_name = relation->schemaname != nullptr ? relation->catalogname : "";
  std::unique_ptr<TableInfo> table_info = std::make_unique<TableInfo>(table_name, schema_name, database_name);

  std::unordered_set<std::string> primary_keys;

  std::vector<std::shared_ptr<ColumnDefinition>> columns;
  std::vector<std::shared_ptr<ColumnDefinition>> foreign_keys;

  for (auto cell = root->tableElts->head; cell != nullptr; cell = cell->next) {
    auto node = reinterpret_cast<Node *>(cell->data.ptr_value);
    switch (node->type) {
      case T_ColumnDef: {
        auto res = ColumnDefTransform(reinterpret_cast<ColumnDef *>(node));
        columns.emplace_back(std::move(res.col));
        foreign_keys.insert(foreign_keys.end(), std::make_move_iterator(res.fks.begin()),
                            std::make_move_iterator(res.fks.end()));
        break;
      }
      case T_Constraint: {
        auto constraint = reinterpret_cast<Constraint *>(node);
        switch (constraint->contype) {
          case CONSTR_PRIMARY: {
            for (auto key_cell = constraint->keys->head; key_cell != nullptr; key_cell = key_cell->next) {
              primary_keys.emplace(reinterpret_cast<value *>(key_cell->data.ptr_value)->val.str);
            }
            break;
          }
          case CONSTR_FOREIGN: {
            std::vector<std::string> fk_sources;
            for (auto attr_cell = constraint->fk_attrs->head; attr_cell != nullptr; attr_cell = attr_cell->next) {
              auto attr_val = reinterpret_cast<value *>(attr_cell->data.ptr_value);
              fk_sources.emplace_back(attr_val->val.str);
            }

            std::vector<std::string> fk_sinks;
            for (auto attr_cell = constraint->pk_attrs->head; attr_cell != nullptr; attr_cell = attr_cell->next) {
              auto attr_val = reinterpret_cast<value *>(attr_cell->data.ptr_value);
              fk_sinks.emplace_back(attr_val->val.str);
            }

            auto fk_sink_table_name = constraint->pktable->relname;
            auto fk_delete_action = CharToActionType(constraint->fk_del_action);
            auto fk_update_action = CharToActionType(constraint->fk_upd_action);
            auto fk_match_type = CharToMatchType(constraint->fk_matchtype);

            auto fk = std::make_unique<ColumnDefinition>(std::move(fk_sources), std::move(fk_sinks), fk_sink_table_name,
                                                         fk_delete_action, fk_update_action, fk_match_type);

            foreign_keys.emplace_back(std::move(fk));
            break;
          }
          default: {
            PARSER_LOG_DEBUG("CreateTransform: constraint of type {} not supported", constraint->contype);
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

// Postgres.CreateDatabaseStmt -> terrier.CreateStatement
std::unique_ptr<parser::SQLStatement> PostgresParser::CreateDatabaseTransform(CreateDatabaseStmt *root) {
  auto table_info = std::make_unique<TableInfo>("", "", root->dbname);
  std::vector<std::shared_ptr<ColumnDefinition>> columns;
  std::vector<std::shared_ptr<ColumnDefinition>> foreign_keys;
  auto result = std::make_unique<CreateStatement>(std::move(table_info), CreateStatement::kDatabase, std::move(columns),
                                                  std::move(foreign_keys));

  // TODO(WAN): per the old system, more options need to be converted
  // see postgresparser.h and the postgresql docs
  return result;
}

// Postgres.CreateFunctionStmt -> terrier.CreateFunctionStatement
std::unique_ptr<SQLStatement> PostgresParser::CreateFunctionTransform(CreateFunctionStmt *root) {
  bool replace = root->replace;
  std::vector<std::shared_ptr<FuncParameter>> func_parameters;

  for (auto cell = root->parameters->head; cell != nullptr; cell = cell->next) {
    auto node = reinterpret_cast<Node *>(cell->data.ptr_value);
    switch (node->type) {
      case T_FunctionParameter: {
        func_parameters.emplace_back(FunctionParameterTransform(reinterpret_cast<FunctionParameter *>(node)));
        break;
      }
      default: {
        // TODO(WAN): previous code just ignored it, is this right?
        break;
      }
    }
  }

  auto return_type = ReturnTypeTransform(reinterpret_cast<TypeName *>(root->returnType));

  // TODO(WAN): assumption from old code, can only pass one function name for now
  std::string func_name = (reinterpret_cast<value *>(root->funcname->tail->data.ptr_value)->val.str);

  std::vector<std::string> func_body;
  AsType as_type = AsType::INVALID;
  PLType pl_type = PLType::INVALID;

  for (auto cell = root->options->head; cell != nullptr; cell = cell->next) {
    auto def_elem = reinterpret_cast<DefElem *>(cell->data.ptr_value);
    if (strcmp(def_elem->defname, "as") == 0) {
      auto list_of_arg = reinterpret_cast<List *>(def_elem->arg);

      for (auto cell2 = list_of_arg->head; cell2 != nullptr; cell2 = cell2->next) {
        std::string query_string = reinterpret_cast<value *>(cell2->data.ptr_value)->val.str;
        func_body.push_back(query_string);
      }

      if (func_body.size() > 1) {
        as_type = AsType::EXECUTABLE;
      } else {
        as_type = AsType::QUERY_STRING;
      }
    } else if (strcmp(def_elem->defname, "language") == 0) {
      auto lang = reinterpret_cast<value *>(def_elem->arg)->val.str;
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

// Postgres.IndexStmt -> terrier.CreateStatement
std::unique_ptr<SQLStatement> PostgresParser::CreateIndexTransform(IndexStmt *root) {
  auto unique = root->unique;
  auto index_name = root->idxname;

  std::vector<std::string> index_attrs;
  for (auto cell = root->indexParams->head; cell != nullptr; cell = cell->next) {
    std::string index_attr = reinterpret_cast<IndexElem *>(cell->data.ptr_value)->name;
    index_attrs.emplace_back(index_attr);
  }

  auto table_name = root->relation->relname == nullptr ? "" : root->relation->relname;
  auto schema_name = root->relation->schemaname == nullptr ? "" : root->relation->schemaname;
  auto database_name = root->relation->catalogname == nullptr ? "" : root->relation->catalogname;
  auto table_info = std::make_unique<TableInfo>(table_name, schema_name, database_name);

  char *access_method = root->accessMethod;
  IndexType index_type;
  // TODO(WAN): do we need to do case conversion?
  if (strcmp(access_method, "invalid") == 0) {
    index_type = IndexType::INVALID;
  } else if ((strcmp(access_method, "btree") == 0) || (strcmp(access_method, "bwtree") == 0)) {
    index_type = IndexType::BWTREE;
  } else if (strcmp(access_method, "hash") == 0) {
    index_type = IndexType::HASH;
  } else if (strcmp(access_method, "skiplist") == 0) {
    index_type = IndexType::SKIPLIST;
  } else if (strcmp(access_method, "art") == 0) {
    index_type = IndexType::ART;
  } else {
    PARSER_LOG_DEBUG("CreateIndexTransform: IndexType {} not supported", access_method);
    throw NOT_IMPLEMENTED_EXCEPTION("CreateIndexTransform error");
  }

  auto result =
      std::make_unique<CreateStatement>(std::move(table_info), index_type, unique, index_name, std::move(index_attrs));
  return result;
}

// Postgres.CreateSchemaStmt -> terrier.CreateStatement
std::unique_ptr<SQLStatement> PostgresParser::CreateSchemaTransform(CreateSchemaStmt *root) {
  std::string schema_name;
  if (root->schemaname != nullptr) {
    schema_name = root->schemaname;
  } else {
    TERRIER_ASSERT(root->authrole != nullptr, "We need a schema name.");
    switch (root->authrole->type) {
      case T_RoleSpec: {
        // TODO(WAN): old system said they didn't need the authrole.. not sure if that's true
        auto authrole = reinterpret_cast<RoleSpec *>(root->authrole);
        schema_name = authrole->rolename;
        break;
      }
      default: { PARSER_LOG_AND_THROW("CreateSchemaTransform", "AuthRole", root->authrole->type); }
    }
  }

  auto table_info = std::make_unique<TableInfo>("", schema_name, "");
  auto if_not_exists = root->if_not_exists;

  // TODO(WAN): the old system basically didn't implement any of this

  if (root->schemaElts != nullptr) {
    PARSER_LOG_DEBUG("CreateSchemaTransform schema_element unsupported");
    throw PARSER_EXCEPTION("CreateSchemaTransform schema_element unsupported");
  }

  auto result = std::make_unique<CreateStatement>(std::move(table_info), if_not_exists);
  return result;
}

// Postgres.CreateTrigStmt -> terrier.CreateStatement
std::unique_ptr<SQLStatement> PostgresParser::CreateTriggerTransform(CreateTrigStmt *root) {
  auto table_name = root->relation->relname == nullptr ? "" : root->relation->relname;
  auto schema_name = root->relation->schemaname == nullptr ? "" : root->relation->schemaname;
  auto database_name = root->relation->catalogname == nullptr ? "" : root->relation->catalogname;
  auto table_info = std::make_unique<TableInfo>(table_name, schema_name, database_name);

  auto trigger_name = root->trigname;

  std::vector<std::string> trigger_funcnames;
  if (root->funcname != nullptr) {
    for (auto cell = root->funcname->head; cell != nullptr; cell = cell->next) {
      std::string name = reinterpret_cast<value *>(cell->data.ptr_value)->val.str;
      trigger_funcnames.emplace_back(name);
    }
  }

  std::vector<std::string> trigger_args;
  if (root->args != nullptr) {
    for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
      std::string arg = (reinterpret_cast<value *>(cell->data.ptr_value))->val.str;
      trigger_args.push_back(arg);
    }
  }

  std::vector<std::string> trigger_columns;
  if (root->columns != nullptr) {
    for (auto cell = root->columns->head; cell != nullptr; cell = cell->next) {
      std::string column = (reinterpret_cast<value *>(cell->data.ptr_value))->val.str;
      trigger_columns.push_back(column);
    }
  }

  auto trigger_when = WhenTransform(root->whenClause);

  // TODO(WAN): what is this doing?
  int16_t trigger_type = 0;
  TRIGGER_CLEAR_TYPE(trigger_type);
  if (root->row) {
    TRIGGER_SETT_ROW(trigger_type);
  }
  trigger_type |= root->timing;
  trigger_type |= root->events;

  auto result = std::make_unique<CreateStatement>(std::move(table_info), trigger_name, std::move(trigger_funcnames),
                                                  std::move(trigger_args), std::move(trigger_columns),
                                                  std::move(trigger_when), trigger_type);
  return result;
}

// Postgres.ViewStmt -> terrier.CreateStatement
std::unique_ptr<SQLStatement> PostgresParser::CreateViewTransform(ViewStmt *root) {
  auto view_name = root->view->relname;

  std::unique_ptr<SelectStatement> view_query;
  switch (root->query->type) {
    case T_SelectStmt: {
      view_query = SelectTransform(reinterpret_cast<SelectStmt *>(root->query));
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

// Postgres.ColumnDef -> terrier.ColumnDefinition
PostgresParser::ColumnDefTransResult PostgresParser::ColumnDefTransform(ColumnDef *root) {
  auto type_name = root->typeName;

  // handle varlen
  size_t varlen = 0;
  if (type_name->typmods != nullptr) {
    auto node = reinterpret_cast<Node *>(type_name->typmods->head->data.ptr_value);
    switch (node->type) {
      case T_A_Const: {
        auto node_type = reinterpret_cast<A_Const *>(node)->val.type;
        switch (node_type) {
          case T_Integer: {
            varlen = static_cast<size_t>(reinterpret_cast<A_Const *>(node)->val.val.ival);
            break;
          }
          default: { PARSER_LOG_AND_THROW("ColumnDefTransform", "typmods", node_type); }
        }
        break;
      }
      default: { PARSER_LOG_AND_THROW("ColumnDefTransform", "typmods", node->type); }
    }
  }

  auto datatype_name = reinterpret_cast<value *>(type_name->names->tail->data.ptr_value)->val.str;
  auto datatype = ColumnDefinition::StrToDataType(datatype_name);

  std::vector<std::shared_ptr<ColumnDefinition>> foreign_keys;

  bool is_primary = false;
  bool is_not_null = false;
  bool is_unique = false;
  std::unique_ptr<AbstractExpression> default_expr;
  std::unique_ptr<AbstractExpression> check_expr;

  if (root->constraints != nullptr) {
    for (auto cell = root->constraints->head; cell != nullptr; cell = cell->next) {
      auto constraint = reinterpret_cast<Constraint *>(cell->data.ptr_value);
      switch (constraint->contype) {
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

          if (constraint->pk_attrs == nullptr) {
            throw NOT_IMPLEMENTED_EXCEPTION("Foreign key columns unspecified");
          }

          auto attr_cell = constraint->pk_attrs->head;
          auto attr_val = reinterpret_cast<value *>(attr_cell->data.ptr_value);
          fk_sinks.emplace_back(attr_val->val.str);
          fk_sources.emplace_back(root->colname);

          auto fk_sink_table_name = constraint->pktable->relname;
          auto fk_delete_action = CharToActionType(constraint->fk_del_action);
          auto fk_update_action = CharToActionType(constraint->fk_upd_action);
          auto fk_match_type = CharToMatchType(constraint->fk_matchtype);

          auto coldef =
              std::make_unique<ColumnDefinition>(std::move(fk_sources), std::move(fk_sinks), fk_sink_table_name,
                                                 fk_delete_action, fk_update_action, fk_match_type);

          foreign_keys.emplace_back(std::move(coldef));
          break;
        }
        case CONSTR_DEFAULT: {
          default_expr = ExprTransform(constraint->raw_expr);
          break;
        }
        case CONSTR_CHECK: {
          check_expr = ExprTransform(constraint->raw_expr);
          break;
        }
        default: { PARSER_LOG_AND_THROW("ColumnDefTransform", "Constraint", constraint->contype); }
      }
    }
  }

  auto name = root->colname;
  auto result = std::make_unique<ColumnDefinition>(name, datatype, is_primary, is_not_null, is_unique,
                                                   std::move(default_expr), std::move(check_expr), varlen);

  return {std::move(result), std::move(foreign_keys)};
}

// Postgres.FunctionParameter -> terrier.FuncParameter
std::unique_ptr<FuncParameter> PostgresParser::FunctionParameterTransform(FunctionParameter *root) {
  // TODO(WAN): significant code duplication, refactor out char* -> DataType
  char *name = (reinterpret_cast<value *>(root->argType->names->tail->data.ptr_value)->val.str);
  parser::FuncParameter::DataType data_type;

  if ((strcmp(name, "int") == 0) || (strcmp(name, "int4") == 0)) {
    data_type = Parameter::DataType::INT;
  } else if (strcmp(name, "varchar") == 0) {
    data_type = Parameter::DataType::VARCHAR;
  } else if (strcmp(name, "int8") == 0) {
    data_type = Parameter::DataType::BIGINT;
  } else if (strcmp(name, "int2") == 0) {
    data_type = Parameter::DataType::SMALLINT;
  } else if ((strcmp(name, "double") == 0) || (strcmp(name, "float8") == 0)) {
    data_type = Parameter::DataType::DOUBLE;
  } else if ((strcmp(name, "real") == 0) || (strcmp(name, "float4") == 0)) {
    data_type = Parameter::DataType::FLOAT;
  } else if (strcmp(name, "text") == 0) {
    data_type = Parameter::DataType::TEXT;
  } else if (strcmp(name, "bpchar") == 0) {
    data_type = Parameter::DataType::CHAR;
  } else if (strcmp(name, "tinyint") == 0) {
    data_type = Parameter::DataType::TINYINT;
  } else if (strcmp(name, "bool") == 0) {
    data_type = Parameter::DataType::BOOL;
  } else {
    PARSER_LOG_AND_THROW("FunctionParameterTransform", "DataType", name);
  }

  auto param_name = root->name != nullptr ? root->name : "";
  auto result = std::make_unique<FuncParameter>(data_type, param_name);
  return result;
}

// Postgres.TypeName -> terrier.ReturnType
std::unique_ptr<ReturnType> PostgresParser::ReturnTypeTransform(TypeName *root) {
  char *name = (reinterpret_cast<value *>(root->names->tail->data.ptr_value)->val.str);
  ReturnType::DataType data_type;

  if ((strcmp(name, "int") == 0) || (strcmp(name, "int4") == 0)) {
    data_type = Parameter::DataType::INT;
  } else if (strcmp(name, "varchar") == 0) {
    data_type = Parameter::DataType::VARCHAR;
  } else if (strcmp(name, "int8") == 0) {
    data_type = Parameter::DataType::BIGINT;
  } else if (strcmp(name, "int2") == 0) {
    data_type = Parameter::DataType::SMALLINT;
  } else if ((strcmp(name, "double") == 0) || (strcmp(name, "float8") == 0)) {
    data_type = Parameter::DataType::DOUBLE;
  } else if ((strcmp(name, "real") == 0) || (strcmp(name, "float4") == 0)) {
    data_type = Parameter::DataType::FLOAT;
  } else if (strcmp(name, "text") == 0) {
    data_type = Parameter::DataType::TEXT;
  } else if (strcmp(name, "bpchar") == 0) {
    data_type = Parameter::DataType::CHAR;
  } else if (strcmp(name, "tinyint") == 0) {
    data_type = Parameter::DataType::TINYINT;
  } else if (strcmp(name, "bool") == 0) {
    data_type = Parameter::DataType::BOOL;
  } else {
    PARSER_LOG_AND_THROW("ReturnTypeTransform", "ReturnType", name);
  }

  auto result = std::make_unique<ReturnType>(data_type);
  return result;
}

// Postgres.Node -> terrier.AbstractExpression
std::unique_ptr<AbstractExpression> PostgresParser::WhenTransform(Node *root) {
  if (root == nullptr) {
    return nullptr;
  }
  std::unique_ptr<AbstractExpression> result;
  switch (root->type) {
    case T_A_Expr: {
      result = AExprTransform(reinterpret_cast<A_Expr *>(root));
      break;
    }
    case T_BoolExpr: {
      result = BoolExprTransform(reinterpret_cast<BoolExpr *>(root));
      break;
    }
    default: { PARSER_LOG_AND_THROW("WhenTransform", "WHEN type", root->type); }
  }
  return result;
}

std::unique_ptr<DeleteStatement> PostgresParser::DeleteTransform(DeleteStmt *root) {
  std::unique_ptr<DeleteStatement> result;
  auto table = RangeVarTransform(root->relation);
  auto where = WhereTransform(root->whereClause);
  result = std::make_unique<DeleteStatement>(std::move(table), std::move(where));
  return result;
}

// Postgres.DropStmt -> terrier.DropStatement
std::unique_ptr<DropStatement> PostgresParser::DropTransform(DropStmt *root) {
  switch (root->removeType) {
    case ObjectType::OBJECT_INDEX: {
      return DropIndexTransform(root);
    }
    case ObjectType::OBJECT_SCHEMA: {
      return DropSchemaTransform(root);
    }
    case ObjectType::OBJECT_TABLE: {
      return DropTableTransform(root);
    }
    case ObjectType::OBJECT_TRIGGER: {
      return DropTriggerTransform(root);
    }
    default: { PARSER_LOG_AND_THROW("DropTransform", "Drop ObjectType", root->removeType); }
  }
}

// Postgres.DropDatabaseStmt -> terrier.DropStmt
std::unique_ptr<DropStatement> PostgresParser::DropDatabaseTransform(DropDatabaseStmt *root) {
  auto table_info = std::make_unique<TableInfo>("", "", root->dbname);
  auto if_exists = root->missing_ok;

  auto result = std::make_unique<DropStatement>(std::move(table_info), DropStatement::DropType::kDatabase, if_exists);
  return result;
}

// Postgres.DropStmt -> terrier.DropStatement
std::unique_ptr<DropStatement> PostgresParser::DropIndexTransform(DropStmt *root) {
  // TODO(WAN): old system wanted to implement other options for drop index

  std::string schema_name;
  std::string index_name;
  auto list = reinterpret_cast<List *>(root->objects->head->data.ptr_value);
  if (list->length == 2) {
    // list length is 2 when schema length is specified, e.g. DROP INDEX/TABLE A.B
    schema_name = reinterpret_cast<value *>(list->head->data.ptr_value)->val.str;
    index_name = reinterpret_cast<value *>(list->head->next->data.ptr_value)->val.str;
  } else {
    index_name = reinterpret_cast<value *>(list->head->data.ptr_value)->val.str;
  }

  auto table_info = std::make_unique<TableInfo>("", schema_name, "");
  auto result = std::make_unique<DropStatement>(std::move(table_info), index_name);
  return result;
}

std::unique_ptr<DropStatement> PostgresParser::DropSchemaTransform(DropStmt *root) {
  auto if_exists = root->missing_ok;
  auto cascade = root->behavior == DropBehavior::DROP_CASCADE;

  std::string schema_name;
  for (auto cell = root->objects->head; cell != nullptr; cell = cell->next) {
    auto table_list = reinterpret_cast<List *>(cell->data.ptr_value);
    schema_name = reinterpret_cast<value *>(table_list->head->data.ptr_value)->val.str;
    break;
  }

  auto table_info = std::make_unique<TableInfo>("", schema_name, "");
  auto result = std::make_unique<DropStatement>(std::move(table_info), if_exists, cascade);
  return result;
}

std::unique_ptr<DropStatement> PostgresParser::DropTableTransform(DropStmt *root) {
  auto if_exists = root->missing_ok;

  std::string table_name;
  std::string schema_name;
  auto list = reinterpret_cast<List *>(root->objects->head->data.ptr_value);
  if (list->length == 2) {
    // list length is 2 when schema length is specified, e.g. DROP INDEX/TABLE A.B
    schema_name = reinterpret_cast<value *>(list->head->data.ptr_value)->val.str;
    table_name = reinterpret_cast<value *>(list->head->next->data.ptr_value)->val.str;
  } else {
    table_name = reinterpret_cast<value *>(list->head->data.ptr_value)->val.str;
  }
  auto table_info = std::make_unique<TableInfo>(table_name, schema_name, "");

  auto result = std::make_unique<DropStatement>(std::move(table_info), DropStatement::DropType::kTable, if_exists);
  return result;
}

// TODO(pakhtar/wan): delete or find right merge location
// was part of original DropIndexTransform... not needed in new code?
//    result->SetIndexName(
// reinterpret_cast<value *>(list->head->data.ptr_value)->val.str);
// }
//   return result;
// }

std::unique_ptr<DeleteStatement> PostgresParser::TruncateTransform(TruncateStmt *truncate_stmt) {
  std::unique_ptr<DeleteStatement> result;

  // TERRIER_ASSERT(truncate_stmt->relations->length == 1, "Single table only");

  auto cell = truncate_stmt->relations->head;
  auto table_ref = RangeVarTransform(reinterpret_cast<RangeVar *>(cell->data.ptr_value));
  result = std::make_unique<DeleteStatement>(std::move(table_ref));

  /* TODO(WAN): review
   * AFAIK the target is a single table.
   * The code below walks a list but only the last item will be saved. Either the list walk is unnecessary,
   * or the results produced are wrong, and should be a vector.
   */

  /*
  for (auto cell = truncate_stmt->relations->head; cell != nullptr; cell = cell->next) {
    auto table_ref = RangeVarTransform(reinterpret_cast<RangeVar *>(cell->data.ptr_value));

    //result->table_ref_.reset(
    //    RangeVarTransform(reinterpret_cast<RangeVar *>(cell->data.ptr_value)));
    break;
  }
   */
  // TODO(pakhtar/wan)  - fix
  return result;
}

std::unique_ptr<DropStatement> PostgresParser::DropTriggerTransform(DropStmt *root) {
  auto list = reinterpret_cast<List *>(root->objects->head->data.ptr_value);
  std::string trigger_name = reinterpret_cast<value *>(list->tail->data.ptr_value)->val.str;

  std::string table_name;
  std::string schema_name;
  // TODO(WAN): I suspect the old code is wrong and/or incomplete
  if (list->length == 3) {
    schema_name = reinterpret_cast<value *>(list->head->data.ptr_value)->val.str;
    table_name = reinterpret_cast<value *>(list->head->next->data.ptr_value)->val.str;
  } else {
    table_name = reinterpret_cast<value *>(list->head->data.ptr_value)->val.str;
  }
  auto table_info = std::make_unique<TableInfo>(table_name, schema_name, "");

  auto result = std::make_unique<DropStatement>(std::move(table_info), DropStatement::DropType::kTrigger, trigger_name);
  return result;
}

std::unique_ptr<ExecuteStatement> PostgresParser::ExecuteTransform(ExecuteStmt *root) {
  auto name = root->name;
  auto params = ParamListTransform(root->params);
  auto result = std::make_unique<ExecuteStatement>(name, std::move(params));
  return result;
}

std::vector<std::shared_ptr<AbstractExpression>> PostgresParser::ParamListTransform(List *root) {
  std::vector<std::shared_ptr<AbstractExpression>> result;

  if (root == nullptr) {
    return result;
  }

  for (auto cell = root->head; cell != nullptr; cell = cell->next) {
    auto param = reinterpret_cast<Node *>(cell->data.ptr_value);
    switch (param->type) {
      case T_A_Const: {
        auto node = reinterpret_cast<A_Const *>(cell->data.ptr_value);
        result.emplace_back(ConstTransform(node));
        break;
      }
      case T_A_Expr: {
        auto node = reinterpret_cast<A_Expr *>(cell->data.ptr_value);
        result.emplace_back(AExprTransform(node));
        break;
      }
      case T_FuncCall: {
        auto node = reinterpret_cast<FuncCall *>(cell->data.ptr_value);
        result.emplace_back(FuncCallTransform(node));
        break;
      }
      default: { PARSER_LOG_AND_THROW("ParamListTransform", "ExpressionType", param->type); }
    }
  }

  return result;
}

std::unique_ptr<ExplainStatement> PostgresParser::ExplainTransform(ExplainStmt *root) {
  std::unique_ptr<ExplainStatement> result;
  auto query = NodeTransform(root->query);
  result = std::make_unique<ExplainStatement>(std::move(query));
  return result;
}

// Postgres.InsertStmt -> terrier.InsertStatement
std::unique_ptr<InsertStatement> PostgresParser::InsertTransform(InsertStmt *root) {
  TERRIER_ASSERT(root->selectStmt != nullptr, "Selects from table or directly selects some values.");

  std::unique_ptr<InsertStatement> result;

  auto column_names = ColumnNameTransform(root->cols);
  auto table_ref = RangeVarTransform(root->relation);
  auto select_stmt = reinterpret_cast<SelectStmt *>(root->selectStmt);

  if (select_stmt->fromClause != nullptr) {
    // select from a table to insert
    auto select_trans = SelectTransform(select_stmt);
    result = std::make_unique<InsertStatement>(std::move(column_names), std::move(table_ref), std::move(select_trans));
  } else {
    // directly insert some values
    TERRIER_ASSERT(select_stmt->valuesLists != nullptr, "Must have values to insert.");
    auto insert_values = ValueListsTransform(select_stmt->valuesLists);
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
    result->push_back(target->name);
  }

  return result;
}

// Transforms value lists into terrier equivalent. Nested vectors, because an InsertStmt may insert multiple tuples.
std::unique_ptr<std::vector<std::vector<std::shared_ptr<AbstractExpression>>>> PostgresParser::ValueListsTransform(
    List *root) {
  auto result = std::make_unique<std::vector<std::vector<std::shared_ptr<AbstractExpression>>>>();

  for (auto value_list = root->head; value_list != nullptr; value_list = value_list->next) {
    std::vector<std::shared_ptr<AbstractExpression>> cur_result;

    auto target = reinterpret_cast<List *>(value_list->data.ptr_value);
    for (auto cell = target->head; cell != nullptr; cell = cell->next) {
      auto expr = reinterpret_cast<Expr *>(cell->data.ptr_value);
      switch (expr->type) {
        case T_ParamRef: {
          cur_result.emplace_back(ParamRefTransform(reinterpret_cast<ParamRef *>(expr)));
          break;
        }
        case T_A_Const: {
          cur_result.emplace_back(ConstTransform(reinterpret_cast<A_Const *>(expr)));
          break;
        }
        case T_TypeCast: {
          cur_result.emplace_back(TypeCastTransform(reinterpret_cast<TypeCast *>(expr)));
          break;
        }
        /*
         * case T_SetToDefault: {
         * TODO(WAN): old system says it wanted to add corresponding expr for default to cur_reuslt
         */
        default: { PARSER_LOG_AND_THROW("ValueListsTransform", "Value type", expr->type); }
      }
    }
    result->emplace_back(std::move(cur_result));
  }

  return result;
}

std::unique_ptr<TransactionStatement> PostgresParser::TransactionTransform(TransactionStmt *transaction_stmt) {
  std::unique_ptr<TransactionStatement> result;

  switch (transaction_stmt->kind) {
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
    default: { PARSER_LOG_AND_THROW("TransactionTransform", "TRANSACTION statement type", transaction_stmt->kind); }
  }
  return result;
}

std::vector<std::shared_ptr<parser::UpdateClause>> PostgresParser::UpdateTargetTransform(List *root) {
  std::vector<std::shared_ptr<parser::UpdateClause>> result;
  for (auto cell = root->head; cell != nullptr; cell = cell->next) {
    auto target = reinterpret_cast<ResTarget *>(cell->data.ptr_value);
    auto column = target->name;
    auto value = ExprTransform(target->val);
    result.push_back(std::make_shared<UpdateClause>(column, std::move(value)));
  }
  return result;
}

std::unique_ptr<PrepareStatement> PostgresParser::PrepareTransform(PrepareStmt *root) {
  auto name = root->name;
  auto query = NodeTransform(root->query);

  // TODO(WAN): why isn't this populated?
  std::vector<std::shared_ptr<ParameterValueExpression>> placeholders;

  auto result = std::make_unique<PrepareStatement>(name, std::move(query), std::move(placeholders));
  return result;
}

// Postgres.VacuumStmt -> terrier.AnalyzeStatement
std::unique_ptr<AnalyzeStatement> PostgresParser::VacuumTransform(VacuumStmt *root) {
  std::unique_ptr<AnalyzeStatement> result;
  switch (root->options) {
    case VACOPT_ANALYZE: {
      auto analyze_table = root->relation != nullptr ? RangeVarTransform(root->relation) : nullptr;
      auto analyze_columns = ColumnNameTransform(root->va_cols);
      result = std::make_unique<AnalyzeStatement>(std::move(analyze_table), std::move(analyze_columns));
      break;
    }
    default: { PARSER_LOG_AND_THROW("VacuumTransform", "Vacuum", root->options); }
  }

  return result;
}

std::unique_ptr<UpdateStatement> PostgresParser::UpdateTransform(UpdateStmt *update_stmt) {
  std::unique_ptr<UpdateStatement> result;

  auto table = RangeVarTransform(update_stmt->relation);
  auto clauses = UpdateTargetTransform(update_stmt->targetList);
  auto where = WhereTransform(update_stmt->whereClause);

  result = std::make_unique<UpdateStatement>(std::move(table), std::move(clauses), std::move(where));
  return result;
}

// This one is a little weird. The old system had it as a JDBC hack.
std::unique_ptr<VariableSetStatement> PostgresParser::VariableSetTransform(UNUSED_ATTRIBUTE VariableSetStmt *root) {
  auto result = std::make_unique<VariableSetStatement>();
  return result;
}

}  // namespace parser
}  // namespace terrier
