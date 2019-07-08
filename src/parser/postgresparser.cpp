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

ParseResult PostgresParser::BuildParseTree(const std::string &query_string) {
  auto text = query_string.c_str();
  auto ctx = pg_query_parse_init();
  auto result = pg_query_parse(text);

  if (result.error != nullptr) {
    PARSER_LOG_DEBUG("BuildParseTree error: msg {}, curpos {}", result.error->message, result.error->cursorpos);
    pg_query_parse_finish(ctx);
    pg_query_free_parse_result(result);
    throw PARSER_EXCEPTION("BuildParseTree error");
  }

  ParseResult transform_result;
  try {
    transform_result = ListTransform(result.tree);
  } catch (const Exception &e) {
    pg_query_parse_finish(ctx);
    pg_query_free_parse_result(result);
    // TODO(WAN): We should also clean up our parse result here
    PARSER_LOG_DEBUG("BuildParseTree: caught {} {} {} {}", e.get_type(), e.get_file(), e.get_line(), e.what());
    throw;
  }

  pg_query_parse_finish(ctx);
  pg_query_free_parse_result(result);
  return transform_result;
}

ParseResult PostgresParser::ListTransform(List *root) {
  ParseResult result;
  // TODO(WAN): good defaults for reserve? also look into having less std::vector's

  if (root != nullptr) {
    for (auto cell = root->head; cell != nullptr; cell = cell->next) {
      auto node = static_cast<Node *>(cell->data.ptr_value);
      NodeTransform(&result, true, node);
    }
  }

  return result;
}

common::ManagedPointer<SQLStatement> PostgresParser::NodeTransform(ParseResult *result, bool is_top_level, Node *node) {
  // TODO(WAN): if this is a valid case, what SQL produces it? Otherwise, exception?
  if (node == nullptr) {
    return common::ManagedPointer<SQLStatement>(nullptr);
  }

  common::ManagedPointer<SQLStatement> stmt;
  switch (node->type) {
    case T_CopyStmt: {
      stmt = CopyTransform(result, is_top_level, reinterpret_cast<CopyStmt *>(node));
      break;
    }
    case T_CreateStmt: {
      stmt = CreateTransform(result, is_top_level, reinterpret_cast<CreateStmt *>(node));
      break;
    }
    case T_CreatedbStmt: {
      stmt = CreateDatabaseTransform(result, is_top_level, reinterpret_cast<CreateDatabaseStmt *>(node));
      break;
    }
    case T_CreateFunctionStmt: {
      stmt = CreateFunctionTransform(result, is_top_level, reinterpret_cast<CreateFunctionStmt *>(node));
      break;
    }
    case T_CreateSchemaStmt: {
      stmt = CreateSchemaTransform(result, is_top_level, reinterpret_cast<CreateSchemaStmt *>(node));
      break;
    }
    case T_CreateTrigStmt: {
      stmt = CreateTriggerTransform(result, is_top_level, reinterpret_cast<CreateTrigStmt *>(node));
      break;
    }
    case T_DropdbStmt: {
      stmt = DropDatabaseTransform(result, is_top_level, reinterpret_cast<DropDatabaseStmt *>(node));
      break;
    }
    case T_DropStmt: {
      stmt = DropTransform(result, is_top_level, reinterpret_cast<DropStmt *>(node));
      break;
    }
    case T_ExecuteStmt: {
      stmt = ExecuteTransform(result, is_top_level, reinterpret_cast<ExecuteStmt *>(node));
      break;
    }
    case T_ExplainStmt: {
      stmt = ExplainTransform(result, is_top_level, reinterpret_cast<ExplainStmt *>(node));
      break;
    }
    case T_IndexStmt: {
      stmt = CreateIndexTransform(result, is_top_level, reinterpret_cast<IndexStmt *>(node));
      break;
    }
    case T_InsertStmt: {
      stmt = InsertTransform(result, is_top_level, reinterpret_cast<InsertStmt *>(node));
      break;
    }
    case T_PrepareStmt: {
      stmt = PrepareTransform(result, is_top_level, reinterpret_cast<PrepareStmt *>(node));
      break;
    }
    case T_SelectStmt: {
      stmt = SelectTransform(result, is_top_level, reinterpret_cast<SelectStmt *>(node));
      break;
    }
    case T_VacuumStmt: {
      stmt = VacuumTransform(result, is_top_level, reinterpret_cast<VacuumStmt *>(node));
      break;
    }
    case T_VariableSetStmt: {
      stmt = VariableSetTransform(result, is_top_level, reinterpret_cast<VariableSetStmt *>(node));
      break;
    }
    case T_ViewStmt: {
      stmt = CreateViewTransform(result, is_top_level, reinterpret_cast<ViewStmt *>(node));
      break;
    }
    case T_TruncateStmt: {
      stmt = TruncateTransform(result, is_top_level, reinterpret_cast<TruncateStmt *>(node));
      break;
    }
    case T_TransactionStmt: {
      stmt = TransactionTransform(result, is_top_level, reinterpret_cast<TransactionStmt *>(node));
      break;
    }
    case T_UpdateStmt: {
      stmt = UpdateTransform(result, is_top_level, reinterpret_cast<UpdateStmt *>(node));
      break;
    }
    case T_DeleteStmt: {
      stmt = DeleteTransform(result, is_top_level, reinterpret_cast<DeleteStmt *>(node));
      break;
    }
    default: {
      PARSER_LOG_DEBUG("NodeTransform: statement type {} unsupported", node->type);
      throw PARSER_EXCEPTION("NodeTransform: unsupported statement type");
    }
  }
  return stmt;
}

common::ManagedPointer<AbstractExpression> PostgresParser::ExprTransform(ParseResult *result, bool is_top_level,
                                                                         Node *node) {
  if (node == nullptr) {
    return common::ManagedPointer<AbstractExpression>(nullptr);
  }

  common::ManagedPointer<AbstractExpression> expr;
  switch (node->type) {
    case T_A_Const: {
      expr = ConstTransform(result, is_top_level, reinterpret_cast<A_Const *>(node));
      break;
    }
    case T_A_Expr: {
      expr = AExprTransform(result, is_top_level, reinterpret_cast<A_Expr *>(node));
      break;
    }
    case T_BoolExpr: {
      expr = BoolExprTransform(result, is_top_level, reinterpret_cast<BoolExpr *>(node));
      break;
    }
    case T_CaseExpr: {
      expr = CaseExprTransform(result, is_top_level, reinterpret_cast<CaseExpr *>(node));
      break;
    }
    case T_ColumnRef: {
      expr = ColumnRefTransform(result, is_top_level, reinterpret_cast<ColumnRef *>(node));
      break;
    }
    case T_FuncCall: {
      expr = FuncCallTransform(result, is_top_level, reinterpret_cast<FuncCall *>(node));
      break;
    }
    case T_NullTest: {
      expr = NullTestTransform(result, is_top_level, reinterpret_cast<NullTest *>(node));
      break;
    }
    case T_ParamRef: {
      expr = ParamRefTransform(result, is_top_level, reinterpret_cast<ParamRef *>(node));
      break;
    }
    case T_SubLink: {
      expr = SubqueryExprTransform(result, is_top_level, reinterpret_cast<SubLink *>(node));
      break;
    }
    case T_TypeCast: {
      expr = AExprTransform(result, is_top_level, reinterpret_cast<A_Expr *>(node));
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
common::ManagedPointer<AbstractExpression> PostgresParser::AExprTransform(ParseResult *result, bool is_top_level,
                                                                          A_Expr *root) {
  // TODO(WAN): the old system says, need a function to transform strings of ops to peloton exprtype
  // e.g. > to COMPARE_GREATERTHAN
  if (root == nullptr) {
    return common::ManagedPointer<AbstractExpression>(nullptr);
  }

  ExpressionType target_type;
  std::vector<const AbstractExpression *> children;

  if (root->kind == AEXPR_DISTINCT) {
    target_type = ExpressionType::COMPARE_IS_DISTINCT_FROM;
    children.emplace_back(ExprTransform(result, false, root->lexpr).get());
    children.emplace_back(ExprTransform(result, false, root->rexpr).get());
  } else if (root->kind == AEXPR_OP && root->type == T_TypeCast) {
    target_type = ExpressionType::OPERATOR_CAST;
  } else {
    auto name = (reinterpret_cast<value *>(root->name->head->data.ptr_value))->val.str;
    target_type = StringToExpressionType(name);
    children.emplace_back(ExprTransform(result, false, root->lexpr).get());
    children.emplace_back(ExprTransform(result, false, root->rexpr).get());
  }

  AbstractExpression *expr;
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
      expr = new OperatorExpression(target_type, type::TypeId::INVALID, std::move(children));
      break;
    }
    case ExpressionType::OPERATOR_CAST: {
      expr = TypeCastTransform(result, false, reinterpret_cast<TypeCast *>(root)).get();
      break;
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
      expr = new ComparisonExpression(target_type, std::move(children));
      break;
    }
    default: {
      PARSER_LOG_DEBUG("AExprTransform: type {} unsupported", static_cast<int>(target_type));
      for (auto *child : children) {
        delete child;
      }
      throw PARSER_EXCEPTION("AExprTransform: unsupported type");
    }
  }
  result->AddExpression(is_top_level, expr);
  return common::ManagedPointer(expr);
}

// Postgres.BoolExpr -> terrier.ConjunctionExpression
common::ManagedPointer<AbstractExpression> PostgresParser::BoolExprTransform(ParseResult *result, bool is_top_level,
                                                                             BoolExpr *root) {
  std::vector<const AbstractExpression *> children;
  for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
    auto node = reinterpret_cast<Node *>(cell->data.ptr_value);
    children.emplace_back(ExprTransform(result, false, node).get());
  }
  AbstractExpression *expr;
  switch (root->boolop) {
    case AND_EXPR: {
      expr = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, children);
      break;
    }
    case OR_EXPR: {
      expr = new ConjunctionExpression(ExpressionType::CONJUNCTION_OR, children);
      break;
    }
    case NOT_EXPR: {
      expr = new OperatorExpression(ExpressionType::OPERATOR_NOT, type::TypeId::INVALID, children);
      break;
    }
    default: {
      PARSER_LOG_DEBUG("BoolExprTransform: type {} unsupported", root->boolop);
      throw PARSER_EXCEPTION("BoolExprTransform: unsupported type");
    }
  }
  result->AddExpression(is_top_level, expr);
  return common::ManagedPointer(expr);
}

// Postgres.CaseExpr -> terrier.CaseExpression
common::ManagedPointer<AbstractExpression> PostgresParser::CaseExprTransform(ParseResult *result, bool is_top_level,
                                                                             CaseExpr *root) {
  if (root == nullptr) {
    return common::ManagedPointer<AbstractExpression>(nullptr);
  }

  auto arg_expr = ExprTransform(result, false, reinterpret_cast<Node *>(root->arg)).get();

  std::vector<CaseExpression::WhenClause *> clauses;
  for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
    auto *w = reinterpret_cast<CaseWhen *>(cell->data.ptr_value);
    auto *when_expr = ExprTransform(result, false, reinterpret_cast<Node *>(w->expr)).get();
    auto *result_expr = ExprTransform(result, false, reinterpret_cast<Node *>(w->result)).get();

    if (arg_expr == nullptr) {
      clauses.push_back(new CaseExpression::WhenClause(when_expr, result_expr));
    } else {
      std::vector<const AbstractExpression *> children;
      children.emplace_back(arg_expr->Copy());
      children.emplace_back(when_expr);
      auto *cmp_expr = new ComparisonExpression(ExpressionType::COMPARE_EQUAL, std::move(children));
      clauses.push_back(new CaseExpression::WhenClause(cmp_expr, result_expr));
    }
  }

  auto *default_expr = ExprTransform(result, false, reinterpret_cast<Node *>(root->defresult)).get();
  auto ret_val_type = clauses[0]->then_->GetReturnValueType();

  delete arg_expr;
  auto expr = new CaseExpression(ret_val_type, std::move(clauses), default_expr);
  result->AddExpression(is_top_level, expr);
  return common::ManagedPointer<AbstractExpression>(expr);
}

// Postgres.ColumnRef -> terrier.TupleValueExpression | terrier.StarExpression
common::ManagedPointer<AbstractExpression> PostgresParser::ColumnRefTransform(ParseResult *result, bool is_top_level,
                                                                              ColumnRef *root) {
  AbstractExpression *expr;
  List *fields = root->fields;
  auto node = reinterpret_cast<Node *>(fields->head->data.ptr_value);
  switch (node->type) {
    case T_String: {
      // TODO(WAN): verify the old system is doing the right thing
      if (fields->length == 1) {
        auto col_name = reinterpret_cast<value *>(node)->val.str;
        expr = new TupleValueExpression(col_name, "");
      } else {
        auto next_node = reinterpret_cast<Node *>(fields->head->next->data.ptr_value);
        auto col_name = reinterpret_cast<value *>(next_node)->val.str;
        auto table_name = reinterpret_cast<value *>(node)->val.str;
        expr = new TupleValueExpression(col_name, table_name);
      }
      break;
    }
    case T_A_Star: {
      expr = new StarExpression();
      break;
    }
    default: {
      PARSER_LOG_DEBUG("ColumnRefTransform: type {} unsupported", node->type);
      throw PARSER_EXCEPTION("ColumnRefTransform: unsupported type");
    }
  }
  result->AddExpression(is_top_level, expr);
  return common::ManagedPointer(expr);
}

// Postgres.A_Const -> terrier.ConstantValueExpression
common::ManagedPointer<AbstractExpression> PostgresParser::ConstTransform(ParseResult *result, bool is_top_level,
                                                                          A_Const *root) {
  if (root == nullptr) {
    return common::ManagedPointer<AbstractExpression>(nullptr);
  }
  return ValueTransform(result, is_top_level, root->val);
}

// Postgres.FuncCall -> terrier.AbstractExpression
common::ManagedPointer<AbstractExpression> PostgresParser::FuncCallTransform(ParseResult *result, bool is_top_level,
                                                                             FuncCall *root) {
  // TODO(WAN): change case?
  std::string func_name = reinterpret_cast<value *>(root->funcname->head->data.ptr_value)->val.str;

  AbstractExpression *expr;
  if (!IsAggregateFunction(func_name)) {
    // normal functions (built-in functions or UDFs)
    func_name = (reinterpret_cast<value *>(root->funcname->tail->data.ptr_value))->val.str;
    std::vector<const AbstractExpression *> children;

    if (root->args != nullptr) {
      for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
        auto expr_node = reinterpret_cast<Node *>(cell->data.ptr_value);
        children.emplace_back(ExprTransform(result, false, expr_node).get());
      }
    }
    expr = new FunctionExpression(func_name, type::TypeId::INVALID, children);
  } else {
    // aggregate function
    auto agg_fun_type = StringToExpressionType("AGGREGATE_" + func_name);
    std::vector<const AbstractExpression *> children;
    if (root->agg_star) {
      auto child = new StarExpression();
      children.emplace_back(child);
      expr = new AggregateExpression(agg_fun_type, std::move(children), root->agg_distinct);
    } else if (root->args->length < 2) {
      auto expr_node = reinterpret_cast<Node *>(root->args->head->data.ptr_value);
      auto child = ExprTransform(result, false, expr_node).get();
      children.emplace_back(child);
      expr = new AggregateExpression(agg_fun_type, std::move(children), root->agg_distinct);
    } else {
      PARSER_LOG_DEBUG("FuncCallTransform: Aggregation over multiple cols not supported");
      throw PARSER_EXCEPTION("FuncCallTransform: Aggregation over multiple cols not supported");
    }
  }
  result->AddExpression(is_top_level, expr);
  return common::ManagedPointer(expr);
}

// Postgres.NullTest -> terrier.OperatorExpression
common::ManagedPointer<AbstractExpression> PostgresParser::NullTestTransform(ParseResult *result, bool is_top_level,
                                                                             NullTest *root) {
  if (root == nullptr) {
    return common::ManagedPointer<AbstractExpression>(nullptr);
  }

  std::vector<const AbstractExpression *> children;

  AbstractExpression *child_expr;
  switch (root->arg->type) {
    case T_ColumnRef: {
      child_expr = ColumnRefTransform(result, false, reinterpret_cast<ColumnRef *>(root->arg)).get();
      break;
    }
    case T_A_Const: {
      child_expr = ConstTransform(result, false, reinterpret_cast<A_Const *>(root->arg)).get();
      break;
    }
    case T_A_Expr: {
      child_expr = AExprTransform(result, false, reinterpret_cast<A_Expr *>(root->arg)).get();
      break;
    }
    case T_ParamRef: {
      child_expr = ParamRefTransform(result, false, reinterpret_cast<ParamRef *>(root->arg)).get();
      break;
    }
    default: { PARSER_LOG_AND_THROW("NullTestTransform", "ArgExpr type", root->arg->type); }
  }
  children.emplace_back(child_expr);

  ExpressionType type =
      root->nulltesttype == IS_NULL ? ExpressionType::OPERATOR_IS_NULL : ExpressionType::OPERATOR_IS_NOT_NULL;

  auto expr = new OperatorExpression(type, type::TypeId::BOOLEAN, std::move(children));
  result->AddExpression(is_top_level, expr);
  return common::ManagedPointer<AbstractExpression>(expr);
}

// Postgres.ParamRef -> terrier.ParameterValueExpression
common::ManagedPointer<AbstractExpression> PostgresParser::ParamRefTransform(ParseResult *result, bool is_top_level,
                                                                             ParamRef *root) {
  auto expr = new ParameterValueExpression(root->number - 1);
  result->AddExpression(is_top_level, expr);
  return common::ManagedPointer<AbstractExpression>(expr);
}

// Postgres.SubLink -> terrier.SubqueryExpression
common::ManagedPointer<AbstractExpression> PostgresParser::SubqueryExprTransform(ParseResult *result, bool is_top_level,
                                                                                 SubLink *node) {
  if (node == nullptr) {
    return common::ManagedPointer<AbstractExpression>(nullptr);
  }

  auto select_stmt = SelectTransform(result, false, reinterpret_cast<SelectStmt *>(node->subselect));
  auto subquery_expr = new SubqueryExpression(select_stmt.CastManagedPointerTo<SelectStatement>());
  std::vector<const AbstractExpression *> children;

  AbstractExpression *expr;
  switch (node->subLinkType) {
    case ANY_SUBLINK: {
      auto col_expr = ExprTransform(result, false, node->testexpr).get();
      children.emplace_back(col_expr);
      children.emplace_back(subquery_expr);
      expr = new ComparisonExpression(ExpressionType::COMPARE_IN, std::move(children));
      break;
    }
    case EXISTS_SUBLINK: {
      children.emplace_back(subquery_expr);
      expr = new OperatorExpression(ExpressionType::OPERATOR_EXISTS, type::TypeId::BOOLEAN, std::move(children));
      break;
    }
    case EXPR_SUBLINK: {
      expr = subquery_expr;
      break;
    }
    default: { PARSER_LOG_AND_THROW("SubqueryExprTransform", "Sublink type", node->subLinkType); }
  }
  result->AddExpression(is_top_level, expr);
  return common::ManagedPointer(expr);
}

// Postgres.TypeCast -> terrier.TypeCastExpression
common::ManagedPointer<AbstractExpression> PostgresParser::TypeCastTransform(ParseResult *result, bool is_top_level,
                                                                             TypeCast *root) {
  auto type_name = reinterpret_cast<value *>(root->typeName->names->tail->data.ptr_value)->val.str;
  auto type = ColumnDefinition::StrToValueType(type_name);
  std::vector<const AbstractExpression *> children;
  children.emplace_back(ExprTransform(result, false, root->arg).get());
  auto expr = new TypeCastExpression(type, std::move(children));
  result->AddExpression(is_top_level, expr);
  return common::ManagedPointer<AbstractExpression>(expr);
}

// Postgres.value -> terrier.ConstantValueExpression
common::ManagedPointer<AbstractExpression> PostgresParser::ValueTransform(ParseResult *result, bool is_top_level,
                                                                          value val) {
  AbstractExpression *expr;
  switch (val.type) {
    case T_Integer: {
      auto v = type::TransientValueFactory::GetInteger(val.val.ival);
      expr = new ConstantValueExpression(v);
      break;
    }

    case T_String: {
      auto v = type::TransientValueFactory::GetVarChar(val.val.str);
      expr = new ConstantValueExpression(v);
      break;
    }

    case T_Float: {
      auto v = type::TransientValueFactory::GetDecimal(std::stod(val.val.str));
      expr = new ConstantValueExpression(v);
      break;
    }

    case T_Null: {
      auto v = type::TransientValueFactory::GetBoolean(false);
      v.SetNull(true);
      expr = new ConstantValueExpression(v);
      break;
    }

    default: { PARSER_LOG_AND_THROW("ValueTransform", "Value type", val.type); }
  }
  result->exprs.emplace_back(expr);
  return common::ManagedPointer(expr);
}

common::ManagedPointer<SQLStatement> PostgresParser::SelectTransform(ParseResult *result, bool is_top_level,
                                                                     SelectStmt *root) {
  switch (root->op) {
    case SETOP_NONE: {
      auto from = FromTransform(result, root);
      auto target = TargetTransform(result, root->targetList);
      auto select_distinct = root->distinctClause != nullptr;
      auto groupby = GroupByTransform(result, root->groupClause, root->havingClause);
      auto orderby = OrderByTransform(result, root->sortClause);
      auto where = WhereTransform(result, root->whereClause);

      int64_t limit = LimitDescription::NO_LIMIT;
      int64_t offset = LimitDescription::NO_OFFSET;
      if (root->limitCount != nullptr) {
        limit = reinterpret_cast<A_Const *>(root->limitCount)->val.val.ival;
        if (root->limitOffset != nullptr) {
          offset = reinterpret_cast<A_Const *>(root->limitOffset)->val.val.ival;
        }
      }
      auto limit_desc = new LimitDescription(limit, offset);
      result->limit_descriptions.emplace_back(limit_desc);

      auto stmt = new SelectStatement(std::move(target), select_distinct, from, where, groupby, orderby,
                                      common::ManagedPointer(limit_desc));
      result->AddStatement(is_top_level, stmt);
      return common::ManagedPointer<SQLStatement>(stmt);
    }
    case SETOP_UNION: {
      auto stmt = SelectTransform(result, false, root->larg);
      auto union_stmt = SelectTransform(result, false, root->rarg).CastManagedPointerTo<SelectStatement>();
      stmt.CastManagedPointerTo<SelectStatement>()->SetUnionSelect(union_stmt);
      return stmt;
    }
    default: { PARSER_LOG_AND_THROW("SelectTransform", "Set operation", root->type); }
  }
}

// Postgres.SelectStmt.whereClause -> terrier.SelectStatement.select_
std::vector<common::ManagedPointer<AbstractExpression>> PostgresParser::TargetTransform(ParseResult *result,
                                                                                        List *root) {
  // Postgres parses 'SELECT;' to nullptr
  if (root == nullptr) {
    throw PARSER_EXCEPTION("TargetTransform: root==null.");
  }

  std::vector<common::ManagedPointer<AbstractExpression>> targets;
  for (auto cell = root->head; cell != nullptr; cell = cell->next) {
    auto target = reinterpret_cast<ResTarget *>(cell->data.ptr_value);
    /*
      TODO(WAN): the heck was this doing here?
      if (target->name != nullptr) {
        expr->alias = target->name;
      }
    */
    targets.emplace_back(ExprTransform(result, true, target->val));
  }
  return targets;
}

// TODO(WAN): doesn't support select from multiple sources, nested queries, various joins
// Postgres.SelectStmt.fromClause -> terrier.TableRef
common::ManagedPointer<TableRef> PostgresParser::FromTransform(ParseResult *result, SelectStmt *select_root) {
  // current code assumes SELECT from one source
  List *root = select_root->fromClause;

  // Postgres parses 'SELECT;' to nullptr
  if (root == nullptr) {
    return common::ManagedPointer<TableRef>(nullptr);
  }

  // TODO(WAN): this codepath came from the old system. Can simplify?

  if (root->length > 1) {
    std::vector<common::ManagedPointer<TableRef>> refs;
    for (auto cell = root->head; cell != nullptr; cell = cell->next) {
      auto node = reinterpret_cast<Node *>(cell->data.ptr_value);
      switch (node->type) {
        case T_RangeVar: {
          refs.emplace_back(RangeVarTransform(result, reinterpret_cast<RangeVar *>(node)));
          break;
        }
        case T_RangeSubselect: {
          refs.emplace_back(RangeSubselectTransform(result, reinterpret_cast<RangeSubselect *>(node)));
          break;
        }
        default: { PARSER_LOG_AND_THROW("FromTransform", "FromType", node->type); }
      }
    }
    auto table_ref = TableRef::CreateTableRefByList(std::move(refs));
    result->table_refs.emplace_back(table_ref);
    return common::ManagedPointer(table_ref);
  }

  auto table_ref = common::ManagedPointer<TableRef>(nullptr);
  auto node = reinterpret_cast<Node *>(root->head->data.ptr_value);
  switch (node->type) {
    case T_RangeVar: {
      table_ref = RangeVarTransform(result, reinterpret_cast<RangeVar *>(node));
      break;
    }
    case T_JoinExpr: {
      auto join = JoinTransform(result, reinterpret_cast<JoinExpr *>(node));
      if (join != nullptr) {
        auto table_ref_ptr = TableRef::CreateTableRefByJoin(join);
        result->table_refs.emplace_back(table_ref_ptr);
        table_ref = common::ManagedPointer(table_ref_ptr);
      }
      break;
    }
    case T_RangeSubselect: {
      table_ref = RangeSubselectTransform(result, reinterpret_cast<RangeSubselect *>(node));
      break;
    }
    default: { PARSER_LOG_AND_THROW("FromTransform", "FromType", node->type); }
  }

  return table_ref;
}

// Postgres.SelectStmt.groupClause -> terrier.GroupByDescription
common::ManagedPointer<GroupByDescription> PostgresParser::GroupByTransform(ParseResult *result, List *group,
                                                                            Node *having_node) {
  if (group == nullptr) {
    return common::ManagedPointer<GroupByDescription>(nullptr);
  }

  std::vector<common::ManagedPointer<AbstractExpression>> columns;
  for (auto cell = group->head; cell != nullptr; cell = cell->next) {
    auto temp = reinterpret_cast<Node *>(cell->data.ptr_value);
    columns.emplace_back(ExprTransform(result, true, temp));
  }

  // TODO(WAN): old system says, having clauses not implemented, depends on AExprTransform
  auto having = common::ManagedPointer<AbstractExpression>(nullptr);
  if (having_node != nullptr) {
    having = ExprTransform(result, true, having_node);
  }

  auto group_by = new GroupByDescription(std::move(columns), having);
  result->group_bys.emplace_back(group_by);
  return common::ManagedPointer(group_by);
}

// Postgres.SelectStmt.sortClause -> terrier.OrderDescription
common::ManagedPointer<OrderByDescription> PostgresParser::OrderByTransform(ParseResult *result, List *order) {
  if (order == nullptr) {
    return common::ManagedPointer<OrderByDescription>(nullptr);
  }

  std::vector<OrderType> types;
  std::vector<common::ManagedPointer<AbstractExpression>> exprs;

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
        exprs.emplace_back(ExprTransform(result, true, target));
        break;
      }
      default: { PARSER_LOG_AND_THROW("OrderByTransform", "OrderBy type", temp->type); }
    }
  }

  auto order_by = new OrderByDescription(std::move(types), std::move(exprs));
  result->order_bys.emplace_back(order_by);
  return common::ManagedPointer(order_by);
}

// Postgres.SelectStmt.whereClause -> terrier.AbstractExpression
common::ManagedPointer<AbstractExpression> PostgresParser::WhereTransform(ParseResult *result, Node *root) {
  if (root == nullptr) {
    return common::ManagedPointer<AbstractExpression>(nullptr);
  }
  return ExprTransform(result, true, root);
}

// Postgres.JoinExpr -> terrier.JoinDefinition
common::ManagedPointer<JoinDefinition> PostgresParser::JoinTransform(ParseResult *result, JoinExpr *root) {
  // TODO(WAN): magic number 4?
  if ((root->jointype > 4) || (root->isNatural)) {
    return common::ManagedPointer<JoinDefinition>(nullptr);
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

  common::ManagedPointer<TableRef> left;
  switch (root->larg->type) {
    case T_RangeVar: {
      left = RangeVarTransform(result, reinterpret_cast<RangeVar *>(root->larg));
      break;
    }
    case T_RangeSubselect: {
      left = RangeSubselectTransform(result, reinterpret_cast<RangeSubselect *>(root->larg));
      break;
    }
    case T_JoinExpr: {
      auto join = JoinTransform(result, reinterpret_cast<JoinExpr *>(root->larg));
      auto left_ref = TableRef::CreateTableRefByJoin(join);
      result->table_refs.emplace_back(left_ref);
      left = common::ManagedPointer(left_ref);
      break;
    }
    default: { PARSER_LOG_AND_THROW("JoinTransform", "JoinArgType", root->larg->type); }
  }

  common::ManagedPointer<TableRef> right;
  switch (root->rarg->type) {
    case T_RangeVar: {
      right = RangeVarTransform(result, reinterpret_cast<RangeVar *>(root->rarg));
      break;
    }
    case T_RangeSubselect: {
      right = RangeSubselectTransform(result, reinterpret_cast<RangeSubselect *>(root->rarg));
      break;
    }
    case T_JoinExpr: {
      auto join = JoinTransform(result, reinterpret_cast<JoinExpr *>(root->rarg));
      auto right_ref = TableRef::CreateTableRefByJoin(join);
      result->table_refs.emplace_back(right_ref);
      right = common::ManagedPointer(right_ref);
      break;
    }
    default: { PARSER_LOG_AND_THROW("JoinTransform", "Right JoinArgType", root->rarg->type); }
  }

  // TODO(WAN): quick fix to prevent segfaulting on the following CROSS JOIN test case
  // SELECT * FROM tab0 AS cor0 CROSS JOIN tab0 AS cor1 WHERE NULL IS NOT NULL;
  if (root->quals == nullptr) {
    PARSER_LOG_AND_THROW("JoinTransform", "root->quals", nullptr);
  }

  common::ManagedPointer<AbstractExpression> condition;
  switch (root->quals->type) {
    case T_A_Expr: {
      condition = AExprTransform(result, true, reinterpret_cast<A_Expr *>(root->quals));
      break;
    }
    case T_BoolExpr: {
      condition = BoolExprTransform(result, true, reinterpret_cast<BoolExpr *>(root->quals));
      break;
    }
    default: { PARSER_LOG_AND_THROW("JoinTransform", "Join condition type", root->quals->type); }
  }

  auto join_def = new JoinDefinition(type, left, right, condition);
  result->join_definitions.emplace_back(join_def);
  return common::ManagedPointer(join_def);
}

std::string PostgresParser::AliasTransform(Alias *root) {
  if (root == nullptr) {
    return "";
  }
  return root->aliasname;
}

// Postgres.RangeVar -> terrier.TableRef
common::ManagedPointer<TableRef> PostgresParser::RangeVarTransform(ParseResult *result, RangeVar *root) {
  auto table_name = root->relname == nullptr ? "" : root->relname;
  auto schema_name = root->schemaname == nullptr ? "" : root->schemaname;
  auto database_name = root->catalogname == nullptr ? "" : root->catalogname;

  auto table_info = new TableInfo(table_name, schema_name, database_name);
  result->table_infos.emplace_back(table_info);
  auto alias = AliasTransform(root->alias);
  auto table_ref = TableRef::CreateTableRefByName(alias, common::ManagedPointer(table_info));
  result->table_refs.emplace_back(table_ref);
  return common::ManagedPointer(table_ref);
}

// Postgres.RangeSubselect -> terrier.TableRef
common::ManagedPointer<TableRef> PostgresParser::RangeSubselectTransform(ParseResult *result, RangeSubselect *root) {
  auto select = SelectTransform(result, false, reinterpret_cast<SelectStmt *>(root->subquery));
  // TODO(WAN): think Matt had something about ManagedPointers and nullptr
  if (select.get() == nullptr) {
    return common::ManagedPointer<TableRef>(nullptr);
  }
  auto alias = AliasTransform(root->alias);
  auto table_ref = TableRef::CreateTableRefBySelect(alias, select.CastManagedPointerTo<SelectStatement>());
  result->table_refs.emplace_back(table_ref);
  return common::ManagedPointer(table_ref);
}

// Postgres.CopyStmt -> terrier.CopyStatement
common::ManagedPointer<SQLStatement> PostgresParser::CopyTransform(ParseResult *result, bool is_top_level,
                                                                   CopyStmt *root) {
  static constexpr char kDelimiterTok[] = "delimiter";
  static constexpr char kFormatTok[] = "format";
  static constexpr char kQuoteTok[] = "quote";
  static constexpr char kEscapeTok[] = "escape";

  auto table = common::ManagedPointer<TableRef>(nullptr);
  auto select_stmt = common::ManagedPointer<SelectStatement>(nullptr);
  if (root->relation != nullptr) {
    table = RangeVarTransform(result, root->relation);
  } else {
    select_stmt = SelectTransform(result, false, reinterpret_cast<SelectStmt *>(root->query))
                      .CastManagedPointerTo<SelectStatement>();
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

  auto stmt = new CopyStatement(table, select_stmt, file_path, format, is_from, delimiter, quote, escape);
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

// Postgres.CreateStmt -> terrier.CreateStatement
common::ManagedPointer<SQLStatement> PostgresParser::CreateTransform(ParseResult *result, bool is_top_level,
                                                                     CreateStmt *root) {
  RangeVar *relation = root->relation;
  auto table_name = relation->relname != nullptr ? relation->relname : "";
  auto schema_name = relation->schemaname != nullptr ? relation->schemaname : "";
  auto database_name = relation->schemaname != nullptr ? relation->catalogname : "";
  auto table_info = new TableInfo(table_name, schema_name, database_name);
  result->table_infos.emplace_back(table_info);

  std::unordered_set<std::string> primary_keys;

  std::vector<common::ManagedPointer<ColumnDefinition>> columns;
  std::vector<common::ManagedPointer<ColumnDefinition>> foreign_keys;

  for (auto cell = root->tableElts->head; cell != nullptr; cell = cell->next) {
    auto node = reinterpret_cast<Node *>(cell->data.ptr_value);
    switch (node->type) {
      case T_ColumnDef: {
        auto res = ColumnDefTransform(result, reinterpret_cast<ColumnDef *>(node));
        columns.emplace_back(res.col);
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

            auto fk = new ColumnDefinition(std::move(fk_sources), std::move(fk_sinks), fk_sink_table_name,
                                           fk_delete_action, fk_update_action, fk_match_type);
            result->column_definitions.emplace_back(fk);
            foreign_keys.emplace_back(common::ManagedPointer(fk));
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

  auto stmt = new CreateStatement(common::ManagedPointer(table_info), CreateStatement::CreateType::kTable,
                                  std::move(columns), std::move(foreign_keys));
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

// Postgres.CreateDatabaseStmt -> terrier.CreateStatement (database)
common::ManagedPointer<parser::SQLStatement> PostgresParser::CreateDatabaseTransform(ParseResult *result,
                                                                                     bool is_top_level,
                                                                                     CreateDatabaseStmt *root) {
  auto table_info = new TableInfo("", "", root->dbname);
  result->table_infos.emplace_back(table_info);
  std::vector<common::ManagedPointer<ColumnDefinition>> columns;
  std::vector<common::ManagedPointer<ColumnDefinition>> foreign_keys;

  // TODO(WAN): per the old system, more options need to be converted
  // see postgresparser.h and the postgresql docs
  auto stmt = new CreateStatement(common::ManagedPointer(table_info), CreateStatement::kDatabase, std::move(columns),
                                  std::move(foreign_keys));
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

// Postgres.CreateFunctionStmt -> terrier.CreateFunctionStatement (function)
common::ManagedPointer<SQLStatement> PostgresParser::CreateFunctionTransform(ParseResult *result, bool is_top_level,
                                                                             CreateFunctionStmt *root) {
  bool replace = root->replace;
  std::vector<common::ManagedPointer<FuncParameter>> func_parameters;

  for (auto cell = root->parameters->head; cell != nullptr; cell = cell->next) {
    auto node = reinterpret_cast<Node *>(cell->data.ptr_value);
    switch (node->type) {
      case T_FunctionParameter: {
        func_parameters.emplace_back(FunctionParameterTransform(result, reinterpret_cast<FunctionParameter *>(node)));
        break;
      }
      default: {
        // TODO(WAN): previous code just ignored it, is this right? Probably not.
        break;
      }
    }
  }

  auto return_type = ReturnTypeTransform(result, reinterpret_cast<TypeName *>(root->returnType));

  // TODO(WAN): assumption from old code, can only pass one function name for now
  std::string func_name = reinterpret_cast<value *>(root->funcname->tail->data.ptr_value)->val.str;

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

  auto stmt = new CreateFunctionStatement(replace, std::move(func_name), std::move(func_body), return_type,
                                          std::move(func_parameters), pl_type, as_type);
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

// Postgres.IndexStmt -> terrier.CreateStatement (index)
common::ManagedPointer<SQLStatement> PostgresParser::CreateIndexTransform(ParseResult *result, bool is_top_level,
                                                                          IndexStmt *root) {
  auto unique = root->unique;
  auto index_name = root->idxname;

  std::vector<common::ManagedPointer<IndexAttr>> index_attrs;
  for (auto cell = root->indexParams->head; cell != nullptr; cell = cell->next) {
    auto *index_elem = reinterpret_cast<IndexElem *>(cell->data.ptr_value);
    IndexAttr *attr;
    if (index_elem->expr == nullptr) {
      attr = new IndexAttr(index_elem->name);
    } else {
      attr = new IndexAttr(ExprTransform(result, true, index_elem->expr));
    }
    result->index_attrs.emplace_back(attr);
    index_attrs.emplace_back(common::ManagedPointer(attr));
  }

  auto table_name = root->relation->relname == nullptr ? "" : root->relation->relname;
  auto schema_name = root->relation->schemaname == nullptr ? "" : root->relation->schemaname;
  auto database_name = root->relation->catalogname == nullptr ? "" : root->relation->catalogname;
  auto table_info = new TableInfo(table_name, schema_name, database_name);
  result->table_infos.emplace_back(table_info);

  char *access_method = root->accessMethod;
  IndexType index_type;
  // TODO(WAN): do we need to do case conversion? I'm think it always comes in lowercase.
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
    PARSER_LOG_DEBUG("CreateIndexTransform: IndexType {} not supported", access_method)
    throw NOT_IMPLEMENTED_EXCEPTION("CreateIndexTransform error");
  }

  auto stmt =
      new CreateStatement(common::ManagedPointer(table_info), index_type, unique, index_name, std::move(index_attrs));
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

// Postgres.CreateSchemaStmt -> terrier.CreateStatement (schema)
common::ManagedPointer<SQLStatement> PostgresParser::CreateSchemaTransform(ParseResult *result, bool is_top_level,
                                                                           CreateSchemaStmt *root) {
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

  auto table_info = new TableInfo("", schema_name, "");
  result->table_infos.emplace_back(table_info);
  auto if_not_exists = root->if_not_exists;

  // TODO(WAN): the old system basically didn't implement any of this

  if (root->schemaElts != nullptr) {
    PARSER_LOG_DEBUG("CreateSchemaTransform schema_element unsupported");
    throw PARSER_EXCEPTION("CreateSchemaTransform schema_element unsupported");
  }

  auto stmt = new CreateStatement(common::ManagedPointer(table_info), if_not_exists);
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

// Postgres.CreateTrigStmt -> terrier.CreateStatement (trigger)
common::ManagedPointer<SQLStatement> PostgresParser::CreateTriggerTransform(ParseResult *result, bool is_top_level,
                                                                            CreateTrigStmt *root) {
  auto table_name = root->relation->relname == nullptr ? "" : root->relation->relname;
  auto schema_name = root->relation->schemaname == nullptr ? "" : root->relation->schemaname;
  auto database_name = root->relation->catalogname == nullptr ? "" : root->relation->catalogname;
  auto table_info = new TableInfo(table_name, schema_name, database_name);
  result->table_infos.emplace_back(table_info);

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

  auto trigger_when = WhenTransform(result, root->whenClause);

  // TODO(WAN): what is this doing?
  int16_t trigger_type = 0;
  TRIGGER_CLEAR_TYPE(trigger_type);
  if (root->row) {
    TRIGGER_SETT_ROW(trigger_type);
  }
  trigger_type |= root->timing;
  trigger_type |= root->events;

  auto stmt = new CreateStatement(common::ManagedPointer(table_info), trigger_name, std::move(trigger_funcnames),
                                  std::move(trigger_args), std::move(trigger_columns), trigger_when, trigger_type);
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

// Postgres.ViewStmt -> terrier.CreateStatement (view)
common::ManagedPointer<SQLStatement> PostgresParser::CreateViewTransform(ParseResult *result, bool is_top_level,
                                                                         ViewStmt *root) {
  auto view_name = root->view->relname;

  common::ManagedPointer<SQLStatement> view_query;
  switch (root->query->type) {
    case T_SelectStmt: {
      view_query = SelectTransform(result, is_top_level, reinterpret_cast<SelectStmt *>(root->query));
      break;
    }
    default: {
      PARSER_LOG_DEBUG("CREATE VIEW as query only supports SELECT");
      throw PARSER_EXCEPTION("CREATE VIEW as query only supports SELECT");
    }
  }

  auto stmt = new CreateStatement(view_name, view_query.CastManagedPointerTo<SelectStatement>());
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

// Postgres.ColumnDef -> terrier.ColumnDefinition
PostgresParser::ColumnDefTransResult PostgresParser::ColumnDefTransform(ParseResult *result, ColumnDef *root) {
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

  std::vector<common::ManagedPointer<ColumnDefinition>> foreign_keys;

  bool is_primary = false;
  bool is_not_null = false;
  bool is_unique = false;
  auto default_expr = common::ManagedPointer<AbstractExpression>(nullptr);
  auto check_expr = common::ManagedPointer<AbstractExpression>(nullptr);

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
          // TODO(WAN): wait, why are these vectors?
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

          auto col_def = new ColumnDefinition(std::move(fk_sources), std::move(fk_sinks), fk_sink_table_name,
                                              fk_delete_action, fk_update_action, fk_match_type);
          result->column_definitions.emplace_back(col_def);
          foreign_keys.emplace_back(common::ManagedPointer(col_def));
          break;
        }
        case CONSTR_DEFAULT: {
          default_expr = ExprTransform(result, true, constraint->raw_expr);
          break;
        }
        case CONSTR_CHECK: {
          check_expr = ExprTransform(result, true, constraint->raw_expr);
          break;
        }
        default: { PARSER_LOG_AND_THROW("ColumnDefTransform", "Constraint", constraint->contype); }
      }
    }
  }

  auto name = root->colname;
  auto col = new ColumnDefinition(name, datatype, is_primary, is_not_null, is_unique, default_expr, check_expr, varlen);
  result->column_definitions.emplace_back(col);
  return {common::ManagedPointer(col), std::move(foreign_keys)};
}

// Postgres.FunctionParameter -> terrier.FuncParameter
common::ManagedPointer<FuncParameter> PostgresParser::FunctionParameterTransform(ParseResult *result,
                                                                                 FunctionParameter *root) {
  // TODO(WAN): significant code duplication, refactor out char* -> DataType
  char *name = (reinterpret_cast<value *>(root->argType->names->tail->data.ptr_value)->val.str);

  FuncParameter::DataType data_type;
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

  auto param_name = root->name != nullptr ? root->name : "";
  auto func_param = new FuncParameter(data_type, param_name);
  result->func_parameters.emplace_back(func_param);
  return common::ManagedPointer(func_param);
}

// Postgres.TypeName -> terrier.ReturnType
common::ManagedPointer<ReturnType> PostgresParser::ReturnTypeTransform(ParseResult *result, TypeName *root) {
  char *name = (reinterpret_cast<value *>(root->names->tail->data.ptr_value)->val.str);

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

  auto ret_type = new ReturnType(data_type);
  result->return_types.emplace_back(ret_type);
  return common::ManagedPointer(ret_type);
}

// Postgres.Node -> terrier.AbstractExpression
common::ManagedPointer<AbstractExpression> PostgresParser::WhenTransform(ParseResult *result, Node *root) {
  if (root == nullptr) {
    return common::ManagedPointer<AbstractExpression>(nullptr);
  }

  common::ManagedPointer<AbstractExpression> expr;
  switch (root->type) {
    case T_A_Expr: {
      expr = AExprTransform(result, true, reinterpret_cast<A_Expr *>(root));
      break;
    }
    case T_BoolExpr: {
      expr = BoolExprTransform(result, true, reinterpret_cast<BoolExpr *>(root));
      break;
    }
    default: { PARSER_LOG_AND_THROW("WhenTransform", "WHEN type", root->type); }
  }
  return expr;
}

// Postgres.DeleteStmt -> terrier.DeleteStatement
common::ManagedPointer<SQLStatement> PostgresParser::DeleteTransform(ParseResult *result, bool is_top_level,
                                                                     DeleteStmt *root) {
  auto table = RangeVarTransform(result, root->relation);
  auto where = WhereTransform(result, root->whereClause);
  auto stmt = new DeleteStatement(table, where);
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

// Postgres.DropStmt -> terrier.DropStatement
common::ManagedPointer<SQLStatement> PostgresParser::DropTransform(ParseResult *result, bool is_top_level,
                                                                   DropStmt *root) {
  switch (root->removeType) {
    case ObjectType::OBJECT_INDEX: {
      return DropIndexTransform(result, is_top_level, root);
    }
    case ObjectType::OBJECT_SCHEMA: {
      return DropSchemaTransform(result, is_top_level, root);
    }
    case ObjectType::OBJECT_TABLE: {
      return DropTableTransform(result, is_top_level, root);
    }
    case ObjectType::OBJECT_TRIGGER: {
      return DropTriggerTransform(result, is_top_level, root);
    }
    default: { PARSER_LOG_AND_THROW("DropTransform", "Drop ObjectType", root->removeType); }
  }
}

// Postgres.DropDatabaseStmt -> terrier.DropStmt (database)
common::ManagedPointer<SQLStatement> PostgresParser::DropDatabaseTransform(ParseResult *result, bool is_top_level,
                                                                           DropDatabaseStmt *root) {
  auto if_exists = root->missing_ok;

  auto table_info = new TableInfo("", "", root->dbname);
  result->table_infos.emplace_back(table_info);

  auto stmt = new DropStatement(common::ManagedPointer(table_info), DropStatement::DropType::kDatabase, if_exists);
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

// Postgres.DropStmt -> terrier.DropStatement (index)
common::ManagedPointer<SQLStatement> PostgresParser::DropIndexTransform(ParseResult *result, bool is_top_level,
                                                                        DropStmt *root) {
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

  auto table_info = new TableInfo("", schema_name, "");
  result->table_infos.emplace_back(table_info);

  auto stmt = new DropStatement(common::ManagedPointer(table_info), index_name);
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

// Postgres.DropStmt -> terrier.DropStatement (schema)
common::ManagedPointer<SQLStatement> PostgresParser::DropSchemaTransform(ParseResult *result, bool is_top_level,
                                                                         DropStmt *root) {
  auto if_exists = root->missing_ok;
  auto cascade = root->behavior == DropBehavior::DROP_CASCADE;

  std::string schema_name;
  for (auto cell = root->objects->head; cell != nullptr; cell = cell->next) {
    auto table_list = reinterpret_cast<List *>(cell->data.ptr_value);
    schema_name = reinterpret_cast<value *>(table_list->head->data.ptr_value)->val.str;
    break;
  }

  auto table_info = new TableInfo("", schema_name, "");
  result->table_infos.emplace_back(table_info);

  auto stmt = new DropStatement(common::ManagedPointer(table_info), if_exists, cascade);
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

// Postgres.DropStmt -> terrier.DropStatement (table)
common::ManagedPointer<SQLStatement> PostgresParser::DropTableTransform(ParseResult *result, bool is_top_level,
                                                                        DropStmt *root) {
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
  auto table_info = new TableInfo(table_name, schema_name, "");
  result->table_infos.emplace_back(table_info);

  auto stmt = new DropStatement(common::ManagedPointer(table_info), DropStatement::DropType::kTable, if_exists);
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

// Postgres.TruncateStmt -> terrier.DeleteStatement
common::ManagedPointer<SQLStatement> PostgresParser::TruncateTransform(ParseResult *result, bool is_top_level,
                                                                       TruncateStmt *truncate_stmt) {
  TERRIER_ASSERT(
      false, "TruncateTransform is a disaster, please look at it. Don't forget to result->AddStatement when you do.");
  return common::ManagedPointer<SQLStatement>(nullptr);
  /*
  TERRIER_ASSERT(truncate_stmt->relations->length == 1, "Single table only");
  auto cell = truncate_stmt->relations->head;
  auto table_ref = RangeVarTransform(reinterpret_cast<RangeVar *>(cell->data.ptr_value));
  result = std::make_unique<DeleteStatement>(std::move(table_ref));

  // TODO(WAN): This code came from the old system. I don't know what it wants or why we're resetting.

  for (auto cell = truncate_stmt->relations->head; cell != nullptr; cell = cell->next) {
    auto table_ref = RangeVarTransform(reinterpret_cast<RangeVar *>(cell->data.ptr_value));

    //result->table_ref_.reset(
    //    RangeVarTransform(reinterpret_cast<RangeVar *>(cell->data.ptr_value)));
    break;
  }
  */
}

// Postgres.DropStmt -> terrier.DropStatement (trigger)
common::ManagedPointer<SQLStatement> PostgresParser::DropTriggerTransform(ParseResult *result, bool is_top_level,
                                                                          DropStmt *root) {
  auto list = reinterpret_cast<List *>(root->objects->head->data.ptr_value);
  std::string trigger_name = reinterpret_cast<value *>(list->tail->data.ptr_value)->val.str;

  std::string table_name;
  std::string schema_name;
  // TODO(WAN): I have no clue what is going on here.
  if (list->length == 3) {
    schema_name = reinterpret_cast<value *>(list->head->data.ptr_value)->val.str;
    table_name = reinterpret_cast<value *>(list->head->next->data.ptr_value)->val.str;
  } else {
    table_name = reinterpret_cast<value *>(list->head->data.ptr_value)->val.str;
  }
  auto table_info = new TableInfo(table_name, schema_name, "");
  result->table_infos.emplace_back(table_info);

  auto stmt = new DropStatement(common::ManagedPointer(table_info), DropStatement::DropType::kTrigger, trigger_name);
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

// Postgres.ExecuteStmt -> terrier.ExecuteStatement
common::ManagedPointer<SQLStatement> PostgresParser::ExecuteTransform(ParseResult *result, bool is_top_level,
                                                                      ExecuteStmt *root) {
  auto name = root->name;
  auto params = ParamListTransform(result, root->params);
  auto stmt = new ExecuteStatement(name, std::move(params));
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

std::vector<common::ManagedPointer<AbstractExpression>> PostgresParser::ParamListTransform(ParseResult *result,
                                                                                           List *root) {
  std::vector<common::ManagedPointer<AbstractExpression>> exprs;

  if (root == nullptr) {
    return exprs;
  }

  for (auto cell = root->head; cell != nullptr; cell = cell->next) {
    auto param = reinterpret_cast<Node *>(cell->data.ptr_value);
    switch (param->type) {
      case T_A_Const: {
        auto node = reinterpret_cast<A_Const *>(cell->data.ptr_value);
        exprs.emplace_back(ConstTransform(result, true, node));
        break;
      }
      case T_A_Expr: {
        auto node = reinterpret_cast<A_Expr *>(cell->data.ptr_value);
        exprs.emplace_back(AExprTransform(result, true, node));
        break;
      }
      case T_FuncCall: {
        auto node = reinterpret_cast<FuncCall *>(cell->data.ptr_value);
        exprs.emplace_back(FuncCallTransform(result, true, node));
        break;
      }
      default: { PARSER_LOG_AND_THROW("ParamListTransform", "ExpressionType", param->type); }
    }
  }

  return exprs;
}

// Postgres.ExplainStmt -> terrier.ExplainStatement
common::ManagedPointer<SQLStatement> PostgresParser::ExplainTransform(ParseResult *result, bool is_top_level,
                                                                      ExplainStmt *root) {
  auto query = NodeTransform(result, false, root->query);
  auto stmt = new ExplainStatement(query);
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

// Postgres.InsertStmt -> terrier.InsertStatement
common::ManagedPointer<SQLStatement> PostgresParser::InsertTransform(ParseResult *result, bool is_top_level,
                                                                     InsertStmt *root) {
  TERRIER_ASSERT(root->selectStmt != nullptr, "Selects from table or directly selects some values.");

  InsertStatement *stmt;

  auto column_names = ColumnNameTransform(root->cols);
  auto table_ref = RangeVarTransform(result, root->relation);
  auto select_stmt = reinterpret_cast<SelectStmt *>(root->selectStmt);

  if (select_stmt->fromClause != nullptr) {
    // select from a table to insert
    auto select_trans = SelectTransform(result, is_top_level, select_stmt);
    stmt =
        new InsertStatement(std::move(column_names), table_ref, select_trans.CastManagedPointerTo<SelectStatement>());
  } else {
    // directly insert some values
    TERRIER_ASSERT(select_stmt->valuesLists != nullptr, "Must have values to insert.");
    auto insert_values = ValueListsTransform(result, select_stmt->valuesLists);
    stmt = new InsertStatement(std::move(column_names), table_ref, std::move(insert_values));
  }

  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

// Postgres.List -> column names
std::vector<std::string> PostgresParser::ColumnNameTransform(List *root) {
  auto result = std::vector<std::string>();

  if (root == nullptr) {
    return result;
  }

  for (auto cell = root->head; cell != nullptr; cell = cell->next) {
    auto target = reinterpret_cast<ResTarget *>(cell->data.ptr_value);
    result.emplace_back(target->name);
  }

  return result;
}

// Transforms value lists into terrier equivalent. Nested vectors, because an InsertStmt may insert multiple tuples.
std::vector<std::vector<common::ManagedPointer<AbstractExpression>>> PostgresParser::ValueListsTransform(
    ParseResult *result, List *root) {
  auto value_lists = std::vector<std::vector<common::ManagedPointer<AbstractExpression>>>();

  for (auto value_list = root->head; value_list != nullptr; value_list = value_list->next) {
    std::vector<common::ManagedPointer<AbstractExpression>> cur_value_list;

    auto target = reinterpret_cast<List *>(value_list->data.ptr_value);
    for (auto cell = target->head; cell != nullptr; cell = cell->next) {
      auto expr = reinterpret_cast<Expr *>(cell->data.ptr_value);
      switch (expr->type) {
        case T_ParamRef: {
          cur_value_list.emplace_back(ParamRefTransform(result, true, reinterpret_cast<ParamRef *>(expr)));
          break;
        }
        case T_A_Const: {
          cur_value_list.emplace_back(ConstTransform(result, true, reinterpret_cast<A_Const *>(expr)));
          break;
        }
        case T_TypeCast: {
          cur_value_list.emplace_back(TypeCastTransform(result, true, reinterpret_cast<TypeCast *>(expr)));
          break;
        }
        /*
         * case T_SetToDefault: {
         * TODO(WAN): old system says it wanted to add corresponding expr for default to cur_reuslt
         */
        default: { PARSER_LOG_AND_THROW("ValueListsTransform", "Value type", expr->type); }
      }
    }
    value_lists.emplace_back(std::move(cur_value_list));
  }

  return value_lists;
}

// Postgres.TransactionStmt -> terrier.TransactionStatement
common::ManagedPointer<SQLStatement> PostgresParser::TransactionTransform(ParseResult *result, bool is_top_level,
                                                                          TransactionStmt *transaction_stmt) {
  TransactionStatement *stmt;

  switch (transaction_stmt->kind) {
    case TRANS_STMT_BEGIN: {
      stmt = new TransactionStatement(TransactionStatement::kBegin);
      break;
    }
    case TRANS_STMT_COMMIT: {
      stmt = new TransactionStatement(TransactionStatement::kCommit);
      break;
    }
    case TRANS_STMT_ROLLBACK: {
      stmt = new TransactionStatement(TransactionStatement::kRollback);
      break;
    }
    default: { PARSER_LOG_AND_THROW("TransactionTransform", "TRANSACTION statement type", transaction_stmt->kind); }
  }
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

std::vector<common::ManagedPointer<UpdateClause>> PostgresParser::UpdateTargetTransform(ParseResult *result,
                                                                                        List *root) {
  std::vector<common::ManagedPointer<UpdateClause>> clauses;
  for (auto cell = root->head; cell != nullptr; cell = cell->next) {
    auto target = reinterpret_cast<ResTarget *>(cell->data.ptr_value);
    auto column = target->name;
    auto value = ExprTransform(result, true, target->val);
    auto upd = new UpdateClause(column, value);
    result->update_clauses.emplace_back(upd);
    clauses.emplace_back(upd);
  }
  return clauses;
}

// Postgres.PrepareStmt -> terrier.PrepareStatement
common::ManagedPointer<SQLStatement> PostgresParser::PrepareTransform(ParseResult *result, bool is_top_level,
                                                                      PrepareStmt *root) {
  auto name = root->name;
  auto query = NodeTransform(result, false, root->query);

  // TODO(WAN): why isn't this populated?
  std::vector<common::ManagedPointer<ParameterValueExpression>> placeholders;

  auto stmt = new PrepareStatement(name, query, std::move(placeholders));
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

// Postgres.VacuumStmt -> terrier.AnalyzeStatement
common::ManagedPointer<SQLStatement> PostgresParser::VacuumTransform(ParseResult *result, bool is_top_level,
                                                                     VacuumStmt *root) {
  switch (root->options) {
    case VACOPT_ANALYZE: {
      auto analyze_table = root->relation != nullptr ? RangeVarTransform(result, root->relation)
                                                     : common::ManagedPointer<TableRef>(nullptr);
      auto analyze_columns = ColumnNameTransform(root->va_cols);
      auto stmt = new AnalyzeStatement(analyze_table, std::move(analyze_columns));
      result->AddStatement(is_top_level, stmt);
      return common::ManagedPointer<SQLStatement>(stmt);
    }
    default: { PARSER_LOG_AND_THROW("VacuumTransform", "Vacuum", root->options); }
  }
}

// Postgres.UpdateStmt -> terrier.UpdateStatement
common::ManagedPointer<SQLStatement> PostgresParser::UpdateTransform(ParseResult *result, bool is_top_level,
                                                                     UpdateStmt *update_stmt) {
  auto table = RangeVarTransform(result, update_stmt->relation);
  auto clauses = UpdateTargetTransform(result, update_stmt->targetList);
  auto where = WhereTransform(result, update_stmt->whereClause);

  auto stmt = new UpdateStatement(table, clauses, where);
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

// TODO(WAN): This one is a little weird. The old system had it as a JDBC hack.
common::ManagedPointer<SQLStatement> PostgresParser::VariableSetTransform(ParseResult *result, bool is_top_level,
                                                                          UNUSED_ATTRIBUTE VariableSetStmt *root) {
  auto stmt = new VariableSetStatement();
  result->AddStatement(is_top_level, stmt);
  return common::ManagedPointer<SQLStatement>(stmt);
}

}  // namespace parser
}  // namespace terrier
