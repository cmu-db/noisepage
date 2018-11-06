#include <libpg_query/pg_list.h>
#include <cstdio>
#include <memory>

#include "libpg_query/pg_list.h"
#include "libpg_query/pg_query.h"

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
#include "parser/pg_trigger.h"
#include "parser/postgresparser.h"
#include "type/value_factory.h"

namespace terrier {
namespace parser {

constexpr int32_t MAX_EXCEPTION_MSG_LEN = 100;

PostgresParser::PostgresParser() = default;

PostgresParser::~PostgresParser() = default;

PostgresParser &PostgresParser::GetInstance() {
  static PostgresParser parser;
  return parser;
}

std::vector<std::unique_ptr<SQLStatement>> PostgresParser::BuildParseTree(const std::string &query_string) {
  auto text = query_string.c_str();
  auto ctx = pg_query_parse_init();
  auto result = pg_query_parse(text);

  if (result.error != nullptr) {
    pg_query_parse_finish(ctx);
    pg_query_free_parse_result(result);

    char msg[MAX_EXCEPTION_MSG_LEN];
    std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "%s at %d.\n", result.error->message, result.error->cursorpos);
    throw ParserException(msg);
  }

  std::vector<std::unique_ptr<SQLStatement>> transform_result;
  try {
    transform_result = ListTransform(result.tree);
  } catch (const std::exception &e) {
    pg_query_parse_finish(ctx);
    pg_query_free_parse_result(result);
    throw e;
  }

  pg_query_parse_finish(ctx);
  pg_query_free_parse_result(result);
  return transform_result;
}

std::vector<std::unique_ptr<SQLStatement>> PostgresParser::ListTransform(List *root) {
  std::vector<std::unique_ptr<SQLStatement>> result;

  if (root != nullptr) {
    try {
      for (auto cell = root->head; cell != nullptr; cell = cell->next) {
        auto node = static_cast<Node *>(cell->data.ptr_value);
        result.emplace_back(NodeTransform(node));
      }
    } catch (const std::exception &e) {
      result.clear();
      throw e;
    }
  }

  return result;
}

std::unique_ptr<SQLStatement> PostgresParser::NodeTransform(Node *node) {
  if (node == nullptr) {
    return nullptr;
  }

  std::unique_ptr<SQLStatement> result = nullptr;
  switch (node->type) {
    case T_SelectStmt: {
      result = SelectTransform(reinterpret_cast<SelectStmt *>(node));
      break;
    }
      /*
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
       */
    case T_InsertStmt: {
      result = InsertTransform(reinterpret_cast<InsertStmt *>(node));
      break;
    }
      /*


                case T_IndexStmt:
                  result = CreateIndexTransform(reinterpret_cast<IndexStmt *>(stmt));
                  break;
                case T_CreateTrigStmt:
                  result = CreateTriggerTransform(reinterpret_cast<CreateTrigStmt *>(stmt));
                  break;
                case T_CreateSchemaStmt:
                  result =
                      CreateSchemaTransform(reinterpret_cast<CreateSchemaStmt *>(stmt));
                  break;
                case T_ViewStmt:
                  result = CreateViewTransform(reinterpret_cast<ViewStmt *>(stmt));
                  break;
                case T_UpdateStmt:
                  result = UpdateTransform((UpdateStmt *)stmt);
                  break;
                case T_DeleteStmt:
                  result = DeleteTransform((DeleteStmt *)stmt);
                  break;

                case T_DropStmt:
                  result = DropTransform((DropStmt *)stmt);
                  break;
                case T_DropdbStmt:
                  result = DropDatabaseTransform((DropDatabaseStmt *)stmt);
                  break;
                case T_TruncateStmt:
                  result = TruncateTransform((TruncateStmt *)stmt);
                  break;
                case T_TransactionStmt:
                  result = TransactionTransform((TransactionStmt *)stmt);
                  break;
                case T_ExecuteStmt:
                  result = ExecuteTransform((ExecuteStmt *)stmt);
                  break;
                case T_PrepareStmt:
                  result = PrepareTransform((PrepareStmt *)stmt);
                  break;
                case T_CopyStmt:
                  result = CopyTransform((CopyStmt *)stmt);
                  break;
                case T_VacuumStmt:
                  result = VacuumTransform((VacuumStmt *)stmt);
                  break;
                case T_VariableSetStmt:
                  result = VariableSetTransform((VariableSetStmt *)stmt);
                  break;
                case T_ExplainStmt:
                  result = ExplainTransform(reinterpret_cast<ExplainStmt *>(stmt));
                  break;*/
    default: {
      char msg[MAX_EXCEPTION_MSG_LEN];
      std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "Statement type %d unsupported.\n", node->type);
      throw NotImplementedException(msg);
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
    case T_ColumnRef: {
      expr = ColumnRefTransform(reinterpret_cast<ColumnRef *>(node));
      break;
    }
    /*
    case T_ParamRef: {
      expr = ParamRefTransform(reinterpret_cast<ParamRef *>(node));
      break;
    }
    case T_FuncCall: {
      expr = FuncCallTransform(reinterpret_cast<FuncCall *>(node));
      break;
    }
    case T_CaseExpr: {
      expr = CaseExprTransform(reinterpret_cast<CaseExpr *>(node));
      break;
    }
    case T_SubLink: {
      expr = SubqueryExprTransform(reinterpret_cast<SubLink *>(node));
      break;
    }
    case T_NullTest: {
      expr = NullTestTransform(reinterpret_cast<NullTest *>(node));
      break;
    }
    case T_TypeCast: {
      expr = TypeCastTransform(reinterpret_cast<TypeCast *>(node));
      break;
    }*/
    default: {
      char msg[MAX_EXCEPTION_MSG_LEN];
      std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "Expression type %d unsupported.\n", node->type);
      throw NotImplementedException(msg);
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
  if (str == "OPERATOR_CAST") {
    return ExpressionType::OPERATOR_CAST;
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
  if (str == "CAST") {
    return ExpressionType::CAST;
  }

  char msg[MAX_EXCEPTION_MSG_LEN];
  std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "ExpressionType %s unsupported.\n", str.c_str());
  throw NotImplementedException(msg);
}

// Postgres.A_Expr -> terrier.AbstractExpression
std::unique_ptr<AbstractExpression> PostgresParser::AExprTransform(A_Expr *root) {
  // TODO(WAN): the old system says, need a function to transform strings of ops to peloton exprtype
  // e.g. > to COMPARE_GREATERTHAN
  if (root == nullptr) {
    return nullptr;
  }

  ExpressionType target_type;

  if (root->kind == AEXPR_DISTINCT) {
    target_type = ExpressionType::COMPARE_IS_DISTINCT_FROM;
  } else {
    auto name = (reinterpret_cast<value *>(root->name->head->data.ptr_value))->val.str;
    target_type = StringToExpressionType(name);
  }

  std::vector<std::shared_ptr<AbstractExpression>> children;
  children.emplace_back(ExprTransform(root->lexpr));
  children.emplace_back(ExprTransform(root->rexpr));

  switch (target_type) {
    case ExpressionType::OPERATOR_UNARY_MINUS:
    case ExpressionType::OPERATOR_PLUS:
    case ExpressionType::OPERATOR_MINUS:
    case ExpressionType::OPERATOR_MULTIPLY:
    case ExpressionType::OPERATOR_DIVIDE:
    case ExpressionType::OPERATOR_CONCAT:
    case ExpressionType::OPERATOR_MOD:
    case ExpressionType::OPERATOR_CAST:
    case ExpressionType::OPERATOR_NOT:
    case ExpressionType::OPERATOR_IS_NULL:
    case ExpressionType::OPERATOR_IS_NOT_NULL:
    case ExpressionType::OPERATOR_EXISTS: {
      return std::make_unique<OperatorExpression>(target_type, type::TypeId::INVALID, std::move(children));
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
      char msg[MAX_EXCEPTION_MSG_LEN];
      std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "AExprTransform for type %hhu unsupported.\n", target_type);
      throw NotImplementedException(msg);
    }
  }
}

// Postgres.BoolExpr -> terrier.ConjunctionExpression
std::unique_ptr<AbstractExpression> PostgresParser::BoolExprTransform(BoolExpr *root) {
  std::unique_ptr<AbstractExpression> result;
  for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
    std::vector<std::shared_ptr<AbstractExpression>> children;
    auto node = reinterpret_cast<Node *>(cell->data.ptr_value);
    children.emplace_back(ExprTransform(node));

    switch (root->boolop) {
      case AND_EXPR: {
        if (children.size() == 2) {
          result = std::make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(children));
        }
        break;
      }
      case OR_EXPR: {
        if (children.size() == 2) {
          result = std::make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(children));
        }
        break;
      }
      case NOT_EXPR: {
        result = std::make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT, type::TypeId::INVALID,
                                                      std::move(children));
        break;
      }
      default: {
        char msg[MAX_EXCEPTION_MSG_LEN];
        std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "BoolExprTransform for type %d unsupported.\n", root->boolop);
        throw NotImplementedException(msg);
      }
    }
  }
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
      char msg[MAX_EXCEPTION_MSG_LEN];
      std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "ColumnRef type %d unsupported.\n", node->type);
      throw NotImplementedException(msg);
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
    default: {
      char msg[MAX_EXCEPTION_MSG_LEN];
      std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "ArgExpr type %d unsupported.\n", root->arg->type);
      throw NotImplementedException(msg);
    }
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
    default: {
      char msg[MAX_EXCEPTION_MSG_LEN];
      std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "Sublink type %d unsupported.\n", node->subLinkType);
      throw NotImplementedException(msg);
    }
  }

  return result;
}



// Postgres.TypeCast -> terrier.ConstantValueExpression | ...
std::unique_ptr<AbstractExpression> PostgresParser::TypeCastTransform(TypeCast *root) {
  switch (root->arg->type) {
    /*
    case T_A_Const: {
      auto const_trans = ConstTransform(reinterpret_cast<A_Const *>(root->arg));
      auto const_expr = dynamic_cast<ConstantValueExpression *>(const_trans.get());
      auto source_value = const_expr->GetValue();
      auto type_name = (reinterpret_cast<value *>(root->typeName->names->tail->data.ptr_value)->val.str;
      auto type = ColumnDefinition::StrToValueType(type_name);
      //auto val = cast source to type;
      auto result = std::make_unique<ConstantValueExpression>(val);
      return result;
      break;
    }
    */
    default: {
      char msg[MAX_EXCEPTION_MSG_LEN];
      std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "TypeCast for type %d unsupported.\n", root->arg->type);
      throw NotImplementedException(msg);
    }
  }
}

// Postgres.value -> terrier.ConstantValueExpression
std::unique_ptr<AbstractExpression> PostgresParser::ValueTransform(value val) {
  std::unique_ptr<AbstractExpression> result;
  switch (val.type) {
    case T_Integer: {
      auto v = type::ValueFactory::GetIntegerValue(val.val.ival);
      result = std::make_unique<ConstantValueExpression>(v);
      break;
    }
    /*
     * case T_String:
     * TODO(WAN): do we support strings? val.val.str
     */
    case T_Float: {
      auto v = type::ValueFactory::GetDecimalValue(std::stod(val.val.str));
      result = std::make_unique<ConstantValueExpression>(v);
      break;
    }
    /*
     * case T_Null:
     * TODO(WAN): what was this? type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER)
     */
    default: {
      char msg[MAX_EXCEPTION_MSG_LEN];
      std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "Value type %d unsupported.\n", val.type);
      throw NotImplementedException(msg);
    }
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
      result->union_select_ = SelectTransform(root->rarg);
      break;
    }
    default: {
      char msg[MAX_EXCEPTION_MSG_LEN];
      std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "Set operation %d unsupported.\n", root->type);
      throw NotImplementedException(msg);
    }
  }

  return result;
}

// Postgres.SelectStmt.whereClause -> terrier.SelectStatement.select_
std::vector<std::unique_ptr<AbstractExpression>> PostgresParser::TargetTransform(List *root) {
  // Postgres parses 'SELECT;' to nullptr
  if (root == nullptr) {
    throw ParserException("TargetTransform: error parsing SQL.");
  }

  std::vector<std::unique_ptr<AbstractExpression>> result;
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
    std::vector<std::unique_ptr<TableRef>> refs;
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
        default: {
          char msg[MAX_EXCEPTION_MSG_LEN];
          std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "FromType %d unsupported.\n", node->type);
          throw NotImplementedException(msg);
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
    default: {
      char msg[MAX_EXCEPTION_MSG_LEN];
      std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "FromType %d unsupported.\n", node->type);
      throw NotImplementedException(msg);
    }
  }

  return result;
}

// Postgres.SelectStmt.groupClause -> terrier.GroupByDescription
std::unique_ptr<GroupByDescription> PostgresParser::GroupByTransform(List *group, Node *having_node) {
  if (group == nullptr) {
    return nullptr;
  }

  std::vector<std::unique_ptr<AbstractExpression>> columns;
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
  std::vector<std::unique_ptr<AbstractExpression>> exprs;

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
          default: {
            char msg[MAX_EXCEPTION_MSG_LEN];
            std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "SortBy type %d unsupported\n", sort->sortby_dir);
            throw NotImplementedException(msg);
          }
        }

        auto target = sort->node;
        exprs.emplace_back(ExprTransform(target));
      }
      default: {
        char msg[MAX_EXCEPTION_MSG_LEN];
        std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "OrderBy type %d unsupported\n", temp->type);
        throw NotImplementedException(msg);
      }
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
    default: {
      char msg[MAX_EXCEPTION_MSG_LEN];
      std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "JoinType %d unsupported\n", root->jointype);
      throw NotImplementedException(msg);
    }
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
    default: {
      char msg[MAX_EXCEPTION_MSG_LEN];
      std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "Left JoinArgType %d unsupported\n", root->larg->type);
      throw NotImplementedException(msg);
    }
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
    default: {
      char msg[MAX_EXCEPTION_MSG_LEN];
      std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "Right JoinArgType %d unsupported\n", root->rarg->type);
      throw NotImplementedException(msg);
    }
  }

  std::unique_ptr<AbstractExpression> condition;
  switch (root->quals->type) {
    case T_A_Expr: {
      condition = AExprTransform(reinterpret_cast<A_Expr *>(root->quals));
      break;
    }
    case T_BoolExpr: {
      condition = BoolExprTransform(reinterpret_cast<BoolExpr *>(root->quals));
      break;
    }
    default: {
      char msg[MAX_EXCEPTION_MSG_LEN];
      std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "Join condition type %d unsupported\n", root->quals->type);
      throw NotImplementedException(msg);
    }
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
// Postgres.CreateStmt -> terrier.CreateStatement
std::unique_ptr<SQLStatement> PostgresParser::CreateTransform(CreateStmt *root) {
  RangeVar *relation = root->relation;
  auto table_name = relation->relname != nullptr ? relation->relname : "";
  auto schema_name = relation->schemaname != nullptr ? relation->schemaname : "";
  auto database_name = relation->schemaname != nullptr ? relation->catalogname : "";
  std::unique_ptr<TableInfo> table_info = std::make_unique<TableInfo>(table_name, schema_name, database_name);

  std::unordered_set<std::string> primary_keys;

  std::vector<std::unique_ptr<ColumnDefinition>> columns;
  std::vector<std::unique_ptr<ColumnDefinition>> foreign_keys;

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

            auto fk = std::make_unique<ColumnDefinition>(std::move(fk_sources), std::move(fk_sinks),
                                                         std::move(fk_sink_table_name), fk_delete_action,
                                                         fk_update_action, fk_match_type);

            foreign_keys.emplace_back(std::move(fk));
            break;
          }
          default: {
            char msg[MAX_EXCEPTION_MSG_LEN];
            std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "Constraint %d unsupported\n", constraint->contype);
            throw NotImplementedException(msg);
          }
        }
        break;
      }
      default: {
        char msg[MAX_EXCEPTION_MSG_LEN];
        std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "tableElt type %d unsupported\n", node->type);
        throw NotImplementedException(msg);
      }
    }
  }

  // TODO(WAN): had to un-const is_primary, this is hacky
  // enforce primary keys
  for (auto &column : columns) {
    // skip foreign key constraint
    if (column->name_.empty()) {
      continue;
    }
    if (primary_keys.find(column->name_) != primary_keys.end()) {
      column->is_primary_ = true;
    }
  }

  auto result = std::make_unique<CreateStatement>(std::move(table_info), CreateStatement::CreateType::kTable,
                                                  std::move(columns), std::move(foreign_keys));
  return result;
}

// Postgres.CreateDatabaseStmt -> terrier.CreateStatement
std::unique_ptr<parser::SQLStatement> PostgresParser::CreateDatabaseTransform(CreateDatabaseStmt *root) {
  auto table_info = std::make_unique<TableInfo>("", "", root->dbname);
  std::vector<std::unique_ptr<ColumnDefinition>> columns;
  std::vector<std::unique_ptr<ColumnDefinition>> foreign_keys;
  auto result = std::make_unique<CreateStatement>(std::move(table_info), CreateStatement::kDatabase, std::move(columns),
                                                  std::move(foreign_keys));

  // TODO(WAN): per the old system, more options need to be converted
  // see postgresparser.h and the postgresql docs
  return result;
}

// Postgres.CreateFunctionStmt -> terrier.CreateFunctionStatement
std::unique_ptr<SQLStatement> PostgresParser::CreateFunctionTransform(CreateFunctionStmt *root) {



  UNUSED_ATTRIBUTE CreateFunctionStmt *temp = root;
  parser::CreateFunctionStatement *result = new CreateFunctionStatement();

  result->replace = root->replace;
  // FunctionParameter* parameters = root->parameters;

  result->func_parameters = new std::vector<FuncParameter *>();
  for (auto cell = root->parameters->head; cell != nullptr; cell = cell->next) {
    Node *node = reinterpret_cast<Node *>(cell->data.ptr_value);
    if ((node->type) == T_FunctionParameter) {
      // Transform Function Parameter
      FuncParameter *funcpar_temp = FunctionParameterTransform(
          reinterpret_cast<FunctionParameter *>(node));

      result->func_parameters->push_back(funcpar_temp);
    }
  }

  ReturnType *ret_temp =
      ReturnTypeTransform(reinterpret_cast<TypeName *>(root->returnType));
  result->return_type = ret_temp;

  // Assuming only one function name can be passed for now.
  char *name = (reinterpret_cast<value *>(root->funcname->tail->data.ptr_value)
      ->val.str);
  std::string func_name_string(name);
  result->function_name = func_name_string;

  // handle options
  for (auto cell = root->options->head; cell != NULL; cell = cell->next) {
    auto def_elem = reinterpret_cast<DefElem *>(cell->data.ptr_value);
    if (strcmp(def_elem->defname, "as") == 0) {
      auto list_of_arg = reinterpret_cast<List *>(def_elem->arg);

      for (auto cell2 = list_of_arg->head; cell2 != NULL; cell2 = cell2->next) {
        auto query_string =
            reinterpret_cast<value *>(cell2->data.ptr_value)->val.str;
        std::string new_func_body(query_string);
        result->function_body.push_back(new_func_body);
      }
      result->set_as_type();
    } else if (strcmp(def_elem->defname, "language") == 0) {
      auto lang = reinterpret_cast<value *>(def_elem->arg)->val.str;
      if ((strcmp(lang, "plpgsql") == 0)) {
        result->language = PLType::PL_PGSQL;
      } else if (strcmp(name, "c") == 0) {
        result->language = PLType::PL_C;
      }
    }
  }

  return reinterpret_cast<parser::SQLStatement *>(result);
}

// Postgres.ColumnDef -> terrier.ColumnDefinition
PostgresParser::ColumnDefTransResult PostgresParser::ColumnDefTransform(ColumnDef *root) {
  auto type_name = root->typeName;

  // handle varlen
  size_t varlen = 0;
  if (type_name->typmods) {
    auto node = reinterpret_cast<Node *>(type_name->typmods->head->data.ptr_value);
    switch (node->type) {
      case T_A_Const: {
        auto node_type = reinterpret_cast<A_Const *>(node)->val.type;
        switch (node_type) {
          case T_Integer: {
            varlen = static_cast<size_t>(reinterpret_cast<A_Const *>(node)->val.val.ival);
            break;
          }
          default: {
            char msg[MAX_EXCEPTION_MSG_LEN];
            std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "typmods %d unsupported\n", node_type);
            throw NotImplementedException(msg);
          }
        }
        break;
      }
      default: {
        char msg[MAX_EXCEPTION_MSG_LEN];
        std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "typmods %d unsupported\n", node->type);
        throw NotImplementedException(msg);
      }
    }
  }

  auto datatype_name = reinterpret_cast<value *>(type_name->names->tail->data.ptr_value)->val.str;
  auto datatype = ColumnDefinition::StrToDataType(datatype_name);

  std::vector<std::unique_ptr<ColumnDefinition>> foreign_keys;

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
            char msg[MAX_EXCEPTION_MSG_LEN];
            std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "Foreign key columns unspecified\n");
            throw NotImplementedException(msg);
          } else {
            auto attr_cell = constraint->pk_attrs->head;
            auto attr_val = reinterpret_cast<value *>(attr_cell->data.ptr_value);
            fk_sinks.emplace_back(attr_val->val.str);
            fk_sources.emplace_back(root->colname);
          }

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
        default: {
          char msg[MAX_EXCEPTION_MSG_LEN];
          std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "Constraint %d unsupported\n", constraint->contype);
          throw NotImplementedException(msg);
        }
      }
    }
  }

  auto name = root->colname;
  auto result = std::make_unique<ColumnDefinition>(name, datatype, is_primary, is_not_null, is_unique,
                                                   std::move(default_expr), std::move(check_expr), varlen);

  return {std::move(result), std::move(foreign_keys)};
}
*/
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
  std::unique_ptr<std::vector<std::string>> result;

  if (root == nullptr) {
    return result;
  }

  for (auto cell = root->head; cell != nullptr; cell = cell->next) {
    auto target = reinterpret_cast<ResTarget *>(cell->data.ptr_value);
    result->push_back(target->name);
  }

  return result;
}

// Transforms value lists into terrier equivalent. Nested vectors because an InsertStmt may insert multiple tuples.
std::unique_ptr<std::vector<std::vector<std::unique_ptr<AbstractExpression>>>> PostgresParser::ValueListsTransform(
    List *root) {
  std::unique_ptr<std::vector<std::vector<std::unique_ptr<AbstractExpression>>>> result;

  for (auto value_list = root->head; value_list != nullptr; value_list = value_list->next) {
    std::vector<std::unique_ptr<AbstractExpression>> cur_result;

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
        default: {
          char msg[MAX_EXCEPTION_MSG_LEN];
          std::snprintf(msg, MAX_EXCEPTION_MSG_LEN, "Value type %d unsupported\n", expr->type);
          throw NotImplementedException(msg);
        }
      }
    }
    result->push_back(std::move(cur_result));
  }

  return result;
}

/*




// Get in a target list and check if is with variables
bool IsTargetListWithVariable(List *target_list) {
  // The only valid situation of a null from list is that all targets are
  // constant
  for (auto cell = target_list->head; cell != nullptr; cell = cell->next) {
    ResTarget *target = reinterpret_cast<ResTarget *>(cell->data.ptr_value);
    LOG_TRACE("Type: %d", target->type);
    // Bypass the target nodes with type:
    // constant("SELECT 1;"), expression ("SELECT 1 + 1"),
    // and boolean ("SELECT 1!=2;");
    // TODO: We may want to see if there are more types to check.
    switch (target->val->type) {
      case T_A_Const:
      case T_A_Expr:
      case T_BoolExpr:
        continue;
      default:
        LOG_DEBUG("HERE");
        return true;
    }
  }
  return false;
}


// This function takes in the Case Expression of a Postgres SelectStmt
// parsenode and transfers it into Peloton AbstractExpression.
expression::AbstractExpression *PostgresParser::CaseExprTransform(
    CaseExpr *root) {
  if (root == nullptr) {
    return nullptr;
  }

  // Transform the CASE argument
  auto arg_expr = ExprTransform(reinterpret_cast<Node *>(root->arg));

  // Transform the WHEN conditions
  std::vector<expression::CaseExpression::WhenClause> clauses;
  for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
    CaseWhen *w = reinterpret_cast<CaseWhen *>(cell->data.ptr_value);

    // When condition
    auto when_expr = ExprTransform(reinterpret_cast<Node *>(w->expr));
    ;

    // Result
    auto result_expr = ExprTransform(reinterpret_cast<Node *>(w->result));

    // Build When Clause and add it to the list
    clauses.push_back(expression::CaseExpression::WhenClause(
        expression::CaseExpression::AbsExprPtr(when_expr),
        expression::CaseExpression::AbsExprPtr(result_expr)));
  }

  // Transform the default result
  auto defresult_expr =
      ExprTransform(reinterpret_cast<Node *>(root->defresult));

  // Build Case Expression
  return arg_expr != nullptr
             ? new expression::CaseExpression(
                   clauses.at(0).second.get()->GetValueType(),
                   expression::CaseExpression::AbsExprPtr(arg_expr), clauses,
                   expression::CaseExpression::AbsExprPtr(defresult_expr))
             : new expression::CaseExpression(
                   clauses.at(0).second.get()->GetValueType(), clauses,
                   expression::CaseExpression::AbsExprPtr(defresult_expr));
}






expression::AbstractExpression *PostgresParser::FuncCallTransform(
    FuncCall *root) {
  expression::AbstractExpression *result = nullptr;
  std::string fun_name = StringUtil::Lower(
      (reinterpret_cast<value *>(root->funcname->head->data.ptr_value))
          ->val.str);

  if (!IsAggregateFunction(fun_name)) {
    // Normal functions (i.e. built-in functions or UDFs)
    fun_name = (reinterpret_cast<value *>(root->funcname->tail->data.ptr_value))
                   ->val.str;
    std::vector<expression::AbstractExpression *> children;
    if (root->args != nullptr) {
      for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
        auto expr_node = (Node *)cell->data.ptr_value;
        expression::AbstractExpression *child_expr = nullptr;
        try {
          child_expr = ExprTransform(expr_node);
        } catch (NotImplementedException e) {
          throw NotImplementedException(StringUtil::Format(
              "Exception thrown in function expr:\n%s", e.what()));
        }
        children.push_back(child_expr);
      }
    }
    result = new expression::FunctionExpression(fun_name.c_str(), children);
  } else {
    // Aggregate function
    auto agg_fun_type = StringToExpressionType("AGGREGATE_" + fun_name);
    if (root->agg_star) {
      expression::AbstractExpression *children =
          new expression::StarExpression();
      result =
          new expression::AggregateExpression(agg_fun_type, false, children);
    } else {
      if (root->args->length < 2) {
        // auto children_expr_list = TargetTransform(root->args);
        expression::AbstractExpression *child;
        auto expr_node = (Node *)root->args->head->data.ptr_value;
        try {
          child = ExprTransform(expr_node);
        } catch (NotImplementedException e) {
          throw NotImplementedException(StringUtil::Format(
              "Exception thrown in aggregation function:\n%s", e.what()));
        }
        result = new expression::AggregateExpression(agg_fun_type,
                                                     root->agg_distinct, child);
      } else {
        throw NotImplementedException(
            "Aggregation over multiple columns not supported yet...\n");
      }
    }
  }
  return result;
}


// This function takes in the whenClause part of a Postgres CreateTrigStmt
// parsenode and transfers it into Peloton AbstractExpression.
expression::AbstractExpression *PostgresParser::WhenTransform(Node *root) {
  if (root == nullptr) {
    return nullptr;
  }
  expression::AbstractExpression *result = nullptr;
  switch (root->type) {
    case T_A_Expr: {
      result = AExprTransform(reinterpret_cast<A_Expr *>(root));
      break;
    }
    case T_BoolExpr: {
      result = BoolExprTransform(reinterpret_cast<BoolExpr *>(root));
      break;
    }
    default: {
      throw NotImplementedException(StringUtil::Format(
          "WHEN of type %d not supported yet...", root->type));
    }
  }
  return result;
}



// This helper function takes in a Postgres FunctionParameter object and
// transforms it into a Peloton FunctionParameter object
parser::FuncParameter *PostgresParser::FunctionParameterTransform(
    FunctionParameter *root) {
  parser::FuncParameter::DataType data_type;
  TypeName *type_name = root->argType;
  char *name =
      (reinterpret_cast<value *>(type_name->names->tail->data.ptr_value)
           ->val.str);
  parser::FuncParameter *result = nullptr;

  // Transform parameter type
  if ((strcmp(name, "int") == 0) || (strcmp(name, "int4") == 0)) {
    data_type = FuncParameter::DataType::INT;
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
    LOG_ERROR("Function Parameter DataType %s not supported yet...\n", name);
    throw NotImplementedException("...");
  }

  std::string param_name(root->name ? root->name : "");
  result = new FuncParameter(param_name, data_type);

  return result;
}

// This helper function takes in a Postgres TypeName object and transforms
// it into a Peloton ReturnType object
parser::ReturnType *PostgresParser::ReturnTypeTransform(TypeName *root) {
  parser::ReturnType::DataType data_type;
  char *name =
      (reinterpret_cast<value *>(root->names->tail->data.ptr_value)->val.str);
  parser::ReturnType *result = nullptr;

  // Transform return type
  if ((strcmp(name, "int") == 0) || (strcmp(name, "int4") == 0)) {
    data_type = FuncParameter::DataType::INT;
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
    LOG_ERROR("Return Type DataType %s not supported yet...\n", name);
    throw NotImplementedException("...");
  }

  result = new ReturnType(data_type);

  return result;
}

// This function takes in a Postgres IndexStmt parsenode
// and transfers into a Peloton CreateStatement parsenode.
// Please refer to parser/parsenode.h for the definition of
// IndexStmt parsenodes.
parser::SQLStatement *PostgresParser::CreateIndexTransform(IndexStmt *root) {
  parser::CreateStatement *result =
      new parser::CreateStatement(CreateStatement::kIndex);
  result->unique = root->unique;
  for (auto cell = root->indexParams->head; cell != nullptr;
       cell = cell->next) {
    char *index_attr =
        reinterpret_cast<IndexElem *>(cell->data.ptr_value)->name;
    result->index_attrs.push_back(std::string(index_attr));
  }
  try {
    result->index_type = StringToIndexType(std::string(root->accessMethod));
  } catch (ConversionException e) {
    delete result;
    throw e;
  }
  result->table_info_.reset(new TableInfo());
  result->table_info_->table_name = root->relation->relname;
  if (root->relation->schemaname)
    result->table_info_->schema_name = root->relation->schemaname;
  result->index_name = root->idxname;
  return result;
}

// This function takes in a Postgres CreateTrigStmt parsenode
// and transfers into a Peloton CreateStatement parsenode.
// Please refer to parser/parsenode.h for the definition of
// CreateTrigStmt parsenodes.
parser::SQLStatement *PostgresParser::CreateTriggerTransform(
    CreateTrigStmt *root) {
  parser::CreateStatement *result =
      new parser::CreateStatement(CreateStatement::kTrigger);

  // funcname
  if (root->funcname) {
    for (auto cell = root->funcname->head; cell != nullptr; cell = cell->next) {
      char *name = (reinterpret_cast<value *>(cell->data.ptr_value))->val.str;
      result->trigger_funcname.push_back(std::string(name));
    }
  }
  // args
  if (root->args) {
    for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
      char *arg = (reinterpret_cast<value *>(cell->data.ptr_value))->val.str;
      result->trigger_args.push_back(std::string(arg));
    }
  }
  // columns
  if (root->columns) {
    for (auto cell = root->columns->head; cell != nullptr; cell = cell->next) {
      char *column = (reinterpret_cast<value *>(cell->data.ptr_value))->val.str;
      result->trigger_columns.push_back(std::string(column));
    }
  }
  // when
  try {
    result->trigger_when.reset(WhenTransform(root->whenClause));
  } catch (NotImplementedException e) {
    delete result;
    throw e;
  }

  int16_t &tgtype = result->trigger_type;
  TRIGGER_CLEAR_TYPE(tgtype);
  if (root->row) TRIGGER_SETT_ROW(tgtype);
  tgtype |= root->timing;
  tgtype |= root->events;

  result->table_info_.reset(new TableInfo());
  result->table_info_->table_name = root->relation->relname;
  if (root->relation->schemaname)
    result->table_info_->schema_name = root->relation->schemaname;
  result->trigger_name = root->trigname;

  return result;
}



parser::SQLStatement *PostgresParser::CreateSchemaTransform(
    CreateSchemaStmt *root) {
  parser::CreateStatement *result =
      new parser::CreateStatement(CreateStatement::kSchema);
  result->table_info_.reset(new parser::TableInfo());

  if (root->schemaname != nullptr) {
    result->table_info_->schema_name = root->schemaname;
  }
  result->if_not_exists = root->if_not_exists;
  if (root->authrole != nullptr) {
    Node *authrole = reinterpret_cast<Node *>(root->authrole);
    if (authrole->type == T_RoleSpec) {
      RoleSpec *role = reinterpret_cast<RoleSpec *>(authrole);
      // Peloton do not need the authrole, the only usage is when no schema name
      // is specified
      if (root->schemaname == nullptr) {
        result->table_info_->schema_name = role->rolename;
      }
    } else {
      delete result;
      throw NotImplementedException(StringUtil::Format(
          "authrole of type %d is not supported yet...", authrole->type));
    }
  }
  if (root->schemaElts == nullptr) {
    return result;
  } else {
    throw NotImplementedException(
        "CREATE SCHEMA does not support schema_element yet...\n");
  }
  for (auto cell = root->schemaElts->head; cell != nullptr; cell = cell->next) {
    Node *node = reinterpret_cast<Node *>(cell->data.ptr_value);
    switch (node->type) {
      case T_CreateStmt:
        // CreateTransform((CreateStmt *)node);
        break;
      case T_ViewStmt:
        // CreateViewTransform((ViewStmt *)node);
        break;
      default:
        delete result;
        throw NotImplementedException(StringUtil::Format(
            "schemaElt of type %d not supported yet...", node->type));
    }
  }

  return result;
}

parser::SQLStatement *PostgresParser::CreateViewTransform(ViewStmt *root) {
  parser::CreateStatement *result =
      new parser::CreateStatement(CreateStatement::kView);
  result->view_name = root->view->relname;
  if (root->query->type != T_SelectStmt) {
    delete result;
    throw NotImplementedException(
        "CREATE VIEW as query only supports SELECT query...\n");
  }
  result->view_query.reset(
      SelectTransform(reinterpret_cast<SelectStmt *>(root->query)));
  return result;
}

parser::DropStatement *PostgresParser::DropTransform(DropStmt *root) {
  switch (root->removeType) {
    case ObjectType::OBJECT_TABLE:
      return DropTableTransform(root);
    case ObjectType::OBJECT_TRIGGER:
      return DropTriggerTransform(root);
    case ObjectType::OBJECT_INDEX:
      return DropIndexTransform(root);
    case ObjectType::OBJECT_SCHEMA:
      return DropSchemaTransform(root);
    default: {
      throw NotImplementedException(StringUtil::Format(
          "Drop of ObjectType %d not supported yet...\n", root->removeType));
    }
  }
}

parser::DropStatement *PostgresParser::DropDatabaseTransform(
    DropDatabaseStmt *root) {
  parser::DropStatement *result =
      new parser::DropStatement(DropStatement::kDatabase);

  result->table_info_.reset(new parser::TableInfo());
  result->table_info_->database_name = root->dbname;
  result->SetMissing(root->missing_ok);
  return result;
}

parser::DropStatement *PostgresParser::DropTableTransform(DropStmt *root) {
  auto result = new DropStatement(DropStatement::EntityType::kTable);
  result->SetMissing(root->missing_ok);

  for (auto cell = root->objects->head; cell != nullptr; cell = cell->next) {
    auto table_info = new TableInfo{};
    auto table_list = reinterpret_cast<List *>(cell->data.ptr_value);
    LOG_TRACE("%d", ((Node *)(table_list->head->data.ptr_value))->type);
    // if schema name is specified, which means you are using the syntax like
    // DROP INDEX/TABLE A.B where A is schema name and B is table/index name
    if (table_list->length == 2) {
      table_info->schema_name =
          reinterpret_cast<value *>(table_list->head->data.ptr_value)->val.str;
      table_info->table_name =
          reinterpret_cast<value *>(table_list->head->next->data.ptr_value)
              ->val.str;
    } else {
      table_info->table_name =
          reinterpret_cast<value *>(table_list->head->data.ptr_value)->val.str;
    }
    result->table_info_.reset(table_info);
    break;
  }
  return result;
}

parser::DropStatement *PostgresParser::DropTriggerTransform(DropStmt *root) {
  auto result = new DropStatement(DropStatement::EntityType::kTrigger);
  auto cell = root->objects->head;
  auto list = reinterpret_cast<List *>(cell->data.ptr_value);
  // first, set trigger name
  result->SetTriggerName(
      reinterpret_cast<value *>(list->tail->data.ptr_value)->val.str);
  TableInfo *table_info = new TableInfo{};
  // if schema name is specified, which means you are using the syntax like
  // DROP TRIGGER A.B.C where A is schema name, B is table name and C is trigger
  // name
  if (list->length == 3) {
    table_info->schema_name =
        reinterpret_cast<value *>(list->head->data.ptr_value)->val.str;
    table_info->table_name =
        reinterpret_cast<value *>(list->head->next->data.ptr_value)->val.str;
  } else if (list->length == 2) {
    table_info->table_name =
        reinterpret_cast<value *>(list->head->data.ptr_value)->val.str;
  }

  result->table_info_.reset(table_info);
  return result;
}

parser::DropStatement *PostgresParser::DropSchemaTransform(DropStmt *root) {
  auto result = new DropStatement(DropStatement::EntityType::kSchema);
  result->SetCascade(root->behavior == DropBehavior::DROP_CASCADE);
  result->SetMissing(root->missing_ok);

  result->table_info_.reset(new parser::TableInfo());
  for (auto cell = root->objects->head; cell != nullptr; cell = cell->next) {
    auto table_list = reinterpret_cast<List *>(cell->data.ptr_value);
    result->table_info_->schema_name =
        reinterpret_cast<value *>(table_list->head->data.ptr_value)->val.str;
    break;
  }
  return result;
}

// TODO: Implement other options for drop index
parser::DropStatement *PostgresParser::DropIndexTransform(DropStmt *root) {
  auto result = new DropStatement(DropStatement::EntityType::kIndex);
  auto cell = root->objects->head;
  auto list = reinterpret_cast<List *>(cell->data.ptr_value);
  // if schema name is specified, which means you are using the syntax like
  // DROP INDEX/TABLE A.B where A is schema name and B is table/index name
  if (list->length == 2) {
    TableInfo *table_info = new TableInfo{};
    table_info->schema_name =
        reinterpret_cast<value *>(list->head->data.ptr_value)->val.str;
    result->SetIndexName(
        reinterpret_cast<value *>(list->head->next->data.ptr_value)->val.str);
    result->table_info_.reset(table_info);
  } else {
    result->SetIndexName(
        reinterpret_cast<value *>(list->head->data.ptr_value)->val.str);
  }
  return result;
}

parser::DeleteStatement *PostgresParser::TruncateTransform(TruncateStmt *root) {
  auto result = new DeleteStatement();
  for (auto cell = root->relations->head; cell != nullptr; cell = cell->next) {
    result->table_ref.reset(
        RangeVarTransform(reinterpret_cast<RangeVar *>(cell->data.ptr_value)));
    break;
  }
  return result;
}

parser::ExecuteStatement *PostgresParser::ExecuteTransform(ExecuteStmt *root) {
  auto result = new ExecuteStatement();
  result->name = root->name;
  if (root->params != nullptr) try {
      result->parameters = ParamListTransform(root->params);
    } catch (NotImplementedException e) {
      delete result;
      throw e;
    }
  return result;
}

parser::PrepareStatement *PostgresParser::PrepareTransform(PrepareStmt *root) {
  auto result = new PrepareStatement();
  result->name = root->name;
  auto stmt_list = new SQLStatementList();
  try {
    stmt_list->statements.push_back(
        std::unique_ptr<SQLStatement>(NodeTransform(root->query)));
  } catch (NotImplementedException e) {
    delete stmt_list;
    delete result;
    throw e;
  }
  result->query.reset(stmt_list);
  return result;
}

parser::CopyStatement *PostgresParser::CopyTransform(CopyStmt *root) {
  static constexpr char kDelimiterTok[] = "delimiter";
  static constexpr char kFormatTok[] = "format";
  static constexpr char kQuoteTok[] = "quote";
  static constexpr char kEscapeTok[] = "escape";

  // The main return value
  auto *result = new CopyStatement();

  if (root->relation) {
    result->table.reset(RangeVarTransform(root->relation));
  } else {
    result->select_stmt.reset(
        SelectTransform(reinterpret_cast<SelectStmt *>(root->query)));
  }

  result->file_path = (root->filename != nullptr ? root->filename : "");
  result->is_from = root->is_from;

  // Handle options
  ListCell *cell = nullptr;
  for_each_cell(cell, root->options->head) {
    auto *def_elem = reinterpret_cast<DefElem *>(cell->data.ptr_value);

    // Check delimiter
    if (strncmp(def_elem->defname, kDelimiterTok, sizeof(kDelimiterTok)) == 0) {
      auto *delimiter_val = reinterpret_cast<value *>(def_elem->arg);
      result->delimiter = *delimiter_val->val.str;
    }

    // Check format
    if (strncmp(def_elem->defname, kFormatTok, sizeof(kFormatTok)) == 0) {
      auto *format_val = reinterpret_cast<value *>(def_elem->arg);
      result->format = StringToExternalFileFormat(format_val->val.str);
    }

    // Check quote
    if (strncmp(def_elem->defname, kQuoteTok, sizeof(kQuoteTok)) == 0) {
      auto *quote_val = reinterpret_cast<value *>(def_elem->arg);
      result->quote = *quote_val->val.str;
    }

    // Check escape
    if (strncmp(def_elem->defname, kEscapeTok, sizeof(kEscapeTok)) == 0) {
      auto *escape_val = reinterpret_cast<value *>(def_elem->arg);
      result->escape = *escape_val->val.str;
    }
  }

  return result;
}

// Analyze statment is parsed with vacuum statement.
parser::AnalyzeStatement *PostgresParser::VacuumTransform(VacuumStmt *root) {
  if (root->options != VACOPT_ANALYZE) {
    throw NotImplementedException("Vacuum not supported.");
  }
  auto result = new AnalyzeStatement();
  if (root->relation != NULL) {  // TOOD: check NULL vs nullptr
    result->analyze_table.reset(RangeVarTransform(root->relation));
  }
  auto analyze_columns = ColumnNameTransform(root->va_cols);
  result->analyze_columns = std::move(*analyze_columns);
  delete analyze_columns;
  return result;
}

parser::VariableSetStatement *PostgresParser::VariableSetTransform(
    UNUSED_ATTRIBUTE VariableSetStmt *root) {
  VariableSetStatement *res = new VariableSetStatement();
  return res;
}





// This function takes in the paramlist of a Postgres ExecuteStmt
// parsenode and transfers it into Peloton AbstractExpressio
std::vector<std::unique_ptr<expression::AbstractExpression>>
PostgresParser::ParamListTransform(List *root) {
  std::vector<std::unique_ptr<expression::AbstractExpression>> result =
      std::vector<std::unique_ptr<expression::AbstractExpression>>();

  for (auto cell = root->head; cell != NULL; cell = cell->next) {
    auto param = reinterpret_cast<Node *>(cell->data.ptr_value);
    switch (param->type) {
      case T_A_Const: {
        result.push_back(std::unique_ptr<expression::AbstractExpression>(
            ConstTransform((A_Const *)(cell->data.ptr_value))));
        break;
      }
      case T_A_Expr: {
        result.push_back(std::unique_ptr<expression::AbstractExpression>(
            AExprTransform((A_Expr *)(cell->data.ptr_value))));
        break;
      }
      case T_FuncCall: {
        result.push_back(std::unique_ptr<expression::AbstractExpression>(
            FuncCallTransform((FuncCall *)(cell->data.ptr_value))));
        break;
      }
      default:
        throw NotImplementedException(StringUtil::Format(
            "Expression type %d not supported in ParamListTransform yet...",
            param->type));
    }
  }

  return result;
}



parser::SQLStatement *PostgresParser::ExplainTransform(ExplainStmt *root) {
  parser::ExplainStatement *result = new parser::ExplainStatement();
  result->real_sql_stmt.reset(NodeTransform(root->query));
  return result;
}

// This function takes in a Postgres DeleteStmt parsenode
// and transfers into a Peloton DeleteStatement parsenode.
// Please refer to parser/parsenode.h for the definition of
// DeleteStmt parsenodes.
parser::SQLStatement *PostgresParser::DeleteTransform(DeleteStmt *root) {
  parser::DeleteStatement *result = new parser::DeleteStatement();
  result->table_ref.reset(RangeVarTransform(root->relation));
  try {
    result->expr.reset(WhereTransform(root->whereClause));
  } catch (NotImplementedException e) {
    delete result;
    throw e;
  }
  return (parser::SQLStatement *)result;
}

// Transform Postgres TransacStmt into Peloton TransactionStmt
parser::TransactionStatement *PostgresParser::TransactionTransform(
    TransactionStmt *root) {
  if (root->kind == TRANS_STMT_BEGIN) {
    return new parser::TransactionStatement(TransactionStatement::kBegin);
  } else if (root->kind == TRANS_STMT_COMMIT) {
    return new parser::TransactionStatement(TransactionStatement::kCommit);
  } else if (root->kind == TRANS_STMT_ROLLBACK) {
    return new parser::TransactionStatement(TransactionStatement::kRollback);
  } else {
    throw NotImplementedException(StringUtil::Format(
        "Commmand type %d not supported yet.\n", root->kind));
  }
}



// This function transfers a list of Postgres statements into
// a Peloton SQLStatementList object. It traverses the parse list
// and call the helper for singles nodes.


std::vector<std::unique_ptr<parser::UpdateClause>>
    *PostgresParser::UpdateTargetTransform(List *root) {
  auto result = new std::vector<std::unique_ptr<parser::UpdateClause>>();
  for (auto cell = root->head; cell != NULL; cell = cell->next) {
    auto update_clause = new UpdateClause();
    ResTarget *target = (ResTarget *)(cell->data.ptr_value);
    update_clause->column = target->name;
    try {
      update_clause->value.reset(ExprTransform(target->val));
    } catch (NotImplementedException e) {
      delete result;
      throw NotImplementedException(
          StringUtil::Format("Exception thrown in update expr:\n%s", e.what()));
    }
    result->push_back(std::unique_ptr<UpdateClause>(update_clause));
  }
  return result;
}

// TODO: Not support with clause, from clause and returning list in update
// statement in peloton
parser::UpdateStatement *PostgresParser::UpdateTransform(
    UpdateStmt *update_stmt) {
  auto result = new parser::UpdateStatement();
  result->table.reset(RangeVarTransform(update_stmt->relation));
  try {
    result->where.reset(WhereTransform(update_stmt->whereClause));
  } catch (NotImplementedException e) {
    delete result;
    throw e;
  }
  try {
    auto clauses = UpdateTargetTransform(update_stmt->targetList);
    result->updates = std::move(*clauses);
    delete clauses;
  } catch (NotImplementedException e) {
    delete result;
    throw e;
  }
  return result;
}

// Call postgres's parser and start transforming it into Peloton's parse tree

*/
}  // namespace parser
}  // namespace terrier
