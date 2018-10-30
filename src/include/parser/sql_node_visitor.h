#pragma once

#include <sql/expression/sql_abstract_expression.h>

namespace terrier::parser {
class SelectStatement;
class CreateStatement;
class CreateFunctionStatement;
class InsertStatement;
class DeleteStatement;
class DropStatement;
class ExplainStatement;
class PrepareStatement;
class ExecuteStatement;
class TransactionStatement;
class UpdateStatement;
class CopyStatement;
class AnalyzeStatement;
class VariableSetStatement;
class JoinDefinition;
struct TableRef;

class GroupByDescription;
class OrderDescription;
class LimitDescription;

class AbstractExpression;
class ComparisonExpression;
class AggregateExpression;
class ConjunctionExpression;
class ConstantValueExpression;
class OperatorExpression;
class ParameterValueExpression;
class StarExpression;
class TupleValueExpression;
class FunctionExpression;
class CaseExpression;
class SubqueryExpression;


class SqlNodeVisitor {
 public:
  virtual ~SqlNodeVisitor() {};
  virtual void Visit(JoinDefinition *) {}
  virtual void Visit(TableRef *) {}
  virtual void Visit(GroupByDescription *) {}
  virtual void Visit(OrderDescription *) {}
  virtual void Visit(LimitDescription *) {}

  virtual void Visit(CreateStatement *) {}
  virtual void Visit(CreateFunctionStatement *) {}
  virtual void Visit(InsertStatement *) {}
  virtual void Visit(DeleteStatement *) {}
  virtual void Visit(DropStatement *) {}
  virtual void Visit(PrepareStatement *) {}
  virtual void Visit(ExecuteStatement *) {}
  virtual void Visit(TransactionStatement *) {}
  virtual void Visit(UpdateStatement *) {}
  virtual void Visit(CopyStatement *) {}
  virtual void Visit(AnalyzeStatement *){};
  virtual void Visit(ExplainStatement *){};

  virtual std::vector<std::shared_ptr<sql::SqlAbstractExpression>> Visit(ComparisonExpression *expr);
  virtual std::vector<std::shared_ptr<sql::SqlAbstractExpression>> Visit(AggregateExpression *expr);
  virtual std::vector<std::shared_ptr<sql::SqlAbstractExpression>> Visit(CaseExpression *expr);
  virtual std::vector<std::shared_ptr<sql::SqlAbstractExpression>> Visit(ConjunctionExpression *expr);
  virtual std::vector<std::shared_ptr<sql::SqlAbstractExpression>> Visit(ConstantValueExpression *expr);
  virtual std::vector<std::shared_ptr<sql::SqlAbstractExpression>> Visit(FunctionExpression *expr);
  virtual std::vector<std::shared_ptr<sql::SqlAbstractExpression>> Visit(OperatorExpression *expr);
  virtual std::vector<std::shared_ptr<sql::SqlAbstractExpression>> Visit(ParameterValueExpression *expr);
  virtual std::vector<std::shared_ptr<sql::SqlAbstractExpression>> Visit(StarExpression *expr);
  virtual std::vector<std::shared_ptr<sql::SqlAbstractExpression>> Visit(TupleValueExpression *expr);
  virtual std::vector<std::shared_ptr<sql::SqlAbstractExpression>> Visit(SubqueryExpression *expr);
};
}  // namespace terrier::parser
