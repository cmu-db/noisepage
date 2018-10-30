#pragma once

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
class OperatorUnaryMinusExpression;
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

  virtual void Visit(ComparisonExpression *expr);
  virtual void Visit(AggregateExpression *expr);
  virtual void Visit(CaseExpression *expr);
  virtual void Visit(ConjunctionExpression *expr);
  virtual void Visit(ConstantValueExpression *expr);
  virtual void Visit(FunctionExpression *expr);
  virtual void Visit(OperatorExpression *expr);
  virtual void Visit(OperatorUnaryMinusExpression *expr);
  virtual void Visit(ParameterValueExpression *expr);
  virtual void Visit(StarExpression *expr);
  virtual void Visit(TupleValueExpression *expr);
  virtual void Visit(SubqueryExpression *expr);
};
}  // namespace terrier::parser
