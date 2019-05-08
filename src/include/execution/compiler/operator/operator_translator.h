#pragma once

namespace terrier::planner {
class AbstractPlanNode;
}

namespace tpl::compiler {

class ConsumerContext;
class Pipeline;
class RowBatch;

class OperatorTranslator {
 public:
  OperatorTranslator(const terrier::planner::AbstractPlanNode &op, Pipeline *pipeline) : pipeline_(pipeline) {}
  virtual ~OperatorTranslator() = default;

  virtual void InitializeQueryState() = 0;
  virtual void TeardownQueryState() = 0;

  virtual void Produce() = 0;

  virtual void Consume(const ConsumerContext *context, RowBatch &batch) const = 0;

 protected:
  Pipeline *pipeline_;
};

}