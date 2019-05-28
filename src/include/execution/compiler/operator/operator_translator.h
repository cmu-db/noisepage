#pragma once

namespace terrier::planner {
class AbstractPlanNode;
}

namespace tpl::compiler {

class ConsumerContext;
class Pipeline;
class RowBatch;

/**
 * Generic Operator Translator
 */
class OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op operator to transalate
   * @param pipeline current pipeline
   */
  OperatorTranslator(const terrier::planner::AbstractPlanNode &op, Pipeline *pipeline)
      : plannode_(op), pipeline_(pipeline) {}

  /// Destructor
  virtual ~OperatorTranslator() = default;

  /**
   * Initialize query state.
   * Each operator may define query state fields as well as top level structs
   * (e.g. output struct, insert struct, index struct, ...)
   */
  virtual void InitializeQueryState() = 0;

  /**
   * Teardown query state.
   * Each operate should clean up any state that is left.
   * (e.g. flush output buffer)
   */
  virtual void TeardownQueryState() = 0;

  /**
   * Generates produce code.
   */
  virtual void Produce() = 0;

  /**
   * Generates consume code.
   * @param context consumer context to use
   * @param batch tuple to consume
   */
  virtual void Consume(const ConsumerContext *context, RowBatch *batch) const = 0;

  /**
   * Casts an operator to a given type
   * @tparam T type to cast to
   * @return the casted operator
   */
  template <typename T>
  const T &GetOperatorAs() const {
    return static_cast<const T &>(plannode_);
  }

 protected:
  /// Plan node to translate
  const terrier::planner::AbstractPlanNode &plannode_;
  /// current pipeline
  Pipeline *pipeline_;
};

}  // namespace tpl::compiler
