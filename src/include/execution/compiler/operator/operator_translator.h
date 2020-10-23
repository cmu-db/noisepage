#pragma once

#include <string>
#include <type_traits>

#include "brain/brain_defs.h"
#include "common/macros.h"
#include "execution/ast/ast_fwd.h"
#include "execution/compiler/expression/column_value_provider.h"
#include "execution/compiler/state_descriptor.h"
#include "execution/exec_defs.h"
#include "execution/util/region_containers.h"

namespace terrier::brain {
class ExecutionOperatingUnitFeature;
class OperatingUnitRecorder;
}  // namespace terrier::brain

namespace terrier::parser {
class AbstractExpression;
}  // namespace terrier::parser

namespace terrier::planner {
class AbstractPlanNode;
}  // namespace terrier::planner

namespace terrier::execution::compiler {

class CodeGen;
class CompilationContext;
class FunctionBuilder;
class Pipeline;
class WorkContext;

/**
 * The base class of all operator translators.
 *
 * All operators are associated to a CompilationContext. The compilation context serves as a central
 * context object containing the query state, all operator translators, all expression translators
 * and all pipeline objects. Upon construction, it is the translators duty to prepare each of its
 * children in the provided compilation context.
 *
 * Operators must also register themselves in the pipelines. If an operator belongs to multiple
 * pipelines, it may construct new pipelines and register its children as appropriate between the
 * one or more pipelines it controls.
 *
 * State:
 * ------
 * Operators usually require some runtime state in order to implement their logic. A hash-join
 * requires a hash table, a sort requires a sorting instance, a block-nested loop join requires a
 * temporary buffer, etc. State exists at two levels: query and pipeline.
 *
 * Query-level state exists for the duration of the entire query. Operators declare and register
 * their state entries with the QueryState object in CompilationContext, initialize their state in
 * InitializeQueryState(), and destroy their state in TearDownQueryState(). All query-state based
 * operations are called exactly once per operator in the plan. Translators may not assume an
 * initialization order.
 *
 * Pipeline-level state exists only within a pipeline. Operators declare and register pipeline-local
 * state in DeclarePipelineState(), initialize it in InitializePipelineState(), and destroy in in
 * TearDownPipelineState(). Since some operators participate in multiple pipelines, a context object
 * is provided to discern <b>which pipeline</b> is being considered.
 *
 * Data-flow:
 * ----------
 * Operators participate in one or more pipelines. A pipeline represents a uni-directional data flow
 * of tuples or tuple batches. Another of way of thinking of pipelines are as a post-order walk of
 * a subtree in the query plan composing of operators that do not materialize tuple (batch) data
 * into cache or memory.
 *
 * All pipelines have a source operator and a sink operator. Tuples (batches) flow from the source
 * to the sink along a pipeline in a WorkContext. When an operator receives a work context, it is
 * a request to generate the logic of the operator (i.e., perform some work). When this work is
 * complete, it should be sent to the following operator in the pipeline.
 *
 * Operators may generate pre-work and post-work pipeline logic by overriding BeginPipelineWork()
 * and FinishPipelineWork() which are called before and after the primary/main pipeline work is
 * generated, respectively.
 *
 * Helper functions:
 * -----------------
 * Operators may require the use of helper functions or auxiliary structures in order to simplify
 * their logic. Helper functions and structures should be defined in both DefineHelperStructs() and
 * DefineHelperFunctions(), respectively. All helper structures and functions are visible across the
 * whole query and must be declared in the provided input container.
 */
class OperatorTranslator : public ColumnValueProvider {
 public:
  /**
   * Create a translator.
   * @param plan The plan node this translator will generate code for.
   * @param compilation_context The context this compilation is occurring in.
   * @param pipeline The pipeline this translator is a part of.
   * @param feature_type Minirunner execution operating unit type for this operator.
   */
  OperatorTranslator(const planner::AbstractPlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline,
                     brain::ExecutionOperatingUnitType feature_type);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(OperatorTranslator);

  /**
   * Destructor.
   */
  virtual ~OperatorTranslator() = default;

  /**
   * Define any helper structures required for processing. Ensure they're declared in the provided
   * declaration container.
   * @param decls Query-level declarations.
   */
  virtual void DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) {}

  /**
   * Define any helper functions required for processing. Ensure they're declared in the provided
   * declaration container.
   * @param decls Query-level declarations.
   */
  virtual void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) {}

  /**
   * Define any helper functions that rely on pipeline's thread local state.
   * @param pipeline Pipeline that helper functions are being generated for.
   * @param decls Query-level declarations.
   */
  virtual void DefineTLSDependentHelperFunctions(const Pipeline &pipeline,
                                                 util::RegionVector<ast::FunctionDecl *> *decls) {}

  /**
   * Initialize all query state.
   * @param function The builder for the query state initialization function.
   */
  virtual void InitializeQueryState(FunctionBuilder *function) const {}

  /**
   * Tear down all query state.
   * @param function The builder for the query state teardown function.
   */
  virtual void TearDownQueryState(FunctionBuilder *function) const {}

  /**
   * Initialize any declared pipeline-local state.
   * @param pipeline The pipeline whose state is being initialized.
   * @param function The function being built.
   */
  virtual void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {}

  /**
   * Perform any work required before beginning main pipeline work. This is executed by one thread.
   * @param pipeline The pipeline whose pre-work logic is being generated.
   * @param function The function being built.
   */
  virtual void BeginPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const {}

  /**
   * Function to initialize relevant counters.
   * @param pipeline The pipeline to initialize counters for.
   * @param function The function being built.
   */
  virtual void InitializeCounters(const Pipeline &pipeline, FunctionBuilder *function) const {}

  /**
   * Function to record relevant counters.
   * @param pipeline The pipeline to record counters for.
   * @param function The function being built.
   */
  virtual void RecordCounters(const Pipeline &pipeline, FunctionBuilder *function) const {}

  /**
   * Perform any logic that should happen at the start of a parallel work function.
   * The parallel work function is the function that is invoked by LaunchWork from the driver.
   * By default, this function re-initializes any relevant counters.
   *
   * @param pipeline The pipeline whose pre parallel work logic is being generated.
   * @param function The function being built.
   */
  virtual void BeginParallelPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const {
    InitializeCounters(pipeline, function);
  }

  /**
   * Perform any logic that should happen at the end of a parallel work function.
   * The parallel work function is the function that is invoked by LaunchWork from the driver.
   * @param pipeline The pipeline whose post parallel work logic is being generated.
   * @param function The function being built.
   */
  virtual void EndParallelPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const {
    RecordCounters(pipeline, function);
  }

  /**
   * Perform the primary logic of a pipeline. This is where the operator's logic should be
   * implemented. The provided context object contains information necessary to help operators
   * decide what work to perform. Specifically,
   *  1. The pipeline the work is for:
   *     Some operators may be connected to multiple pipelines. For example, hash joins rely on two
   *     pipelines. The provided work context indicates specifically which pipeline the work is for
   *     so that operators generate the correct code.
   *  2. Access to thread-local state:
   *     Operators may rely on pipeline-local state. The provided context allows operators to access
   *     such state using slot references they've received when declaring pipeline state in
   *     DeclarePipelineState().
   *  3. An expression evaluation mechanism:
   *     The provided work context can be used to evaluate an expression. Expression evaluation is
   *     context sensitive. The context also provides a mechanism to cache expression results.
   * @param context The context of the work.
   * @param function The function being built.
   */
  virtual void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const = 0;

  /**
   * Perform any work required <b>after</b> the main pipeline work. This is executed by one thread.
   * @param pipeline The pipeline whose post-work logic is being generated.
   * @param function The function being built.
   */
  virtual void FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const {}

  /**
   * Tear down and destroy any pipeline-local state.
   * @param pipeline The pipeline whose state is being destroyed.
   * @param function The function being built.
   */
  virtual void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {}

  /**
   * @return The value (vector) of the attribute at the given index in this operator's output.
   */
  ast::Expr *GetOutput(WorkContext *context, uint32_t attr_idx) const;

  /**
   * @return The value (vector) of the attribute at the given index (@em attr_idx) produced by the
   *         child at the given index (@em child_idx).
   */
  ast::Expr *GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const override;

  /**
   * @return The plan the translator is generating.
   */
  const planner::AbstractPlanNode &GetPlan() const { return plan_; }

  /**
   * @return The compilation context the translator is owned by.
   */
  CompilationContext *GetCompilationContext() const { return compilation_context_; }

  /** @return Feature type. */
  brain::ExecutionOperatingUnitType GetFeatureType() const { return feature_type_; }

  /** @return The plan node as a generic node. */
  const planner::AbstractPlanNode *Op() const { return &plan_; }

  /** @return The address of the current tuple slot, if any. */
  virtual ast::Expr *GetSlotAddress() const { UNREACHABLE("This translator does not deal with tupleslots."); }

 protected:
  /** Get the code generator instance. */
  CodeGen *GetCodeGen() const;

  /** Get a pointer to the query state. */
  ast::Expr *GetQueryStatePtr() const;

  /** Get the execution context pointer in the current function. */
  ast::Expr *GetExecutionContext() const;

  /** Get the thread state container pointer from the execution context stored in the query state. */
  ast::Expr *GetThreadStateContainer() const;

  /** Get the memory pool pointer from the execution context stored in the query state. */
  ast::Expr *GetMemoryPool() const;

  /** The pipeline this translator is a part of. */
  Pipeline *GetPipeline() const { return pipeline_; }

  /** The plan node for this translator as its concrete type. */
  template <typename T>
  const T &GetPlanAs() const {
    static_assert(std::is_base_of_v<planner::AbstractPlanNode, T>, "Template is not a plan node");
    return static_cast<const T &>(plan_);
  }

  /**
   * Used by operators when they need to generate a struct containing a child's output.
   * Also used by the output layer to materialize the output.
   * @param child_index The child index refers to which specific child to inspect.
   * @param field_name_prefix The prefix is added to each field/attribute of the child.
   * @param fields The fields vector collects the resulting field declarations.
   */
  void GetAllChildOutputFields(uint32_t child_index, const std::string &field_name_prefix,
                               util::RegionVector<ast::FieldDecl *> *fields) const;

  /** @return The ID of this OperatorTranslator, used for identifying features in operating unit feature vectors. */
  execution::translator_id_t GetTranslatorId() const { return translator_id_; }

  /** @return True if we should collect counters in TPL, used for Lin's models. */
  bool IsCountersEnabled() const;

  /** @return True if we should collect pipeline metrics */
  bool IsPipelineMetricsEnabled() const;

  /** @return True if this translator should pass ownership of its counters further down, used for Lin's models. */
  virtual bool IsCountersPassThrough() const { return false; }

  /** Declare a counter for Lin's models. */
  StateDescriptor::Entry CounterDeclare(const std::string &counter_name, Pipeline *pipeline) const;
  /** Set the value of a counter for Lin's models. */
  void CounterSet(FunctionBuilder *function, const StateDescriptor::Entry &counter, int64_t val) const;
  /** Set the value of a counter for Lin's models. */
  void CounterSetExpr(FunctionBuilder *function, const StateDescriptor::Entry &counter, ast::Expr *val) const;
  /** Add to the value of a counter for Lin's models. */
  void CounterAdd(FunctionBuilder *function, const StateDescriptor::Entry &counter, int64_t val) const;
  /** Add to the value of a counter for Lin's models. */
  void CounterAdd(FunctionBuilder *function, const StateDescriptor::Entry &counter, ast::Identifier val) const;
  /** Get the feature value out of a counter. */
  ast::Expr *CounterVal(StateDescriptor::Entry entry) const;
  /** Record a specified feature's value. */
  void FeatureRecord(FunctionBuilder *function, brain::ExecutionOperatingUnitType feature_type,
                     brain::ExecutionOperatingUnitFeatureAttribute attrib, const Pipeline &pipeline,
                     ast::Expr *val) const;
  /** Record arithmetic feature values by setting feature values to val. */
  void FeatureArithmeticRecordSet(FunctionBuilder *function, const Pipeline &pipeline,
                                  execution::translator_id_t translator_id, ast::Expr *val) const;
  /** Record arithmetic feature values by multiplying existing feature values by val. */
  void FeatureArithmeticRecordMul(FunctionBuilder *function, const Pipeline &pipeline,
                                  execution::translator_id_t translator_id, ast::Expr *val) const;

  /**
   * Get arguments for a hook function.
   *
   * A hook function takes in 3 arguments. The first argument is the QueryState.
   * The second argument is the thread-local state. The third argument depends
   * on the particular hook function.
   *
   * @param pipeline Pipeline that the hook is being added for
   * @param arg Third argument identifier if necessary (nullptr to indicate hook does not use the 3rd arg)
   * @param arg_type Type of the third argument if arg is specified
   * @returns hook function arguments
   */
  util::RegionVector<ast::FieldDecl *> GetHookParams(const Pipeline &pipeline, ast::Identifier *arg,
                                                     ast::Expr *arg_type) const;

 private:
  // For mini-runner stuff.
  friend class Pipeline;
  /** Set the child translator. */
  void SetChildTranslator(common::ManagedPointer<OperatorTranslator> translator) { child_translator_ = translator; }
  /** Set the parent translator.. */
  void SetParentTranslator(common::ManagedPointer<OperatorTranslator> translator) { parent_translator_ = translator; }
  friend class brain::OperatingUnitRecorder;
  /** @return The child translator. */
  common::ManagedPointer<OperatorTranslator> GetChildTranslator() const { return child_translator_; }
  /** @return The parent translator. */
  common::ManagedPointer<OperatorTranslator> GetParentTranslator() const { return parent_translator_; }

 private:
  static std::atomic<execution::translator_id_t> translator_id_counter;
  execution::translator_id_t translator_id_;

  // The plan node.
  const planner::AbstractPlanNode &plan_;
  // The compilation context.
  CompilationContext *compilation_context_;
  // The pipeline the operator belongs to.
  Pipeline *pipeline_;

  /** The child operator translator. */
  common::ManagedPointer<OperatorTranslator> child_translator_{nullptr};
  /** The parent operator translator. */
  common::ManagedPointer<OperatorTranslator> parent_translator_{nullptr};
  /** ExecutionOperatingUnitType. */
  brain::ExecutionOperatingUnitType feature_type_{brain::ExecutionOperatingUnitType::INVALID};
};

}  // namespace terrier::execution::compiler
