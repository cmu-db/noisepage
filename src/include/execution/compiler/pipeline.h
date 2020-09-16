#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
#include "execution/ast/ast_fwd.h"
#include "execution/ast/identifier.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/state_descriptor.h"
#include "execution/exec_defs.h"
#include "execution/util/region_containers.h"

namespace terrier::execution::exec {
class ExecutionSettings;
}  // namespace terrier::execution::exec

namespace terrier::brain {
class OperatingUnitRecorder;
}  // namespace terrier::brain

namespace terrier::execution::compiler {

class CodeGen;
class CompilationContext;
class ExecutableQueryFragmentBuilder;
class ExpressionTranslator;
class OperatorTranslator;
class PipelineDriver;

/**
 * A pipeline represents an ordered sequence of relational operators that operate on tuple data
 * without explicit copying or materialization. Tuples are read at the start of the pipeline, pass
 * through each operator, and are materialized in some form only at the end of the pipeline.
 *
 * Pipelines are flexible allowing the flow of batches of tuples as well as individual tuples, thus
 * supporting vector-at-a-time (VaaT) and tuple-at-a-time (TaaT) execution. Translators composing
 * the pipeline are aware of this hybrid approach and can generate code in both paradigms.
 *
 * Pipelines form the unit of parallelism. Each pipeline can either be launched serially or in
 * parallel.
 */
class Pipeline {
 public:
  /**
   * Enum class representing a degree of parallelism. The Serial and Parallel values are clear. The
   * Flexible option should be used when both serial and parallel operation is supported, but no
   * preference is taken.
   */
  enum class Parallelism : uint8_t { Serial = 0, Parallel = 2 };

  /**
   * Enum class representing whether the pipeline is vectorized.
   */
  enum class Vectorization : uint8_t { Disabled = 0, Enabled = 1 };

  /**
   * Create an empty pipeline in the given compilation context.
   * @param ctx The compilation context the pipeline is in.
   */
  explicit Pipeline(CompilationContext *ctx);

  /**
   * Create a pipeline with the given operator as the root.
   * @param op The root operator of the pipeline.
   * @param parallelism The operator's requested parallelism.
   */
  Pipeline(OperatorTranslator *op, Parallelism parallelism);

  /**
   * Register an operator in this pipeline with a customized parallelism configuration.
   * @param op The operator to add to the pipeline.
   */
  void RegisterStep(OperatorTranslator *op);

  /**
   * Register the source/driver for the pipeline.
   * @param driver The single driver for the pipeline.
   * @param parallelism The driver's requested parallelism.
   */
  void RegisterSource(PipelineDriver *driver, Parallelism parallelism);

  /**
   * Update the current parallelism level for this pipeline to the value provided.
   * @param parallelism The desired parallelism level.
   */
  void UpdateParallelism(Parallelism parallelism);

  /**
   * Enable or disable the pipeline's parallelism check during register RegisterStep.
   * @param check Whether the to check for parallelism or not.
   */
  void SetParallelCheck(bool check);

  /**
   * Register an expression in this pipeline. This expression may or may not create/destroy state.
   * @param expression The expression to register.
   */
  void RegisterExpression(ExpressionTranslator *expression);

  /**
   * Declare an entry in this pipeline's state.
   * @param name The name of the element.
   * @param type_repr The TPL type representation of the element.
   * @return The slot where the inserted state exists.
   */
  StateDescriptor::Entry DeclarePipelineStateEntry(const std::string &name, ast::Expr *type_repr);

  /**
   * Register the provided pipeline as a dependency for this pipeline. In other words, this pipeline
   * cannot begin until the provided pipeline completes.
   * @param dependency Another pipeline this pipeline is dependent on.
   */
  void LinkSourcePipeline(Pipeline *dependency);

  /**
   * Store in the provided output vector the set of all dependencies for this pipeline. In other
   * words, store in the output vector all pipelines that must execute (in order) before this
   * pipeline can begin.
   * @param[out] deps The sorted list of pipelines to execute before this pipeline can begin.
   */
  void CollectDependencies(std::vector<Pipeline *> *deps);

  /**
   * Perform initialization logic before code generation.
   * @param exec_settings The execution settings used for query compilation.
   */
  void Prepare(const exec::ExecutionSettings &exec_settings);

  /**
   * Generate all functions to execute this pipeline in the provided container.
   * @param builder The builder for the executable query container.
   * @param query_id The ID of the query that generates this pipeline.
   */
  void GeneratePipeline(ExecutableQueryFragmentBuilder *builder, query_id_t query_id) const;

  /**
   * @return True if the pipeline is parallel; false otherwise.
   */
  bool IsParallel() const { return parallelism_ == Parallelism ::Parallel; }

  /**
   * @return True if this pipeline is fully vectorized; false otherwise.
   */
  bool IsVectorized() const { return false; }

  /**
   * Typedef used to specify an iterator over the steps in a pipeline.
   */
  using StepIterator = std::vector<OperatorTranslator *>::const_reverse_iterator;

  /**
   * @return An iterator over the operators in the pipeline.
   */
  StepIterator Begin() const { return steps_.rbegin(); }

  /**
   * @return An iterator positioned at the end of the operators steps in the pipeline.
   */
  StepIterator End() const { return steps_.rend(); }

  /**
   * @return True if the given operator is the driver for this pipeline; false otherwise.
   */
  bool IsDriver(const PipelineDriver *driver) const { return driver == driver_; }

  /**
   * @return Arguments common to all pipeline functions.
   */
  util::RegionVector<ast::FieldDecl *> PipelineParams() const;

  /**
   * @return A unique name for a function local to this pipeline.
   */
  std::string CreatePipelineFunctionName(const std::string &func_name) const;

  /** @return The unique ID of this pipeline. */
  pipeline_id_t GetPipelineId() const { return pipeline_id_t{id_}; }

 private:
  // Return the thread-local state initialization and tear-down function names.
  // This is needed when we invoke @tlsReset() from the pipeline initialization
  // function to setup the thread-local state.
  ast::Identifier GetSetupPipelineStateFunctionName() const;
  ast::Identifier GetTearDownPipelineStateFunctionName() const;
  ast::Identifier GetWorkFunctionName() const;

  // Generate the pipeline state initialization logic.
  ast::FunctionDecl *GenerateSetupPipelineStateFunction() const;

  // Generate the pipeline state cleanup logic.
  ast::FunctionDecl *GenerateTearDownPipelineStateFunction() const;

  // Generate pipeline initialization logic.
  ast::FunctionDecl *GenerateInitPipelineFunction() const;

  // Generate the main pipeline work function.
  ast::FunctionDecl *GeneratePipelineWorkFunction() const;

  // Generate the main pipeline logic.
  ast::FunctionDecl *GenerateRunPipelineFunction(query_id_t query_id) const;

  // Generate pipeline tear-down logic.
  ast::FunctionDecl *GenerateTearDownPipelineFunction() const;

 private:
  // Internals which are exposed for minirunners.
  friend class compiler::CompilationContext;
  friend class brain::OperatingUnitRecorder;

  /** @return The vector of pipeline operators that make up the pipeline. */
  const std::vector<OperatorTranslator *> &GetTranslators() const { return steps_; }

  void InjectStartPipelineTracker(FunctionBuilder *builder) const;

  void InjectEndResourceTracker(FunctionBuilder *builder, query_id_t query_id) const;

 private:
  // A unique pipeline ID.
  uint32_t id_;
  // The compilation context this pipeline is part of.
  CompilationContext *compilation_context_;
  // The code generation instance.
  CodeGen *codegen_;
  // Operators making up the pipeline.
  std::vector<OperatorTranslator *> steps_;
  // The driver.
  PipelineDriver *driver_;
  // Expressions participating in the pipeline.
  std::vector<ExpressionTranslator *> expressions_;
  // Configured parallelism.
  Parallelism parallelism_;
  // Whether to check for parallelism in new pipeline elements.
  bool check_parallelism_;
  // All pipelines this one depends on completion of.
  std::vector<Pipeline *> dependencies_;
  // Cache of common identifiers.
  ast::Identifier state_var_;
  // The pipeline state.
  StateDescriptor state_;
};

}  // namespace terrier::execution::compiler
