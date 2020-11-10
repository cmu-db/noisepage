#pragma once

#include "execution/ast/identifier.h"
#include "execution/compiler/ast_fwd.h"
#include "execution/util/region_containers.h"

namespace noisepage::execution::compiler {

class FunctionBuilder;
class Pipeline;

/**
 * Interface for any operator that drives a pipeline. Pipeline drivers are responsible for launching
 * parallel work.
 */
class PipelineDriver {
 public:
  /**
   * No-op virtual destructor.
   */
  virtual ~PipelineDriver() = default;

  /**
   * @return The list of extra fields added to the "work" function. By default, the first two
   *         arguments are the query state and the pipeline state.
   */
  virtual util::RegionVector<ast::FieldDecl *> GetWorkerParams() const = 0;

  /**
   * This is called to launch the provided worker function in parallel across a set of threads.
   * @param function The function being built.
   * @param work_func_name The name of the work function that implements the pipeline logic.
   */
  virtual void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const = 0;
};

}  // namespace noisepage::execution::compiler
