#pragma once

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "execution/compiler/operator/seq_scan_translator.h"
#include "planner/plannodes/cte_scan_plan_node.h"

namespace noisepage::execution::compiler {

/**
 * CteScan Translator
 */
class CteScanTranslator : public SeqScanTranslator {
 public:
  /**
   * Create a translator for the given plan.
   * @param plan The plan.
   * @param compilation_context The context this translator belongs to.
   * @param pipeline The pipeline this translator is participating in.
   */
  CteScanTranslator(const planner::CteScanPlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(CteScanTranslator);
};

}  // namespace noisepage::execution::compiler
