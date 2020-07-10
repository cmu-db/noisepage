#pragma once

namespace terrier::brain {
class OperatingUnitRecorder;
}  // namespace terrier::brain

namespace terrier::execution::ast {
class StructDecl;
}  // namespace terrier::execution::ast

namespace terrier::execution::compiler {

class Pipeline;

/**
 * Base class for aggregation translators.
 * TODO(WAN): right now we just use this to expose stuff for minirunners.
 */
class BaseAggregationTranslator {
 protected:
  friend brain::OperatingUnitRecorder;

  // Check if the input pipeline is either the build-side or producer-side.
  virtual bool IsBuildPipeline(const Pipeline &pipeline) const { return false; }
  virtual bool IsProducePipeline(const Pipeline &pipeline) const { return false; }

  // For minirunners.
  ast::StructDecl *GetStructDecl() const { return struct_decl_; }
  ast::StructDecl *struct_decl_;
};

}  // namespace terrier::execution::compiler
