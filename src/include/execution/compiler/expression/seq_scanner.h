#pragma once

#include "execution/compiler/function_builder.h"

namespace terrier::execution::compiler {
/**
 * seq_scanner is a utility function for sequantial scan
 */
class SeqScanner {
 public:
  SeqScanner(CodeGen *codegen)
      : codegen_(codegen),
        tvi_(codegen_->NewIdentifier("tvi")),
        pci_(codegen_->NewIdentifier("pci")),
        slot_(codegen_->NewIdentifier("slot")){};
  void DeclareTVI(FunctionBuilder *builder, uint32_t table_oid, ast::Identifier col_oid);

  void TVILoop(FunctionBuilder *builder);

  void DeclarePCI(FunctionBuilder *builder);

  void PCILoop(FunctionBuilder *builder);

  void PCILoopCondition(FunctionBuilder *builder, bool is_vectorizable, bool has_predicate);

  ast::Identifier DeclareSlot(FunctionBuilder *builder);

  void TVIClose(FunctionBuilder *builder);

  void TVIReset(FunctionBuilder *builder);

  ast::Identifier GetTVI() { return tvi_; };
  ast::Identifier GetPCI() { return pci_; };

 private:
  CodeGen *codegen_;
  ast::Identifier tvi_;
  ast::Identifier pci_;
  ast::Identifier slot_;
};

}  // namespace terrier::execution::compiler