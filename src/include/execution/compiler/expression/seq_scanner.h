#pragma once

#include "execution/compiler/function_builder.h"

namespace terrier::execution::compiler {
/**
 * seq_scanner is utility function for sequantial scan
 */
class SeqScanner {
 public:
  /**
   * Constructor
   * @param codegen code generator
   */
  explicit SeqScanner(CodeGen *codegen)
      : codegen_(codegen),
        tvi_(codegen_->NewIdentifier("tvi")),
        pci_(codegen_->NewIdentifier("pci")),
        slot_(codegen_->NewIdentifier("slot")) {}
  /**
   * Declare a Table Vector Iterator
   * @param builder the function builder to append to
   * @param table_oid the oid of the table
   * @param col_oid oid of the column
   *
   */
  void DeclareTVI(FunctionBuilder *builder, uint32_t table_oid, ast::Identifier col_oid);

  /**
   * The advance call of TVI loop
   * @param builder the function builder to append to
   *
   */
  void TVILoop(FunctionBuilder *builder);

  /**
   * Declare a ProjectedColumn Iterator
   * @param builder the function builder to append to
   *
   */
  void DeclarePCI(FunctionBuilder *builder);

  /**
   * The advance call of PCI loop
   * @param builder the function builder to append to
   *
   */
  void PCILoop(FunctionBuilder *builder);

  /**
   * The advance call of PCI loop with condition
   * @param builder the function builder to append to
   * @param is_vectorizable whether it is vectorizable
   * @param has_predicate whether it has predicate
   *
   */
  void PCILoopCondition(FunctionBuilder *builder, bool is_vectorizable, bool has_predicate);

  /**
   * Get a tuple slot
   * @param builder the function builder to append to
   *
   */
  ast::Identifier DeclareSlot(FunctionBuilder *builder);

  /**
   * Close TVI
   * @param builder the function builder to append to
   *
   */
  void TVIClose(FunctionBuilder *builder);

  /**
   * Reset TVI
   * @param builder the function builder to append to
   *
   */
  void TVIReset(FunctionBuilder *builder);

  /**
   * Get PCI
   * @return current pci
   *
   */
  ast::Identifier GetPCI() { return pci_; }

 private:
  CodeGen *codegen_;
  ast::Identifier tvi_;
  ast::Identifier pci_;
  ast::Identifier slot_;
};

}  // namespace terrier::execution::compiler
