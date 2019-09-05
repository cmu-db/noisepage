#pragma once

#include "execution/vm/bytecode_generator.h"
#include "execution/vm/bytecode_label.h"

namespace terrier::execution::vm {

/**
 * Base class for all control-flow builders
 */
class ControlFlowBuilder {
 public:
  /**
   * Constructor
   * @param generator bytecode generator to use when generating code.
   */
  explicit ControlFlowBuilder(BytecodeGenerator *generator) : generator_(generator) {}

  /**
   * Destructor. Should be overriden,
   */
  virtual ~ControlFlowBuilder() = default;

 protected:
  /**
   * @return the bytecode generator
   */
  BytecodeGenerator *Generator() { return generator_; }

 private:
  BytecodeGenerator *generator_;
};

/**
 * Builder for blocks that can be broken out of.
 */
class BreakableBlockBuilder : public ControlFlowBuilder {
 public:
  /**
   * Constructor
   * @param generator bytecode generator to use when generating code.
   */
  explicit BreakableBlockBuilder(BytecodeGenerator *generator) : ControlFlowBuilder(generator) {}

  ~BreakableBlockBuilder() override;

  /**
   * Emits the byte for break out of the block.
   */
  void Break();

  /**
   * @return the break label
   */
  BytecodeLabel *BreakLabel() { return &break_label_; }

 protected:
  /**
   * Helper method to jump to a label
   * @param label label to jump to.
   */
  void EmitJump(BytecodeLabel *label);

 private:
  BytecodeLabel break_label_;
};

/**
 * Builder for blocks that represent loops.
 */
class LoopBuilder : public BreakableBlockBuilder {
 public:
  /**
   * Constructor
   * @param generator bytecode generator to use when generating code.
   */
  explicit LoopBuilder(BytecodeGenerator *generator) : BreakableBlockBuilder(generator) {}

  ~LoopBuilder() override;

  /**
   * Emit the loop header. This is the label to jump to when reiterating.
   */
  void LoopHeader();

  /**
   * Emits the bytecode to jump to the loop header.
   */
  void JumpToHeader();

  /**
   * Emits the bytecode used to "continue" during iteration.
   */
  void Continue();

  /**
   * Binds the continue label. This defines the point to which to jump when "continue" is used.
   */
  void BindContinueTarget();

 private:
  /**
   * @return the header label
   */
  BytecodeLabel *HeaderLabel() { return &header_label_; }

  /**
   * @return the continue label
   */
  BytecodeLabel *ContinueLabel() { return &continue_label_; }

 private:
  // These label allow us to jump to a specific point during iteration.
  BytecodeLabel header_label_;
  BytecodeLabel continue_label_;
};

/**
 * A class to help in creating if-then-else constructs in VM bytecode.
 */
class IfThenElseBuilder : public ControlFlowBuilder {
 public:
  /**
   * Constructor
   * @param generator bytecode generator to use when generating code.
   */
  explicit IfThenElseBuilder(BytecodeGenerator *generator) : ControlFlowBuilder(generator) {}

  ~IfThenElseBuilder() override;

  /**
   * Emit then label
   */
  void Then();

  /**
   * Emit else label
   */
  void Else();

  /**
   * Emit code to jump to the end of the statement.
   */
  void JumpToEnd();

  /**
   * @return the then label
   */
  BytecodeLabel *ThenLabel() { return &then_label_; }

  /**
   * @return the else label
   */
  BytecodeLabel *ElseLabel() { return &else_label_; }

 private:
  BytecodeLabel *EndLabel() { return &end_label_; }

 private:
  BytecodeLabel then_label_;
  BytecodeLabel else_label_;
  BytecodeLabel end_label_;
};

}  // namespace terrier::execution::vm
