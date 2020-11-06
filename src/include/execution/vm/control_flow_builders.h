#pragma once

#include "execution/vm/bytecode_label.h"

namespace noisepage::execution::vm {

class BytecodeGenerator;

/**
 * Base class for all control-flow builders.
 */
class ControlFlowBuilder {
 public:
  /**
   * Construct a builder that will modify the given generator instance.
   * @param generator The generator to use for generating code.
   */
  explicit ControlFlowBuilder(BytecodeGenerator *generator) : generator_(generator) {}

  /**
   * Destructor. Should be overriden,
   */
  virtual ~ControlFlowBuilder() = default;

 protected:
  /** @return The bytecode generator. */
  BytecodeGenerator *GetGenerator() { return generator_; }

 private:
  BytecodeGenerator *generator_;
};

/**
 * Base class for code-blocks that can be broken out of.
 */
class BreakableBlockBuilder : public ControlFlowBuilder {
 public:
  /**
   * Construct a builder that will modify the given generator instance.
   * @param generator The generator to use for generating code.
   */
  explicit BreakableBlockBuilder(BytecodeGenerator *generator) : ControlFlowBuilder(generator) {}

  /**
   * Destructor.
   */
  ~BreakableBlockBuilder() override;

  /**
   * Break out of the current block.
   */
  void Break();

  /**
   * @return The label that all breaks from this block jump to.
   */
  BytecodeLabel *GetBreakLabel() { return &break_label_; }

 protected:
  /**
   * Helper method to jump to a label.
   * @param label The label to jump to.
   */
  void EmitJump(BytecodeLabel *label);

 private:
  // The label we jump to from breaks.
  BytecodeLabel break_label_;
};

/**
 * Helper class to build loops.
 */
class LoopBuilder : public BreakableBlockBuilder {
 public:
  /**
   * Construct a loop builder.
   * @param generator The generator the loop writes.
   */
  explicit LoopBuilder(BytecodeGenerator *generator) : BreakableBlockBuilder(generator) {}

  /**
   * Destructor.
   */
  ~LoopBuilder() override;

  /**
   * Emit the loop header. This is the label to jump to when reiterating.
   */
  void LoopHeader();

  /**
   * Jump to the header of the loop from the current position in the bytecode.
   */
  void JumpToHeader();

  /**
   * Generate a 'continue' to skip the remainder of the loop and jump to the header.
   */
  void Continue();

  /**
   * Binds the continue label. This defines the point to which to jump when "continue" is used.
   */
  void BindContinueTarget();

 private:
  /** @return The label associated with the header of the loop. */
  BytecodeLabel *GetHeaderLabel() { return &header_label_; }

  /** @return The label associated with the target of the continue. */
  BytecodeLabel *GetContinueLabel() { return &continue_label_; }

 private:
  BytecodeLabel header_label_;
  BytecodeLabel continue_label_;
};

/**
 * A class to help in creating if-then-else constructs in VM bytecode.
 */
class IfThenElseBuilder : public ControlFlowBuilder {
 public:
  /**
   * Construct an if-then-else generator.
   * @param generator The generator the if-then-else writes.
   */
  explicit IfThenElseBuilder(BytecodeGenerator *generator) : ControlFlowBuilder(generator) {}

  /**
   * Destructor.
   */
  ~IfThenElseBuilder() override;

  /**
   * Generate the 'then' part of the if-then-else.
   */
  void Then();

  /**
   * Generate the 'else' part of the if-then-else.
   */
  void Else();

  /**
   * Jump to the end of the if-then-else.
   */
  void JumpToEnd();

  /**
   * @return The label associated with the 'then' block of this if-then-else.
   */
  BytecodeLabel *GetThenLabel() { return &then_label_; }

  /**
   * @return THe label associated with the 'else' block of this if-then-else
   */
  BytecodeLabel *GetElseLabel() { return &else_label_; }

 private:
  BytecodeLabel *EndLabel() { return &end_label_; }

 private:
  BytecodeLabel then_label_;
  BytecodeLabel else_label_;
  BytecodeLabel end_label_;
};

}  // namespace noisepage::execution::vm
