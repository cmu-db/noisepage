#pragma once

#include "execution/vm/bytecode_generator.h"
#include "execution/vm/bytecode_label.h"

namespace tpl::vm {

/**
 * Base class for all control-flow builders
 */
class ControlFlowBuilder {
 public:
  explicit ControlFlowBuilder(BytecodeGenerator *generator)
      : generator_(generator) {}

  virtual ~ControlFlowBuilder() = default;

 protected:
  BytecodeGenerator *generator() { return generator_; }

 private:
  BytecodeGenerator *generator_;
};

class BreakableBlockBuilder : public ControlFlowBuilder {
 public:
  explicit BreakableBlockBuilder(BytecodeGenerator *generator)
      : ControlFlowBuilder(generator) {}

  ~BreakableBlockBuilder() override;

  void Break();

  BytecodeLabel *break_label() { return &break_label_; }

 protected:
  void EmitJump(BytecodeLabel *label);

 private:
  BytecodeLabel break_label_;
};

class LoopBuilder : public BreakableBlockBuilder {
 public:
  explicit LoopBuilder(BytecodeGenerator *generator)
      : BreakableBlockBuilder(generator) {}

  ~LoopBuilder() override;

  void LoopHeader();
  void JumpToHeader();

  void Continue();

  void BindContinueTarget();

 private:
  BytecodeLabel *header_label() { return &header_label_; }
  BytecodeLabel *continue_label() { return &continue_label_; }

 private:
  BytecodeLabel header_label_;
  BytecodeLabel continue_label_;
};

/**
 * A class to help in creating if-then-else constructs in VM bytecode.
 */
class IfThenElseBuilder : public ControlFlowBuilder {
 public:
  explicit IfThenElseBuilder(BytecodeGenerator *generator)
      : ControlFlowBuilder(generator) {}

  ~IfThenElseBuilder() override;

  void Then();
  void Else();
  void JumpToEnd();

  BytecodeLabel *then_label() { return &then_label_; }
  BytecodeLabel *else_label() { return &else_label_; }

 private:
  BytecodeLabel *end_label() { return &end_label_; }

 private:
  BytecodeLabel then_label_;
  BytecodeLabel else_label_;
  BytecodeLabel end_label_;
};

}  // namespace tpl::vm
