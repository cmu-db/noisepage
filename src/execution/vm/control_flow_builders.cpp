#include "execution/vm/control_flow_builders.h"

#include "execution/vm/bytecode_emitter.h"

namespace tpl::vm {

// ---------------------------------------------------------
// Breakable blocks
// ---------------------------------------------------------

BreakableBlockBuilder::~BreakableBlockBuilder() {
  TPL_ASSERT(!break_label()->is_bound(), "Break label cannot be bound!");
  generator()->emitter()->Bind(break_label());
}

void BreakableBlockBuilder::Break() { EmitJump(break_label()); }

void BreakableBlockBuilder::EmitJump(BytecodeLabel *label) { generator()->emitter()->EmitJump(Bytecode::Jump, label); }

// ---------------------------------------------------------
// Loop Builders
// ---------------------------------------------------------

LoopBuilder::~LoopBuilder() = default;

void LoopBuilder::LoopHeader() {
  TPL_ASSERT(!header_label()->is_bound(), "Header cannot be rebound");
  generator()->emitter()->Bind(header_label());
}

void LoopBuilder::JumpToHeader() { generator()->emitter()->EmitJump(Bytecode::Jump, header_label()); }

void LoopBuilder::Continue() { EmitJump(continue_label()); }

void LoopBuilder::BindContinueTarget() {
  TPL_ASSERT(!continue_label()->is_bound(), "Continue label can only be bound once");
  generator()->emitter()->Bind(continue_label());
}

// ---------------------------------------------------------
// If-Then-Else Builders
// ---------------------------------------------------------

IfThenElseBuilder::~IfThenElseBuilder() {
  if (!else_label()->is_bound()) {
    generator()->emitter()->Bind(else_label());
  }

  TPL_ASSERT(!end_label()->is_bound(), "End label should not be bound yet");
  generator()->emitter()->Bind(end_label());
}

void IfThenElseBuilder::Then() { generator()->emitter()->Bind(then_label()); }

void IfThenElseBuilder::Else() { generator()->emitter()->Bind(else_label()); }

void IfThenElseBuilder::JumpToEnd() { generator()->emitter()->EmitJump(Bytecode::Jump, end_label()); }

}  // namespace tpl::vm
