#include "execution/vm/control_flow_builders.h"

#include "execution/vm/bytecode_emitter.h"

namespace terrier::execution::vm {

// ---------------------------------------------------------
// Breakable blocks
// ---------------------------------------------------------

BreakableBlockBuilder::~BreakableBlockBuilder() {
  TERRIER_ASSERT(!break_label()->IsBound(), "Break label cannot be bound!");
  generator()->emitter()->Bind(break_label());
}

void BreakableBlockBuilder::Break() { EmitJump(break_label()); }

void BreakableBlockBuilder::EmitJump(BytecodeLabel *label) { generator()->emitter()->EmitJump(Bytecode::Jump, label); }

// ---------------------------------------------------------
// Loop Builders
// ---------------------------------------------------------

LoopBuilder::~LoopBuilder() = default;

void LoopBuilder::LoopHeader() {
  TERRIER_ASSERT(!header_label()->IsBound(), "Header cannot be rebound");
  generator()->emitter()->Bind(header_label());
}

void LoopBuilder::JumpToHeader() { generator()->emitter()->EmitJump(Bytecode::Jump, header_label()); }

void LoopBuilder::Continue() { EmitJump(continue_label()); }

void LoopBuilder::BindContinueTarget() {
  TERRIER_ASSERT(!continue_label()->IsBound(), "Continue label can only be bound once");
  generator()->emitter()->Bind(continue_label());
}

// ---------------------------------------------------------
// If-Then-Else Builders
// ---------------------------------------------------------

IfThenElseBuilder::~IfThenElseBuilder() {
  if (!else_label()->IsBound()) {
    generator()->emitter()->Bind(else_label());
  }

  TERRIER_ASSERT(!end_label()->IsBound(), "End label should not be bound yet");
  generator()->emitter()->Bind(end_label());
}

void IfThenElseBuilder::Then() { generator()->emitter()->Bind(then_label()); }

void IfThenElseBuilder::Else() { generator()->emitter()->Bind(else_label()); }

void IfThenElseBuilder::JumpToEnd() { generator()->emitter()->EmitJump(Bytecode::Jump, end_label()); }

}  // namespace terrier::execution::vm
