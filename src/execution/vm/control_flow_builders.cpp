#include "execution/vm/control_flow_builders.h"

#include "execution/vm/bytecode_emitter.h"

namespace terrier::execution::vm {

// ---------------------------------------------------------
// Breakable blocks
// ---------------------------------------------------------

BreakableBlockBuilder::~BreakableBlockBuilder() {
  TERRIER_ASSERT(!BreakLabel()->IsBound(), "Break label cannot be bound!");
  Generator()->Emitter()->Bind(BreakLabel());
}

void BreakableBlockBuilder::Break() { EmitJump(BreakLabel()); }

void BreakableBlockBuilder::EmitJump(BytecodeLabel *label) { Generator()->Emitter()->EmitJump(Bytecode::Jump, label); }

// ---------------------------------------------------------
// Loop Builders
// ---------------------------------------------------------

LoopBuilder::~LoopBuilder() = default;

void LoopBuilder::LoopHeader() {
  TERRIER_ASSERT(!HeaderLabel()->IsBound(), "Header cannot be rebound");
  Generator()->Emitter()->Bind(HeaderLabel());
}

void LoopBuilder::JumpToHeader() { Generator()->Emitter()->EmitJump(Bytecode::Jump, HeaderLabel()); }

void LoopBuilder::Continue() { EmitJump(ContinueLabel()); }

void LoopBuilder::BindContinueTarget() {
  TERRIER_ASSERT(!ContinueLabel()->IsBound(), "Continue label can only be bound once");
  Generator()->Emitter()->Bind(ContinueLabel());
}

// ---------------------------------------------------------
// If-Then-Else Builders
// ---------------------------------------------------------

IfThenElseBuilder::~IfThenElseBuilder() {
  if (!ElseLabel()->IsBound()) {
    Generator()->Emitter()->Bind(ElseLabel());
  }

  TERRIER_ASSERT(!EndLabel()->IsBound(), "End label should not be bound yet");
  Generator()->Emitter()->Bind(EndLabel());
}

void IfThenElseBuilder::Then() { Generator()->Emitter()->Bind(ThenLabel()); }

void IfThenElseBuilder::Else() { Generator()->Emitter()->Bind(ElseLabel()); }

void IfThenElseBuilder::JumpToEnd() { Generator()->Emitter()->EmitJump(Bytecode::Jump, EndLabel()); }

}  // namespace terrier::execution::vm
