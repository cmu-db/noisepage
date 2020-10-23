#include "execution/vm/control_flow_builders.h"

#include "execution/vm/bytecode_emitter.h"
#include "execution/vm/bytecode_generator.h"

namespace noisepage::execution::vm {

// ---------------------------------------------------------
// Breakable blocks
// ---------------------------------------------------------

BreakableBlockBuilder::~BreakableBlockBuilder() {
  NOISEPAGE_ASSERT(!GetBreakLabel()->IsBound(), "Break label cannot be bound!");
  GetGenerator()->GetEmitter()->Bind(GetBreakLabel());
}

void BreakableBlockBuilder::Break() { EmitJump(GetBreakLabel()); }

void BreakableBlockBuilder::EmitJump(BytecodeLabel *label) {
  GetGenerator()->GetEmitter()->EmitJump(Bytecode::Jump, label);
}

// ---------------------------------------------------------
// Loop Builders
// ---------------------------------------------------------

LoopBuilder::~LoopBuilder() = default;

void LoopBuilder::LoopHeader() {
  NOISEPAGE_ASSERT(!GetHeaderLabel()->IsBound(), "Header cannot be rebound");
  GetGenerator()->GetEmitter()->Bind(GetHeaderLabel());
}

void LoopBuilder::JumpToHeader() { GetGenerator()->GetEmitter()->EmitJump(Bytecode::Jump, GetHeaderLabel()); }

void LoopBuilder::Continue() { EmitJump(GetContinueLabel()); }

void LoopBuilder::BindContinueTarget() {
  NOISEPAGE_ASSERT(!GetContinueLabel()->IsBound(), "Continue label can only be bound once");
  GetGenerator()->GetEmitter()->Bind(GetContinueLabel());
}

// ---------------------------------------------------------
// If-Then-Else Builders
// ---------------------------------------------------------

IfThenElseBuilder::~IfThenElseBuilder() {
  if (!GetElseLabel()->IsBound()) {
    GetGenerator()->GetEmitter()->Bind(GetElseLabel());
  }

  NOISEPAGE_ASSERT(!EndLabel()->IsBound(), "End label should not be bound yet");
  GetGenerator()->GetEmitter()->Bind(EndLabel());
}

void IfThenElseBuilder::Then() { GetGenerator()->GetEmitter()->Bind(GetThenLabel()); }

void IfThenElseBuilder::Else() { GetGenerator()->GetEmitter()->Bind(GetElseLabel()); }

void IfThenElseBuilder::JumpToEnd() { GetGenerator()->GetEmitter()->EmitJump(Bytecode::Jump, EndLabel()); }

}  // namespace noisepage::execution::vm
