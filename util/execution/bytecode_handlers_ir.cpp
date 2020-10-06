#include "execution/vm/bytecode_handlers.h"
#include "execution/vm/bytecodes.h"

extern "C" {

void *kAllFuncs[] = {  // NOLINT
#define ENTRY(Name, ...) reinterpret_cast<void *>(&Op##Name),
    BYTECODE_LIST(ENTRY)
#undef ENTRY
};
}
