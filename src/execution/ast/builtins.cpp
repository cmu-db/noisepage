#include "execution/ast/builtins.h"

namespace terrier::execution::ast {

// static
const char *Builtins::kBuiltinFunctionNames[] = {
#define ENTRY(Name, FunctionName, ...) #FunctionName,
    BUILTINS_LIST(ENTRY)
#undef ENTRY
};

}  // namespace terrier::execution::ast
