#include "execution/ast/builtins.h"

namespace terrier::ast {

// static
const char *Builtins::kBuiltinFunctionNames[] = {
#define ENTRY(Name, FunctionName, ...) #FunctionName,
    BUILTINS_LIST(ENTRY)
#undef ENTRY
};

}  // namespace terrier::ast
