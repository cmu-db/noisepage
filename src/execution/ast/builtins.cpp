#include "execution/ast/builtins.h"

namespace tpl::ast {

// static
const char *Builtins::kBuiltinFunctionNames[] = {
#define ENTRY(Name, FunctionName, ...) #FunctionName,
    BUILTINS_LIST(ENTRY)
#undef ENTRY
};

}  // namespace tpl::ast
