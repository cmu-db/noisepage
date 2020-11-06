#include "execution/ast/builtins.h"

namespace noisepage::execution::ast {

// static
const char *Builtins::builtin_function_names[] = {
#define ENTRY(Name, FunctionName, ...) #FunctionName,
    BUILTINS_LIST(ENTRY)
#undef ENTRY
};

}  // namespace noisepage::execution::ast
