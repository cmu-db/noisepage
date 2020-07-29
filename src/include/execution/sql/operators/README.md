# SQL Operators Directory

This directory contains implementations of common SQL **operators** such as comparisons, arithmetic,
bitwise, and casting operators. SQL operators are NOT aware of SQL NULL semantics. That logic must 
be handled at a higher-level: either by SQL functions in `src/include/sql/functions` (used by 
tuple-at-a-time code) or in vector-based operations in `src/include/sql/vector_operations`. In a
sense, operators sit "beneath" tuple-at-a-time and vector-at-a-time functions in the architectural 
stack.

All operators are implemented as C++ function objects. Function objects are any object that implement
the function call operator `operator()` (see [here](https://en.cppreference.com/w/cpp/utility/functional)
for more information). The C++ STL has examples of function objects, `std::less<>` being one. Function
objects offer the same power as regular functions, but can also carry state. Additionally, since function
objects have a type, we can use template meta-programming with traits to fine-tune compile-time code
generation.

Operator logic is shared across both tuple-at-a-time code and vectorized code. Tuple-at-a-time code
leverages operators as vanilla functions. Since most light-weight operators are implemented in the
header, the compiler will ensure proper inlining removing any function call overhead. Vectorized code
uses function object types as template arguments to perform compile-time code generation.

It is of tantamount importance that all operators use this function object design. For "heavy"
functions whose implementation complexity outweigh invocation overhead (e.g., SQL LIKE(*)), you may
implement that logic in a CPP file, but it must still be wrapped in a function object in order to be
used in vectorized code. While this requires some work, there are plenty of examples to use.
And ... it's really not that much work.
