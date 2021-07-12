# Design Doc: TPL Closures

### Overview

This document describes the implementation of closures in TPL. It includes both a high-level description as well as a complete walkthough of the low-level implementation details.

### Architecture

TPL closures are implemented as regular TPL functions with the added ability to capture arbitrary variables. In the same way that return values in TPL are implemented via a "hidden" out-parameter to each function, the variables captured by a TPL closure are represented as a TPL structure that is passed as a second hidden parameter to the function that implements the logic of the closure. 

The closure itself is represented as a stack-allocated structure - a local variable within the frame of the function in which the lambda that produces the closure appears. This structure contains `N` fields. The first `N - 1` fields are the variables captured by the closure. The final field is a pointer to the compiled function that implements the closure's logic - a regular TPL function.

TPL closures introduce some interesting implementation challenges that manifest during both code generation and during execution of the generated code. These implementation details are explored in further detail in the sections below.

Closures can be passed like values throughout a TPL program; this allows one to, for instance, construct a closure and pass it to a higher-order function that then invokes it within its body (this is how clsoures are used to implement generalized output callbacks and enable the implementation of user-defined functions). However, there is a major limitation to our current closure implementation design: because the TPL structure that implements the closure is allocated in the stack frame of the function in which the lambda that produced the closure appears, the closure cannot escape the lexical scope of this function. In other words, we cannot return a closure from a TPL function and invoke it elsewhere because the structure that backs its implementation would be deallocated the moment the function that creates it returns. This is a major limitation, and most languages that support closures (read: every language implementation that I can find) include the ability for closures the escape the scope in which they are defined. Adding support for this functionality obviously requires some additional engineering and often significantly complicates the implementation. We get away with this implementation for now because our use-cases for closures never require us to generate code that allows the closure to escape the scope in which it is defined, but this is likely an issue we should address in the future.

In a future refactor of this design, it may be beneficial (from a software-design perspective, at least) to implement closures as their own first-class type. Rather than implementing a closure as a TPL structure containing the closure's captures with the ad-hoc constraint that the final member is _always_ a pointer to the closure's associated function, we might consider adding a dedicated `ClosureType` type to the TPL DSL. At a first approximation, this would incur zero additional cost (compile-time or runtime) and would simplify some of the implementation because we could more easily distinguish between regular function invocations and closure invocations.

### Implementation: Code Generation

TODO

### Implementation: Runtime

TODO
