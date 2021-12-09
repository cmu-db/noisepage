# Design Doc: TPL Closures

### Overview

This document describes the implementation of closures in TPL. It includes both a high-level description as well as a complete walkthough of the low-level implementation details.

### Architecture

TPL closures are implemented as regular TPL functions with the added ability to capture arbitrary variables. In the same way that return values in TPL are implemented via a "hidden" out-parameter to each function, the variables captured by a TPL closure are represented as a TPL structure that is passed as a second hidden parameter to the function that implements the logic of the closure. 

The closure itself is represented as a stack-allocated structure - a local variable within the frame of the function in which the lambda that produces the closure appears. This structure contains `N` fields. The first `N - 1` fields are the variables captured by the closure. The final field is a pointer to the compiled function that implements the closure's logic - a regular TPL function.

TPL closures introduce some interesting implementation challenges that manifest during both code generation and during execution of the generated code. These implementation details are explored in further detail in the sections below.

Closures can be passed like values throughout a TPL program; this allows one to, for instance, construct a closure and pass it to other functions that may then invoke it to perform computations or produce side-effects. However, there is a major limitation to our current closure implementation design: because the TPL structure that implements the closure is allocated in the stack frame of the function in which the lambda that produced the closure appears, the closure cannot escape the lexical scope of this function. In other words, we cannot return a closure from a TPL function and invoke it elsewhere because the structure that backs its implementation would be deallocated the moment the function that creates it returns. This is a major limitation, and most languages that support closures (read: every language implementation that I can find) include the ability for closures the escape the scope in which they are defined. Adding support for this functionality obviously requires some additional engineering and often significantly complicates the implementation. We get away with this implementation for now because our use-cases for closures never require us to generate code that allows the closure to escape the scope in which it is defined, but this is likely an issue we should address in the future.

In a future refactor of this design, it may be beneficial (from a software-design perspective, at least) to implement closures as their own first-class type. Rather than implementing a closure as a TPL structure containing the closure's captures with the ad-hoc constraint that the final member is _always_ a pointer to the closure's associated function, we might consider adding a dedicated `ClosureType` type to the TPL DSL. At a first approximation, this would incur zero additional cost (compile-time or runtime) and would simplify some of the implementation because we could more easily distinguish between regular function invocations and closure invocations.

Another limitation of the current implementation of closures is the inability to easily specify their type. Because TPL is statically typed, this makes implementing higher-order functions that accept closures as arguments or return closures more difficult. At present I am unsure of the best way to address this limitation. Languages like C++ and Rust get around this with either of 1) type erasure (e.g. C++'s `std::function` or Rust's `Fn` trait) or 2) generics. Both of these like relatively involved approaches for our purposes here in TPL. Perhaps we might consider some kind of implicit-conversion facility between function pointer types (which can be concisely specified) and closures (even those that capture).

### Code Generation Details

Closures and the lambda expressions that produce them introduce some additional complexity to code generation. The general flow of code generation for a function that contains a lambda expression proceeds as follows:

- Visit the function declaration for the function in which the lambda expression appears
- Visit the body of the function; during visitation of the statement(s) in the function's body, the lambda expression is encountered
- Visit the lambda expression
  - Allocate a new local in the frame of the current function for the closure structure
  - Emit the bytecode to "capture" local variables; this is performed by loading the address of all captured locals into the fields in the closure (captures) structure
  - Emit the bytecode to pass (a pointer to) the locally-allocated captures structure to the function that will implement the closure's logic
  - Allocate the TPL function for the body of the closure
  - Defer an action for the current function to visit the body of the closure; this deferred action captures (in C++-land, not in TPL!) the TPL function allocated for the closure
- Complete visitation of the function in which the lambda expression appears
- As the final step in visitation of the function, execute the deferred action to visit the body of the closure's function

### Walkthough #0: Closure Without Captures

As a first example, we consider the following TPL program:

```
fun main() -> int32 {
  var addOne = lambda [] (x: int32) -> int32 {
                return x + 1           
                }
  return addOne(1)
}
```

The bytecode generated for this program, with annotations, is shown below.

```
Data: 
  Data section size 0 bytes (0 locals)

Function 0 <main>:
  Frame size 32 bytes (1 parameter, 5 locals)
    param        hiddenRv:  offset=0       size=8       align=8       type=*int32

    // The addOne local captures the closure that results from evaluating the lambda expression
    local          addOne:  offset=8       size=8       align=8       type=lambda[(int32,*int32)->int32]

    // The captures structure for the closure is allocated in the frame
    // of the function in which the lambda expression appears
    local  addOneCaptures:  offset=16      size=8       align=8       type=struct{*(int32,*int32)->int32}
    local            tmp1:  offset=24      size=4       align=4       type=int32  
    local            tmp2:  offset=28      size=4       align=4       type=int32  

  // In the current implementation, the closure is synonymous with a
  // pointer to the base of the captures structure; the bytecode in
  // the body of the function generated for the closure assumes this
  0x00000000    Assign8                                         local=&addOne  local=&addOneCaptures
  0x0000000c    AssignImm4                                      local=&tmp2  i32=1

  // Invoke the function generated for the closure; the captures structure is 
  // passed as an implicit final argument to the function call
  0x00000018    Call                                            func=<addOne>  local=&tmp1  local=tmp2  local=addOne  
  0x0000002c    Assign4                                         local=hiddenRv  local=tmp1
  0x00000038    Return                                          

Function 1 <addOne>:
  Frame size 32 bytes (3 parameters, 5 locals)
    param  hiddenRv:  offset=0       size=8       align=8       type=*int32 
    param         x:  offset=8       size=4       align=4       type=int32  
    param  captures:  offset=16      size=8       align=8       type=*int32 
    local      tmp1:  offset=24      size=4       align=4       type=int32  
    local      tmp2:  offset=28      size=4       align=4       type=int32  

  // The lambda that generated this function has no captures, therefore
  // we don't need to do anything special here to handle captured variables

  0x00000000    AssignImm4                                      local=&tmp2  i32=1
  // Perform the addition
  0x0000000c    Add_int32_t                                     local=&tmp1  local=x  local=tmp2
  // Set the return value
  0x0000001c    Assign4                                         local=hiddenRv  local=tmp1
  0x00000028    Return   
```

### Walkthough #1: Closure With Captures

As a second example, we consider the following TPL program:

```
fun main() -> int32 {
    var x = 1
    var addValue = lambda [x] (y: int32) -> int32 {
                    return x + y           
                   }
    return addValue(2)
}
```

The bytecode generated for this program, with annotations, is shown below.

```
Data: 
  Data section size 0 bytes (0 locals)

Function 0 <main>:
  Frame size 56 bytes (1 parameter, 7 locals)
    param          hiddenRv:  offset=0       size=8       align=8       type=*int32 
    local                 x:  offset=8       size=4       align=4       type=int32  
    local          addValue:  offset=16      size=8       align=8       type=lambda[(int32,*int32)->int32]

    // The first member of the captures structure is a pointer to the captured local;
    // the second member of the captures structure is a pointer to the associated function
    local  addValueCaptures:  offset=24      size=16      align=8       type=struct{*int32,*(int32,*int32)->int32}
    local              tmp1:  offset=40      size=8       align=8       type=**int32
    local              tmp2:  offset=48      size=4       align=4       type=int32  
    local              tmp3:  offset=52      size=4       align=4       type=int32  

  0x00000000    AssignImm4                                      local=&x  i32=1

  // Capture the variable `x`; load the address of the local variable `x` into the captures structure
  0x0000000c    Lea                                             local=&tmp1  local=&addValueCaptures  i32=0
  0x0000001c    Assign8                                         local=tmp1  local=&x
  
  // Initialize the closure itself as a pointer to the base of the captures structure
  0x00000028    Assign8                                         local=&addValue  local=&addValueCaptures
  
  0x00000034    AssignImm4                                      local=&tmp3  i32=2
  0x00000040    Call                                            func=<addValue>  local=&tmp2  local=tmp3  local=addValue  
  0x00000054    Assign4                                         local=hiddenRv  local=tmp2
  0x00000060    Return                                          

Function 1 <addValue>:
  Frame size 52 bytes (3 parameters, 7 locals)
    param  hiddenRv:  offset=0       size=8       align=8       type=*int32 
    param         y:  offset=8       size=4       align=4       type=int32
    param  captures:  offset=16      size=8       align=8       type=*int32 
    local      tmp1:  offset=24      size=4       align=4       type=int32  
    local      tmp2:  offset=32      size=8       align=8       type=**int32
    local      xptr:  offset=40      size=8       align=8       type=*int32 
    local      tmp3:  offset=48      size=4       align=4       type=int32  

  // Load the captured `x` pointer to the local `xptr`
  0x00000000    Lea                                             local=&tmp2  local=captures  i32=0
  0x00000010    DerefN                                          local=&xptr  local=tmp2  u32=8
  
  // Dereference the pointer to the captured `x` to get its value
  0x00000020    DerefN                                          local=&tmp3  local=xptr  u32=4

  // Perform the addition and return the result
  0x00000030    Add_int32_t                                     local=&tmp1  local=tmp3  local=y
  0x00000040    Assign4                                         local=hiddenRv  local=tmp1
  0x0000004c    Return 
```