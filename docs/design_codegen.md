# Design Doc: Execution Engine Code Generation

### Overview

As described in the _Execution Engine Design Document_, NoisePage utilizes [data-centric code generation](https://15721.courses.cs.cmu.edu/spring2020/papers/14-compilation/p539-neumann.pdf) to compile the query plans produced by the optimizer to a byetcode representation that is then either interpreted or JIT-compiled. This document describes some of the nuances of the code generation process. While it is a strict subset of the process descibed in the _Execution Engine Design Document_, code generation is a complex topic, and giving it its own document allows us to focus in on the details without getting lost in unrelated concerns from the layers of the execution engine above and below it.

### Data-Centric Code Generation

Our goal in code generation is to produce a bytecode program that implements a query plan. 

The straightforward and most common way of accomplishing this is to have each operator in the query plan tree assume responsibility for generating the code that it requires to execute. The complete byetcode program might then be realized by having each operator generate code into a distinct bytecode function and then chaining these functions together via calls from the functions produced by parent operators to those produced by child operators. 

As mentioned above, this approach is straightforward to reason about and to implement. The code generated for each operator is nicely self-contained in a single bytecode function, allowing developers to verify the correctness of the generated code and debug code generation issues. However, the simplicity of this approach comes at the cost of query runtime performance. We now incur function-call overhead in the transition between each operator. More importantly, we leave ourselves open to the same performance issues present in any operator-centric execution model: poor code and data locality resulting from tuple-at-a-time processing among each operator.

Data-centric code generation is a solution to these performance issues. TODO

### Pipelines

TODO

**Complications**

Since the original implementation of code generation, we have introduced several features that have required updates to the pipeline interface. Namely:
- Inductive Common Table Expressions (`WITH RECURSIVE` and `WITH ITERATIVE`) which introduce the concept of _nested pipelines_
- User-Defined Functions which introduce the concept of an _output callback_

Both of these aditions, nested pipelines and output callbacks, slightly complicate the code generation process, and this additional complexity is reflected in the pipeline interface, to which we now turn our attention.

### The Pipeline Interface

The are several flavors of pipelines within NoisePage that differ slightly in the signature of their top-level bytecode functions, as well as their semantics. In this section, we explain each of these distinct flavors and provide the signatures of each of these top-level functions.

#### Serial Pipelines

We begin the discussion with serial pipelines because they are slightly less complicated. 

**State Initialization**

The interface for the pipeline state initialization functions is the same across all pipeline variants.

To initialize the pipeline state, we generate:

```
fun Query0_Pipeline1_InitPipelineState(*QueryState, *PipelineState)
```

and the teardown the pipeline state, we generate:

```
fun Query0_Pipeline1_TeardownPipelineState(*QueryState, *PipelineState)
```

**Pipline Initialization**

The interface for pipeline initialization varies depending on the pipeline variant. 

In the common case, we generate:

```
fun Query0_Pipeline1_Init(*QueryState)
```

In the case of a nested pipeline, we generate:

```
fun Query0_Pipeline1_Init(*QueryState, *PipelineState)
```

In the case of a pipeline with an output callback, we generate:

```
fun Query0_Pipeline1_Init(*QueryState, *PipelineState)
```

A pointer to the pipeline state (`*PipelineState`) is provided to the call for nested pipelines and pipelines with output callbacks because in both of these cases the pipeline state associated with the thread running the pipeline is not owned by the pipeline in question. Instead, this pipeline state structure is allocated on the stack at runtime and passed through the bytecode function invocations.

**Pipeline Run**

The interface for the pipeline _Run_ function varies depending on the pipeline variant.

In the common case, we generate:

```
fun Query0_Pipeline1_Run(*QueryState)
```

In the case of nested pipelines, we generate:

```
fun Query0_Pipeline1_Run(*QueryState, *PipelineState)
```

In the case of pipelines with an output callback, we generate:

```
fun Query0_Pipeline1_Run(*QueryState, *PipelineState, Closure)
```

The distinction between pipelines with output callbacks and nested pipelines manifests here. The output callback (in the form of a TPL closure) is provided as a third parameter to the _Run_ function such that it can be invoked by the operators that utilize it (for now, just the `OutputTranslator`) in the body of the pipeline _Work_ function.

**Pipeline Teardown**

The interface for pipeline teardown varies depending on the pipeline variant.

In the common case, we generate:

```
fun Query0_Pipeline1_Teardown(*QueryState)
```

In the case of a nested pipeline, we generate:

```
fun Query0_Pipeline1_Teardown(*QueryState, *PipelineState)
```

In the case of a pipeline with an output callback, we generate:

```
fun Query0_Pipeline1_Teardown(*QueryState, *PipelineState)
```

The reason that a pointer to the pipeline state is provided to the call in the latter two cases is the same as in the case of pipeline initialization.

#### Parallel Pipelines

Parallel pipelines require different semantics from serial pipelines. Despite these differences, only the _Work_ function is affected by the change from a serial to a parallel pipeline.

TODO

### References

- [Efficiently Compiling Efficient Query Plans for Modern Hardware](https://15721.courses.cs.cmu.edu/spring2020/papers/14-compilation/p539-neumann.pdf) by Thomas Neumann. The paper that introduced the concept of data-centric code generation, among other techniques now considered standard best-practice in compiling query engines.
