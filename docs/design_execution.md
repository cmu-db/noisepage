# Design Doc: Execution Engine Subsystem

### Overview

The NoisePage execution engine subsystem encompasses all of the functionality required for query compilation, interpretation, and execution. While the high-level architecture of the subsystem is described in a variety of places, including in 15-721 lectures and in some of NoisePage-related publications, a comprehensive overview of the entire process is missing. This document goes into some additional detail that is missing from the existing sources and might prove useful during the onboarding process for new developers.

### Terminology

This section of the document describes all of the terminology used throughout the NoisePage execution engine. Think of it like a glossary. Some of the terms may be difficult to understand without the further context provided in the walkthrough that follows, but it is often convenient to have most of the important, domain-specific terms collected in one location for quick reference. If you encounter a term that is missing from this terminology section, either while reviewing the walkthrough below or while working in the system, please feel free to add it here.

Terms are arranged in alphabetical order. We considered attempting to arrange them in a rough hierarchy that corresponds to their role within the system, but this quickly becomes unwieldy as the relationship between some terms is ambiguous. When the term corresponds closely enough with a type / function in NoisePage's source code, we call this out explicitly in parentheses following the term itself.

- Code Generation (`CompilationContext::Compile()`): Code generation refers to the process of translating the physical query plan produced by the query optimizer to the highest-level internal representation that the execution engine can execute (TBC bytecode). The output of code generation is an executable query (`ExecutableQuery`).
- Executable Query (`ExecutableQuery`): An executable query corresponds to the highest-level query abstraction within the execution engine that may feasibly be executed.
- Execution Context (`ExecutionContext`): A context structure that is provided to the lower layer of the NoisePage execution engine at the time that execution of the query is requested. The context contains various metadata required during the execution process, such as an accessor for the database catalog, memory tracking information, etc. 
- Execution Mode (`ExecutionMode`): The NoisePage execution engine (notionally) supports three distinct execution modes: `Interpreted`, `Compiled`, and `Adaptive`. Don't conflate these execution modes with the various layers of the execution subsystem (e.g. both `Compiled` and `Adaptive` execution modes must pass through the query compilation process). The execution mode determines the execution path used within the execution engine after the executable query is produced during code generation. Generally speaking, the `Interpreted` mode does not enter the query compilation layer of the execution engine, while both the `Compiled` and `Adaptive` modes do. See the bullets below for a brief description of each of the individual execution modes.
- Execution Mode - `Interpreted`: The `Interpreted` execution mode refers to the execution mode of the NoisePage execution engine that does not proceed past code generation into query compilation, and instead executes the bytecode representation of the query directly via an interpreter (or Virtual Machine in the parlance of the system).
- Execution Mode - `Compiled`: The `Compiled` execution mode refers to the execution mode of the NoisePage execution engine that does proceed past code generation into query compilation and subsequent native execution.
- Execution Mode - `Adaptive`: The `Adaptive` execution mode is a combination of the `Interpreted` and `Compiled` execution modes that represents (in some sense) the best of both worlds: the low-latency initial execution of query interpretation and the higher steady-state performance of query compilation. In `Adaptive` mode, executable fragments of the query are simultaneously interpreted and compiled asynchronously; when the query compilation process finishes, the natively-compiled query is swapped in for the bytecode representation to complete execution of the query. For more details on adaptive query compilation, see the relevant [paper](https://15721.courses.cs.cmu.edu/spring2018/papers/03-compilation/kohn-icde2018.pdf).
- Fragment (`Fragment`): A self-contained unit of execution that represents a chunk of a larger query. All executable queries are composed of at least one fragment.
- Physical Plan: The physical plan is the query plan output by the query optimizer. This plan serves as the input to the execution engine.
- Pipeline (`Pipeline`): A pipeline is an abstraction that serves as a unit of organization during code generation. In the early stages of code generation, individual operators in the physical query plan tree are grouped into pipelines. Organizing the operators in a query plan in this way is ultimately intended to limit the amount of tuple materialization that must occur during the execution of the query. [This paper](https://par.nsf.gov/servlets/purl/10066914) provides a more comprehensive overview of pipelines and their role in code generation, and furthermore describes the specifics as they are implemented in NoisePage.
- Plan Node: For our purposes, a plan node refers to a single plan node in the physical query plan tree. Each node has a type, along with other associated metadata relevant to the particular type of the node.
- Terrier Bytecode (TBC): The low-level bytecode representation of a TPL program. TBC corresponds to the bytecode that composes a compiled TPL module, and is the representation that may be interpreted directly on the virtual machine in the `Interpreted` execution mode.
- Terrier Programming Language (TPL): TPL refers to both the high-level domain-specific language utilized within NoisePage, as well as the ecosystem surrounding the language. For instance, we refer to a "TPL module" to denote a compiled TPL program, even though the actual content of the module is TBC bytecode.
- Translator: A translator is an abstraction utilized in the code generation layer of the execution engine. Roughly, a translator corresponds to a node in the physical plan tree for the query, and assumes responsibility for generating the code that implements this operator. In this way, translators serve as the bridge between the logical representation of the query (as a physical query plan, I know, poor choice of words) and the physical representation in code that will undergo further stages of lowering to prepare it for execution. Translators come in two flavors: expression translators and operator translators.
- Expression Translator (`ExpressionTranslator`): An expression translator is a translator that implements an expression from the physical plan tree.
- Operator Translator (`OperatorTranslator`): An operator translator is a translator that implements an operator from the physical plan tree.

**Disambiguation: `Module`**

This term deserves its own special section for disambiguation because it is so heavily overloaded.

- Module (`vm::Module`): The top-level container for all code related to a single TPL program. This includes the compiled TPL program produced by the TPL compiler (a `vm::BytecodeModule` instance) as well as the compiled module produced by the LLVM JIT (`LLVMEngine::CompiledModule`).
- Bytecode Module (`vm::BytecodeModule`): The compiled TPL program produced by the TPL compiler. A bytecode module is produced during code generation.
- Compiled Module (`LLVMEngine::CompiledModule`): The natively-compiled code produced by LLVM JIT compilation. A compiled module is produced during query compilation.
- LLVM Module (`llvm::Module`): LLVM's own internal top-level container for program logic. This type does not cross abstraction boundaries within NoisePage, but rather is merely an implementation detail with the LLVM JIT engine (`LLVMEngine`).

### Architecture

We partition the NoisePage execution engine into the following top-level layers:

1. Code Generation
2. Query Compilation and Execution

### Starting Point: Physical Query Plan

We need to draw the boundaries of the NoisePage execution engine somewhere. For the purposes of this documentation, that starting point is the physical query plan. The physical plan is the query plan produced by the query optimizer. Unlike a logical query plan, the physical plan has all of the fine-grained query execution decisions determined (e.g. `JOIN` algorithm, index selection, etc.) that we need in order to actually enter the execution engine subsystem.

### Code Generation (Physical Plan -> Executable Query)

The code generation layer of the execution engine translates the physical query plan produced by the optimizer to the highest-level abstraction that the subsystem is capable of executing - an executable query, represented by the `ExecutableQuery` class.

The entry point to code generation is the `CompilationContext::Compile()` function. This function merely performs some housekeeping and then passes off to the instance method `CompilationContext::GeneratePlan()` to perform the bulk of the processing that occurs at this stage. We can further partition the process of code generation into several logical sub-phases:

1. Pipeline Definition
2. AST Construction
3. Module Generation
4. Executable Query Construction

**Pipeline Definition (Physical Plan -> Pipeline Abstraction)**

It is at this stage of the process that a crucial change to the abstraction model for the query is made. As mentioned above, the input to code generation is the physical query plan, which is represented as a tree of high-level operators that implement the query. During the initial stages of code generation, however, the operator abstraction is broken and replaced with the pipeline abstraction. The following comment from `operator_translator.h` explains the pipeline abstraction nicely:

```
 /**
  * Operators participate in one or more pipelines. A pipeline represents a uni-directional data flow
  * of tuples or tuple batches. Another of way of thinking of pipelines are as a post-order walk of
  * a subtree in the query plan composing of operators that do not materialize tuple (batch) data
  * into cache or memory.
  */
```

The code in `CompilationContext::GeneratePlan()` makes it a bit difficult to observe precisely how pipelines are constructed from the input operator tree. The crucial observation is the recursive nature of the `CompilationContext::Prepare()` and `CompilationContext::PrepareOut()` functions. We invoke one of these two functions on the root of the physical query plan tree, which is itself a plan node, and this kicks off a descent of the plan tree that ultimately produces the pipelines for the query.

In the initial traversal of the plan tree, for each plan node we encounter, we perform (roughly) the following steps:

- Construct a translator for the operator or expression represented by the plan node
- In the body of constructor for the translator, recursively invoke `CompilationContext::Prepare()` on the relevant children of the plan node with which this translator is associated

As we descend the plan tree and produce translators for each operator or expression in the physical plan tree, we simultaneously register each translator with one or more pipelines. A translator may appear in multiple pipelines in the event that it represents a materialization point. For instance, consider the translator for the hash join operator (`HashJoinTranslator`). This translator must participate in both the pipeline that produces the tuples utilized on the "build side" of the join as well as the pipeline that produces tuples utilized on the "probe side." In the NoisePage implementation, the hash join translator constructs a new pipeline with itself as the root to handle this situation.

**AST Construction (Pipeline Abstraction -> AST)**

At this point, the physical query plan has been translated into a collection of pipelines, each of which is composed of a sequence of translators. The next step in code generation is construction of an abstract syntax tree (AST) from the raw pipeline abstraction. This AST subsequently serves as input to the TPL compiler (not to be confused with the concept of query compilation in the broader context of the NoisePage execution engine!).

In order to construct an AST from the raw pipeline abstraction, we perform the following high-level steps:

- Declare the top-level structures for the fragment (because we currently utilize a single fragment for each executable query, these top-level fragment structures correspond to query-level state)
- Declare top-level functions for the fragment (because we currently utilize a single fragment for each executable query, these top-level fragment functions correspond to query-level functions e.g. query setup and teardown)
- Iterate over all pipelines in the fragment and generate all related pipeline-level code and data (because we currently utilize a single fragment for each executable query, iterating over all pipelines for the fragment is tantamount to iterating over all pipelines for the query). It is at this point where the bulk of AST construction is actually performed. For each pipeline, we iterate over all of the translators defined within it and invoke each of their `PerformPipelineWork()` functions in the appropriate order. It is in the body of these functions within the individual translators themselves that the specifics of AST construction occurs.

At the end of AST construction, we now have an AST abstraction that is ready for the TPL compiler. This AST is organized in much the same way that the AST for any programming language is organized: as a collection of data and functions organized into an approximate control-flow structure (but not a proper control-flow graph). During the invocation of the individual translators within each pipeline mentioned above, each translator emits code into the AST at the appropriate point according to the context in which occurs. For instance, a translator that appears within pipeline `foo` emits the code that implements its functionality in the body of the `perform_pipeline_work` function for the `foo` pipeline (the actual name of the function in AST and generated TPL differs, but the concept here is the important part).

The final thing to mention here is that during AST construction, individual translators emit only high-level code constructs. That is, individual translators are not responsible for emitting the low-level programming constructs that manage control flow (as an example) but rather operate on high-level primitives like table iterators.

**TPL Module Generation (AST -> TPL Module)**

The entry point for compilation of the AST to a TPL module occurs at `ExecutableQueryFragmentBuilder::Compile()`. Again, it is important not to get bogged down in the terminology here. The `Compile()` member of the `ExecutableQueryFragmentBuilder` class refers to compilation of a TPL AST to a TPL module; it is totally distinct from the concept of query compilation that occurs in the lower layers of the NoisePage execution engine. 

TPL compilation is implemented in `Compiler::Run()`. This is one of the more comprehensible stages of the code generation process, and the source code for this function is relatively-well documented in source. At a high-level, the TPL compiler performs the following steps to compile the input AST to a TPL module:

1. Parsing: This step is elided in the event that the program is not being compiled from raw TPL source code, which is the case here because we manually constructed the AST for the program during the previous sub-phases of code generation.
2. Semantic Analysis: The input AST is traversed and checked for logical errors (e.g. type-system violations).
3. Bytecode Generation: Where the magic happens. The input AST is traversed and translated to a low-level bytecode representation that may be executed directly in a lower layer of the NoisePage execution engine.
4. Module Generation: A TPL module is constructed that assumes ownership for the raw bytecode produced in the previous step.

There is a one-to-one mapping between code and data defined in the input AST and the code and data that appears in the completed TPL module. That is, there is no fundamental change to the abstraction that occurs at this stage of code generation (as was the case during pipeline definition). Instead, TPL module generation represents both an important lowering step (a TPL module is the fundamental unit of execution in the `Interpreted` execution mode) as well as a unification point between the more general TPL ecosystem and the NoisePage execution engine. We mentioned above that the TPL compiler can handle both raw TPL programs (the textual representation that a programmer may write by hand) as well as previously-constructed TPL AST (the intermediary form produced in the previous phase of code generation). Implementing the TPL ecosystem in a manner that is agnostic of the NoisePage execution engine itself makes the system more amenable to development.

**Executable Query Construction (TPL Module -> Executable Query)**

The process of transforming a TPL module into an executable query is a simple one. Each executable query fragment corresponds to a single TPL module, so we simply transfer ownership of the module into the executable query fragment. Furthermore, because the NoisePage execution engine currently generates a single fragment for all queries, we have the relationship that each query corresponds to exactly one TPL module.

### Query Compilation and Execution

Labeling this layer of the execution engine as being responsible for both query compilation and execution is somewhat misleading because at times the query compilation path is elided altogether (more on this later). However, it doesn't make sense to partition the two into distinct layers because the interface to each execution mode supported by the execution engine is the same.

The entry point to this second layer of the execution engine is the `ExecutableQuery::Run()` member function. After some setup, this function is invoked for an `ExecutableQuery` instance in the body of `TrafficCop::RunExecutableQuery()`. 

The function itself accepts two arguments: the execution context in which to execute the executable query, and the query execution mode (`ExecutionMode` enumeration). The execution context simply provides a centralized location for metadata related to query execution that is available throughout this lower layer of the execution engine. The execution mode passed to `ExecutableQuery::Run()` is one of three possible values:

- `ExecutionMode::Interpret`: Denotes that the bytecode module (`BytecodeModule`) owned by the compiled TPL module (`Module`) should be interpreted directly on the VM; query compilation is bypassed altogether.
- `ExecutionMode::Compiled`: Denotes that the bytecode module (`BytecodeModule`) owned by the compiled TPL module (`Module`) should be further processed through query compilation to produce a compiled module (`CompiledModule`) that can subsequently be natively executed.
- `ExecutionMode::Adaptive`: Denotes that the query should be executed adaptively, using a combination of both interpretation and compilation. Support for adaptive query compilation in NoisePage is currently a work in progress, so we will defer discussion of this procedure until a later date when the feature is fully realized.

Before we continue into the details of interpreted and compiled execution modes, it is worth noting that the high-level interface to both execution modes is identical. In the body of `ExecutableQuery::Run()` we perform the same steps regardless of the desired execution mode:

- Iterate through each fragment (`ExecutableQuery::Fragment`) that composes the executable query, and `Run()` it
- Running a query fragment consists of iterating through each function that composes the fragment and requesting a C++ function object that implements that function from the fragment's TPL module (recall every fragment owns exactly one TPL module that represents the TPL program that implements the fragment's functionality). The execution mode is specified in this request to the TPL module, and it is at this point that the logic of the execution engine diverges based on the execution mode in use.
- The TPL module returns a C++ function object that implements the requested function; we push query processing forward by invoking this function object on the query state and then return to the top of the loop to request the next function from the TPL module. This process continues until all functions in the fragment have been executed, at which point the fragment is complete.
- If the query is composed of multiple fragments, we return to the top of the outer loop and `Run()` the next fragment. This process continues until all fragments in the executable query have been run, at which point query execution is complete.

We will now examine the internals of each of the available execution modes.

**Interpretation (`ExecutionMode::Interpret)`**

TODO: Someone with more experience with the VM should address this section. Otherwise, I will when I get the chance to examine the code more closely.

**Compilation (`ExecutionMode::Compiled`)**

The heart and soul of query compilation lives in the NoisePage LLVM engine (`LLVMEngine`). The entry point to query compilation occurs at `Module::CompileToMachineCode()` which invokes `LLVMEngine::Compile()` on the bytecode module (`BytecodeModule`) that was produced by the TPL compiler during code generation.

Query compilation to machine code takes place in two distinct phases:

1. Lowering from the bytecode representation of the TPL program in the bytecode module to LLVM intermediate representation (IR)
2. Utilization of the LLVM JIT to compile the LLVM IR to native machine code for the target platform

As the short description above suggests, the first phase is the one we implement ourselves in NoisePage; the majority of the code in the LLVM engine is concerned with this task. In contrast, once we have lowered the TPL program to LLVM IR, it is a relatively simple matter to utilize the existing LLVM infrastructure to JIT this IR to native code. This is, of course, the whole point of LLVM, and also why LLVM is a common sight among DBMS execution engines that support compilation to native code.

The most interesting part of the lowering process from TPL to LLVM IR occurs in the `CompiledModuleBuilder::DefineFunction()` function. It is here that the bytecode representation of each function in the TPL program is translated to an equivalent representation in LLVM IR via the LLVM IR builder API. The code itself is relatively straightforward and well-commented so we won't belabor the point here, but the general idea is that we iterate over each bytecode (effectively an instruction) in the current function and emit the corresponding LLVM IR into the corresponding function in the compiled module `CompiledModule` under construction.

Once the LLVM IR is compiled to native machine code, we need to make the native code callable from the DBMS server process itself. We accomplish this by:

1. Loading the object code for the native machine code into an in-memory buffer
2. Converting this raw object code into an object file, loading this object file, and linking it to the address space of the DBMS server process
3. Resolving and caching each of the function definitions in the module (not strictly necessary, but elides the need to resolve procedures at runtime)

Once the native code is loaded and linked to the DBMS process address space, all of the functions it exposes are accessible from regular C++ function calls. The aforementioned use of C++ function objects to wrap these calls unifies the API across calls to both interpreted (`ExecutionMode::Interpret`) and compiled (`ExecutionMode::Compiled` and `ExecutionMode::Adaptive`) functions.

### Conclusion

This concludes the walkthrough of the NoisePage execution engine. Note that many important aspects of the execution engine's interaction with the rest of the system have been omitted in order to keep this document (at least somewhat) concise and comprehensible. Some of these other components of the system are described in other design documents within this directory.
