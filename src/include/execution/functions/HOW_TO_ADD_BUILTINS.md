In order to add a builtin function foo(...) we have to do the following:
1) Implement the function foo() in src/include/execution/sql/functions/ in the appropriate file. Note that your
    implementation must follow the signature "static void foo(...)". If you wish to return something of type T,
    have foo take in a T* as an argument and use that to return the desired value.
    String functions work somewhat different yet similar. Those take an ExecutionContext as its first argument for
    allocation management purposes. Refer to the functions in src/include/execution/sql/functions/string_functions.h
    for examples.

2) Add a declaration for the function you made in BUILTINS_LIST inside src/include/execution/ast/builtins.h. Each line
    in this list is of the form F(Name, FunctionName) where FunctionName is the canonically the name of the function you
    created in step 1 and Name is the name of this specific builtin. This usually is the same as FunctionName with the
    first letter capitalized.

    Make sure that the builtin you want to add wasn't already there before adding.

   During code generation, a function expression with foo() will be converted into a TPL AST CallExpr node with
   Foo in its builtin field along with its respective parameters. This node needs to be type checked in the next step
   before being translated into TPL bytecode.

3) Add semantic checking/typing for your new bytecode that you made. This is done in src/execution/sema/sema_builtin.cpp.
    For each builtin function from bytecodes.h in step 2, there is a function in sema_builtin that checks a call to this
    bytecode and also sets the return type of the CallExpr node for this builtin. Refer to CheckMathTrigCall
    and its usages for a commented example on Sin and other trigonometric functions.

    This node will get translated into TPL bytecode.

5) Add a TPL bytecode in src/include/execution/vm/bytecodes.h inside the BYTECODES_LIST macro. See the
   definition of Sin on the line that begins as "F(Sin, OperandType::Local, OperandType::Local)". The two
   "OperandType::Local"'s at the end signify the two arguments our Sin function implementation from (1) takes.
   Make sure that the bytecode you want to add wasn't already there before adding.

6) Now we focus on the conversion of the CallExpr node from steps 2 and 3 into the bytecode we made in step 5. This
    is done in src/execution/vm/bytecode_generator.cpp inside VisitBuiltinCallExpr. If foo fits into one of the categories
    laid out in the switch case in there then go ahead and implement it inside the helper function devised for that or
    else make a new one.
    Let us take VisitBuiltinTrigCall for example. Here, for the Sin function we have that Sin
    takes in two arguments: a pointer to a destination where the Sin function can fill in its result and a real valued
    input as the second argument. Refer to the beginning of the function where space is created for this destination as
    a LocalVar and the input argument is also obtained as a LocalVar by Visiting the parameter of the CallExpr.
    After obtaining the respective LocalVars for all parameters, emit the bytecode for the function you want along with
    its parameters as shows.

7) Now we want to interpret the new bytecode inside src/execution/vm/vm.cpp. This file is a giant switch statement.
    We want to make a switch case for our new bytecode. Refer to the preexisting cases to see how to grab the bytecodes
    and data you want from the VM::Frame (in the variable called "frame") and call a bytecode handler that we will define
    in the next step.
    IT IS VERY IMPORTANT THAT YOU READ YOUR ARGUMENTS IN THE EXACT SAME ORDER YOU EMITTED THEM IN STEP 6.

8) We want to define a handler for our bytecode in src/include/execution/vm/bytecode_handlers.h.
    The name of your handler should be OpFoo if your function is called foo. Make sure this takes in values from
    execution::sql. From here we call the function we defined in step 1.

9) This is the last step. Now we want to make this function available in the catalog table, pg_proc. To do this edit
    src/catalog/database_catalog.cpp. Here, we want to add an entry into pg_proc in BootstrapProcs and also add a line
    to initialize the metadata object (of type FunctionContext) in BootstrapProcContexts and add it to pg_proc.
