# C++ Guidelines: Code Style

## Comments, Formatting, and Libraries

Please **comment** your code. Comment all the class definitions, non-trivial member functions and variables, and all the steps in your algorithms. We use Doxygen style comments and will reject commits that don't fill out comments.

We generally follow the [Google C++ style guide](https://google.github.io/styleguide/cppguide.html). As they mention in that guide, these rules exist to keep the code base manageable while still allowing coders to use C++ language features productively.
Make sure that you follow the [naming rules](https://google.github.io/styleguide/cppguide.html#General_Naming_Rules). For instance, use `class UpperCaseCamelCase` for type names, `int lower_case_with_underscores` for variable/method/function names.

Please refrain from using any libraries other than the `STL` (and `googletest` for unit testing) without contacting us.

## Code Organization and Best Practice
We strive to write modern C++17 code, but C++ is a language with a lot of legacy features from the 80s. Certain guidelines must be followed to make the use of language features tasteful and elegant.

### `.h` and `.cpp` files

Surprisingly, there is no universal standards on what to call c++ code files. In this project, we will use `.h` for headers, inside the various `/include` directories, and `.cpp` for the implementation. 

When possible, implementation should be separated between `.h` and `.cpp` files, as this will make the compilation process much faster and hassle-free. Documentation should be written in the `.h` files. There are a couple of exceptions to this rule:
  - One-liners or otherwise boilerplate code that is unlikely to change. What constitutes a one-liner is somewhat ambiguous and can be subjective, but > 5 lines would not pass the smell test.
  - Templates. The c++ compiler generates instantiations of actual code based on the template given, and may need this information in the compilation units themselves, thus requiring all definition to be present in the included header. There are two solutions to this: either write all of the template code in the header or explicitly instantiate templates. Because doing the latter is painful, we generally will write those definitions in-place.

### Forward Declarations

When referring to some object only by reference, object or some template arguments, it is not necessary that the code knows its structure. As a result, we do not necessarily need to provide its complete declaration with members and methods (i.e. #include), but can get away with a hint that such a class exist. Example is given below:

```c++
class Foo;
...
void DoSomething(Foo *foo);  // compiles
void DoSomethingElse(Foo *foo) {
  foo->bar();  // error, member access into opaque type
}
...
```
Doing this saves re-compilation time. As a rule of thumb, forward declare when possible in the header, but always include the actual headers in the `.cpp` file.

### Assertions

`NOISEPAGE_ASSERT`s should be placed anywhere you believe there to be a runtime invariant that should be enforced, particularly at the start of a function body that has constraints on its input parameters. These serve as self-documenting code, and will help catch bugs if other developers call into your functions with invalid input.

### Exceptions

C++ exceptions should only be thrown in "exceptional cases".  Specifically, it should only be thrown when the current code path needs to be completely aborted such as fatal errors where the program should terminate (internal exceptions) or the users request cannot proceed due to user input error (e.g. referenced a non-existent table).

### Globals

Globals should almost never be used in our code base. They make dependencies hidden, namespaces polluted, and can cause unintentional link-time headaches for the less compiler-savvy.

With the exception of debug logging and other developer tools wrapped behind macros, you should NOT use a global. Stateless functions should be written as static methods under utility classes, and global variables avoided, and instead passed around as arguments.

### Prefer `constexpr` to `#define`

When defining a constant or a simple macro, you should use `constexpr`. This gives the benefits of proper scoping, stricter compilation checks, and more reasonable error output.

### The "Wall of Text"

A "Wall of Text" is a dense, unstructured piece of code with few or no paragraph breaks. Here is an example extracted from our old codebase, verbatim:
```c++
...
ResultType TestingSQLUtil::ExecuteSQLQuery(
    const std::string query, std::vector<ResultValue> &result,
    std::vector<FieldInfo> &tuple_descriptor, int &rows_changed,
    std::string &error_message) {
  LOG_TRACE("Query: %s", query.c_str());
  // prepareStatement
  std::string unnamed_statement = "unnamed";
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query);
  PELOTON_ASSERT(sql_stmt_list);
  if (!sql_stmt_list->is_valid) {
    return ResultType::FAILURE;
  }
  auto statement = traffic_cop_.PrepareStatement(unnamed_statement, query,
                                                 std::move(sql_stmt_list));
  if (statement.get() == nullptr) {
    traffic_cop_.setRowsAffected(0);
    rows_changed = 0;
    error_message = traffic_cop_.GetErrorMessage();
    return ResultType::FAILURE;
  }
  // ExecuteStatment
  std::vector<type::Value> param_values;
  bool unnamed = false;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  // SetTrafficCopCounter();
  counter_.store(1);
  auto status = traffic_cop_.ExecuteStatement(statement, param_values, unnamed,
                                              nullptr, result_format, result);
  if (traffic_cop_.GetQueuing()) {
    ContinueAfterComplete();
    traffic_cop_.ExecuteStatementPlanGetResult();
    status = traffic_cop_.ExecuteStatementGetResult();
    traffic_cop_.SetQueuing(false);
  }
  if (status == ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
  }
  LOG_TRACE("Statement executed. Result: %s",
            ResultTypeToString(status).c_str());
  rows_changed = traffic_cop_.getRowsAffected();
  return status;
}
...
```
Needless to say, this is bad. You should not write one in our new codebase. Having dense code itself is not a problem, but the "Wall of Text" is usually a symptom of one of the following, more severe problems, ranked from least severe to most severe, with mitigation strategies:
- Lack of meaningful logical blocks, comments, and readability features such as extra blank line between irrelevant statement groups. **Solution**: Write code in logical blocks, clearly separated by an extra blank line, and prefixed with comments.

- Lack of abstraction in the code. Some of the code clutter may qualify for helper methods. Longer methods might have been broken up into several smaller ones. **Solution**: Avoid long methods. Actively seek out opportunities to abstract reusable code blocks out into functions. 

- Copy-and-pasted code. **Solution**: DON'T

### Strong Typedefs

A strong typedef is simply a typedef that is opaque to the compiler, and cannot be implicit converted to and from its underlying types. To make this more concrete:
```c++
using my_int = int64_t;
...
my_int foo = 64;  // okay
int64_t bar = foo;  // okay
foo = bar  // still okay
```

This leads to problems. Namely the following situation:
```c++
using col_id = uint16_t;
void Update(uint16_t offset, col_id id);
...
for (uint16_t i = 0; i < num_cols; i++) {
  col_id id = projection_list[i];
  Update(col_id, i)  // Oops
}
```

A strong typedef solves this by forcing the compiler to not know about the underlying type. (You can find the implementation under `src/include/common/typedef.h`) To use it:

```c++
STRONG_TYPEDEF(col_id, uint16_t);
...
for (uint16_t i = 0; i < num_cols; i++) {
  col_id id = projection_list[i];
  Update(col_id, i)  // Compile time error
}
```

To get the underlying value out of a strong typedef, use the `!` operator. The typename works as a constructor as well. A simple example:

```c++
col_id id(42);
uint16_t foo = !id;  // foo is now 42
```

Most of the operators on an integer still makes sense on a strong typedef as well, with some additional restrictions pertaining to types. (can only compare `col_id` to other `col_id`, can only add a real integer to a `col_id` instead of adding a `col_id` to a `col_id`, etc.)

### Concurrent Data Structures

A DBMS is full of use cases for various concurrent data structures, and there are many implementations of concurrent data structures online, of differing quality. Before you bring in any external implementation, ask yourself:
- Do I need a concurrent data structure?
- Is this data structure implementation the right choice for my use case and workload?
- Will someone in the future want to swap this out for a different implementation?

Concurrent data structures, especially lock-free ones, are not magic. They perform well only in the environment they are designed for. Our advice is to always start simple, and ensure correctness with latches and other simple mechanisms. Performance gain needs to be measured and verified on representative benchmarks, taken against multiple alternatives.

Finally, always prefer a wrapper whose underlying implementation can be swapped out with minimal effort to direct invocation of third-party code, when it comes to data structures.

### Enforcing function access control

Sometimes, `public`, `protected`, and `private` are not granular enough for controlling who can call a function.
Historically, we have thrown a `@warning` into the function documentation, but unfortunately function comments can be
outdated and/or ignored. However, if you know exactly which types are allowed to call some function,
then it is actually possible to enforce this at compile-time (provided that your callers are acting in good faith).

This is done by templatizing your function to take in the `CallerType`, and using a `static_assert` in the
implementation to check that the `CallerType` is valid. This breaks if the caller is not reporting their `CallerType`
truthfully, but it is much easier to catch that in code review than it is to check for `@warning` violation.
You can think of the `CallerType` as a door entry code, with all the security that entails.

An example of this pattern is the following `DatabaseCatalog` function:

```cpp
template <typename CallerType>
bool SetTableSchemaPointer(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t oid,
                           const Schema *schema) {
  static_assert(std::is_same_v<CallerType, storage::RecoveryManager>, "Only recovery should call this.");
  ...
```

Unfortunately, this has the following drawbacks: being templated, the function must either expose its implementation
in the header or explicit template instantiation must be used. Exposing implementation in the header may require
adding more `#include`s to the header, which can lead to a painful blowup of compile time, so the latter is preferred.

You may also be interested in the cppreference for [std::enable_if](https://en.cppreference.com/w/cpp/types/enable_if),
which allows you to compile out functions entirely. However, this may lead to mystery linking errors in tests.

### Defining enumerations

1. C++ enums serialize as a number. This is annoying for debugging purposes.
2. Writing a ToString and FromString for every member of an enum also sucks.
3. We can't use the version provided by our JSON library (NLOHMANN_JSON_SERIALIZE_ENUM) because that requires including
   a HUGE header, blowing up compile time.
4. By using the macro in `enum_defs.h`, you will get an automatic ToString and FromString that is kept in sync.

The macro is `ENUM_DEFINE(EnumName, EnumCppType, EnumValMacro)`:

- `EnumName` is the name of your enum.
- `EnumCppType` is the underlying C++ type of your enum, for example, `uint8_t`.
- `EnumValMacro` is a macro that you must define, taking exactly one macro T as an argument, and applying T to the (
  EnumName, EnumMemberName).

The macro defines the enum with one additional field (NUM_ENUM_ENTRIES) and two corresponding EnumNameToString and
EnumNameFromString functions.

This is illustrated in the following truncated example:

```
// First, define a macro-taking-a-macro with all your enum members.
#define EXPRESSION_TYPE_ENUM(T)                     \
    T(ExpressionType, INVALID)                      \
                                                    \
    T(ExpressionType, OPERATOR_UNARY_MINUS)         \
    T(ExpressionType, OPERATOR_PLUS)                \
    T(ExpressionType, OPERATOR_MINUS)               \
    T(ExpressionType, OPERATOR_MULTIPLY)            \
    T(ExpressionType, OPERATOR_DIVIDE)              \
    T(ExpressionType, OPERATOR_CONCAT)              \
    T(ExpressionType, OPERATOR_MOD)                 \
    ...

// Next, define the enum itself.
ENUM_DEFINE(ExpressionType, uint8_t, EXPRESSION_TYPE_ENUM);

// This is equivalent to
enum ExpressionType : uint8_t { INVALID, OPERATOR_UNARY_MINUS, ..., NUM_ENUM_ENTRIES }
std::string ExpressionTypeToString(ExpressionType type);
ExpressionType ExpressionTypeFromString(const std::string &str);

// Finally, clean up your macro for macro hygiene.
#undef EXPRESSION_TYPE_ENUM
```

Note that unfortunately `enum_defs.h` leaks the following unrelated macro definitions:

- `ENUM_DEFINE_ITEM`
- `ENUM_TO_STRING`
- `ENUM_FROM_STRING`