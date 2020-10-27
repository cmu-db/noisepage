# Clang Tools

## clang-tidy

Important clang-tidy points are emphasized here. Read this if you're planning on changing how we use clang-tidy.

Quick warning: **don't trust the internet for examples** on how to use clang-tidy. Most projects mislead you by having typos, using non-existent features or messing up in a way that disables all the checks. The [official documentation](https://clang.llvm.org/extra/clang-tidy/index.html) and [source code](https://clang.llvm.org/extra/doxygen/dir_83d3dc8f7afce718e8cda93164271fb8.html) are your best bet.

**Overview**

1. clang-tidy is a static analyzer and linter. It checks for common code smells.
2. When you run clang-tidy, it searches parent directories for a .clang-tidy file.
    - `clang-tidy -dump-config` will print out the current configuration.
3. clang-tidy reads the output of compile_commands.json, which is generated when you run cmake.
    1. compile_commands.json only contains .cpp files.
    2. Therefore, clang-tidy can only process C++ (.cpp) files. It cannot process header (.h) files by themselves.
    3. clang-tidy will however process .h files which are included in a .cpp file.
    4. You might not want warnings from all header files, e.g. only src/, not third_party/.
    5. clang-tidy only supports whitelisting headers via a single regex string.
        - HeaderFilterRegex: 'REGEX' in .clang-tidy
        - -header-filter=REGEX on the command line, **note that this overrides your .clang-tidy setting**.
        - The (undocumented) format of the regex is POSIX ERE. See the implementation of [HeaderFilterRegex](https://clang.llvm.org/extra/doxygen/ClangTidyDiagnosticConsumer_8cpp_source.html#l00533) and [llvm::Regex](http://llvm.org/doxygen/Regex_8h_source.html#l00040).
4. clang-tidy separates the notion of Warning, Error, Compilation Error.
    - clang-tidy will only display warnings for enabled [checks](https://clang.llvm.org/extra/clang-tidy/checks/list.html).
    - clang-tidy will convert all warnings that match the WarningsAsErrors regex to errors.
        - **WarningsAsErrors does not enable any checks on its own**.
    - The list of checks are parsed as regex. **If you make a typo, it silently ignores that check**.
    - Run `clang-tidy -list-checks` to confirm which checks are enabled.
5. You can get clang-tidy to leave certain lines alone.
    - To ignore the same line, put `// NOLINT` at the end
    - To ignore the next line, put `// NOLINTNEXTLINE` on the immediate preceding line
    - Wherever possible, avoid using line-filter. Nobody wants to maintain line numbers when code gets added/deleted.
    - A `clang-diagnostic-error` may mean a compilation problem. No amount of NOLINT or disabling checks will shut that up. If you're pulling in third-party header dependencies, make sure they're a dependency for our check-clang-tidy make target too.

**Gotchas**

1. Running clang-tidy on a list of files will run clang-tidy on each file sequentially.
2. If you include a header file which needs fixing in multiple .cpp files, clang-tidy will repeat the fix multiple times. An example which was included in three files:
```
Original code : if (last_errno_)
"Fixed" code  : if (last_errno_ != 0 != 0 != 0)
```

For both of the above problems, LLVM recommends using their [run-clang-tidy.py](https://github.com/llvm-mirror/clang-tools-extra/blob/master/clang-tidy/tool/run-clang-tidy.py) script. It will gather all the code fixes and apply them at once to prevent this issue. It also supports parallelism.

However, if you use LLVM's default run-clang-tidy.py, caveat emptor:

- It doesn't work on [Python 3](https://github.com/llvm-mirror/clang-tools-extra/blob/master/clang-tidy/tool/run-clang-tidy.py#L166).
- It will override .clang-tidy's [HeaderFilterRegex](https://github.com/llvm-mirror/clang-tools-extra/blob/master/clang-tidy/tool/run-clang-tidy.py#L86).
- From a Pythonic/PyLint point of view, the code quality isn't great.
- That said, it is actively being developed and the above criticisms may no longer be valid.

We currently use a modified version of run-clang-tidy.py.