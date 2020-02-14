# Source Code Tools Data Files

This directory contains data files that we use for our various source code tools to check for 
improper code or to suppress warnings (e.g., memory leaks).

If you add a file to this directory, please add comment to explain what it does and how to extend 
it.

* bad-words.txt -- Keywords that cannot appear in the source code.
* clang-format-exclusions.txt -- Files/directory to ignore when we run clang-format.
* lsan-suppressions.txt -- Source to ignore for memory leaks.
* sanitize-blacklist.txt -- Source to ignore with the sanitizers.
* valgrind-suppressions.txt -- Source to ignore with Valgrind. This is currently not used.
