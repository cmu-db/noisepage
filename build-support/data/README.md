# Source Code Tools Data Files

This directory contains data files that we use for our various source code tools to check for 
improper code or to suppress warnings (e.g., memory leaks).

If you add a file to this directory, please add comment to explain what it does and how to extend 
it.

* `bad_words.txt` -- Keywords that cannot appear in the source code.
* `clangformat_suppressions.txt` -- Files/directory to ignore when we run clang-format.
* `sanitize_suppressions.txt` -- Targets to ignore with the sanitizers. This is used in `run-test.sh`.
* `lsan_suppressions.txt` -- Leaks to ignore with LSAN. This is used in `run-test.sh`.
