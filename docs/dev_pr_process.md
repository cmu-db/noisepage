# Developer Documentation: Pull Request Process

This document describes the process through which pull requests are submitted, reviewed, and merged in NoisePage.

### Table of Contents

- [Before Opening your PR](#before-opening-your-pr)
- [After Opening your PR](#after-opening-your-pr)
- [Code Review](#code-review)
- [After Merging your PR](#after-merging-your-pr)

### Before Opening Your PR

We currently use [Jenkins](https://jenkins.io/) and [Codecov](https://codecov.io/gh/cmu-db/terrier) for our Continuous Integration. We require all pull requests to pass Jenkins and Codecov prior to merging.

Pull requests are automatically subjected to a variety of checks and tests in our continuous integration environment. You can avoid time-consuming failure in continuous integration by running the following checks locally and addressing any issues they reveal prior to opening your pull request. Where applicable, we assume that you use the `Debug` build configuration when running these checks and tests.

- `make check-censored`
  - This performs a full-text search over the project source code files (i.e. `grep`) and identifies forbidden words such as `malloc`, `free`, and `shared_ptr`, among others. The full list of forbidden words is defined in `build-support/data/bad_words.txt`.
- `make check-format`
  - Identifies code formatting issues by running [clang-format](https://clang.llvm.org/docs/ClangFormat.html). Issues identified by `make check-format` can be fixed automatically by running `make format`. This command also runs `clang-format` and performs transformations such as rearranging lines, fixing identation, and ensuring line length limits are observed. After running this command, `make check-format` should pass.
- `make check-lint`
  - This runs [cpplint](https://github.com/cpplint/cpplint), which checks that your code is written in (approximately) Google C++ style. Issues identified by `make check-lint` must be addressed manually.
- `make check-clang-tidy`
  - This runs [clang-tidy](http://clang.llvm.org/extra/clang-tidy/) on your code. It checks your code for common errors and/or bugs. Issues identified by `make check-clang-tidy` must be addressed manually.
- `doxygen -u Doxyfile.in && doxygen Doxyfile.in 2> warnings.txt`
  - Run this command in the `apidoc/` directory, directly below the project root. This runs Doxygen to write the API documentation for NoisePage and directs all warnings to the local file `warnings.txt`. Continuous integration rejects PRs for which Doxygen produces any warnings, so it is useful to run this command locally first to verify that you have added / updated all of the necessary documentation.
- `make unittest`
  - This builds and runs the entirety of the unit test suite. In a debug build with AddressSanitizer enabled, this can take a relatively-long time to run.

## After Opening your PR

**Tags**

When filing issues or opening PRs, you should always tag them appropriately to make it easier for everyone to keep track of what's going on. The common tags are:
- `best-practice`: style fixes or refactors
- `bug`: fixes incorrect behavior
- `feature`: adds a new feature
- `in-progress`: not ready to be reviewed or merged
- `infrastructure`: CMake, third party dependencies, and Continuous Integration changes
- `performance`: optimizes Terrier performance
- `ready-for-review`: passes CI, and is ready for code review
- `ready-to-merge`: passes CI and code review, and is ready for merge
- `tests`: test infrastructure (Google Test, Google Benchmark, JUnit) changes

**Continuous Integration**

The full CI pipeline for pull requests is described by the Jenkinsfile at the root of the project. This Jenkinsfile uses utility functions defined in `Jenkinsfile-utils.groovy` (also at the project root) to describe the pipeline. Review these files for further details on the specific tests and benchmarks that are run in the continuous integration environment for a pull request.

**Merge Conflicts**

The submitter is generally responsible for keeping their branch up to date and free of merge conflicts. Old branches without merge conflicts may be updated by a repository admin, but the submitted should be proactive about their PRs. Any PRs with merge conflicts will be tagged `in-progress` and need to be reviewed again after conflicts are resolved.

## Code Review

Once a pull request completes Continuous Integration and has been tagged `ready-to-review`, it should be reviewed by developers familiar with the system component(s) impacted by the changes. Reviewers should follow the following guidelines:
- Don't just view the diff on Github as these views lack context and make it difficult to spot larger code design issues. View the modified files in their totality, and clone large PRs locally.
- Do the changes adhere to developer guidelines with respect to file structure, program behavior, and code style? Basically, look for code style issues that our automated tools can't catch.
- Check the documentation. Just because a PR passes the Doxygen check doesn't mean that the documentation is comprehensive or accurate. Could a new developer still follow the behavior of the changes?
- Check the test cases. Do they exercise common scenarios as well as edge cases? Is the tests' behavior obvious or documented enough that a developer could easily use them to debug issues that arise in the future?
- Check the Coveralls results in detail. If the changes are not being exercised then it usually means that either the code is not correct or the tests are not thorough enough — sometimes both.

## After Merging your PR

TODO: Describe nightly tests and the process of putting out fire if nightly build fails
