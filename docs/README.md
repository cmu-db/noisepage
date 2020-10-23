# NoisePage Developer Docs

**Table of Contents**

- [New Students](#new-students)
- [Getting Started](#getting-started)
- [Development](#development)

## New Students

Hi! Welcome to CMU DB.

1. Ask Andy to add you to the [noisepage-dev](https://mailman.srv.cs.cmu.edu/mailman/listinfo/noisepage-dev) mailing list. This will also subscribe you to the [db-seminar](https://mailman.srv.cs.cmu.edu/mailman/listinfo/db-seminar) mailing list for CMU-DB events.
2. Ask Andy to add you to the CMU-DB Slack channel. Make sure you join these channels:
   - `#general` -- Post all development-related questions here.
   - `#random` -- Random DB / CS / CMU stuff.
3. If you need access to testing or development machines, ask on Slack in `#dev-machines`. Check the pinned message in `#dev-machines` for more details.
4. (COVID19) ~~If you would like a seat in the ninth floor lab (GHC 9022), please email [Jessica Packer](http://csd.cs.cmu.edu/people/staff/jessica-packer) (CC Andy) to get a key. Important: Please make sure to **return the key before you leave CMU**.~~
5. (COVID19) Due to COVID19, we no longer meet on campus. Instead, we hang out on Zoom; just join the links in `#random`. The Zoom lab isn't just for doing work - feel free to cook, do other work, or live-stream your pets walking around.
6. Please follow the instructions in [Getting Started](#getting-started) below.

## Getting Started

### System setup

1. **GitHub** We use GitHub for all our development.
   - **Account** [Sign up](https://github.com/join) for a GitHub account.
   - **Fork** Visit the [NoisePage repository](https://github.com/cmu-db/noisepage). Click the `Fork` button in the top right and fork the repository. You should be able to see the forked repository at `https://github.com/YOUR_GITHUB_USERNAME/noisepage`.
   - **SSH key** You should add a [SSH key](https://docs.github.com/en/free-pro-team@latest/github/authenticating-to-github/adding-a-new-ssh-key-to-your-github-account) to your GitHub account.  
2. **OS** Make sure you are running [Ubuntu 20.04](https://releases.ubuntu.com/20.04/) or macOS 10.14+. If not, the recommended approach is to dual boot or to use a VM.
3. **IDE** We officially only support [CLion](https://www.jetbrains.com/clion/).
   - You can download CLion for free with the generous [Jetbrains educational license](https://www.jetbrains.com/community/education/#students).
   - More setup instructions are available [here](https://github.com/cmu-db/noisepage/tree/master/docs/tech_clion.md). This includes general CMake build flags.
4. **Packages** This is covered in the CLion setup, but as a reminder:
   - The default CLion cloned repository location is `~/CLionProjects/noisepage`.
   - Go to the folder: `cd ~/CLionProjects/noisepage/script/installation`
   - Install all the necessary packages: `sudo bash ./packages.sh`
5. **macOS** If you are on a Mac, you should add this to your `~/.zshrc`:
   ```
   export PATH="/usr/local/opt/llvm@8/bin:$PATH"
   export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/opt/libpqxx/lib/
   export CC=/usr/local/Cellar/llvm@8/8.0.1_3/bin/clang
   export CXX=/usr/local/Cellar/llvm@8/8.0.1_3/bin/clang++
   export LLVM_DIR=/usr/local/Cellar/llvm@8/8.0.1_3
   export ASAN_OPTIONS=detect_container_overflow=0
   ```

### Further reading

You should learn a little about the following:

1. [CLion](https://github.com/cmu-db/noisepage/tree/master/docs/tech_clion.md)
2. [git](https://github.com/cmu-db/noisepage/tree/master/docs/tech_git.md)
3. [C++ and how we use it](https://github.com/cmu-db/noisepage/tree/master/docs/cpp_guidelines.md)

## Configuration

### CMake flags to know

- We try our best to list all available options in [CMakeLists.txt](https://github.com/cmu-db/noisepage/blob/master/CMakeLists.txt). Search for `# HEADER CMake options and global variables.`
- CMake options, specify with `-DNOISEPAGE_{option}=On`.
  - `NOISEPAGE_BUILD_BENCHMARKS`              : Enable building benchmarks as part of the ALL target. Default ON.
  - `NOISEPAGE_BUILD_TESTS`                   : Enable building tests as part of the ALL target. Default ON.
  - `NOISEPAGE_GENERATE_COVERAGE`             : Enable C++ code coverage. Default OFF.
  - `NOISEPAGE_UNITTEST_OUTPUT_ON_FAILURE`    : Enable verbose unittest failures. Default OFF. Can be very verbose.
  - `NOISEPAGE_UNITY_BUILD`                   : Enable unity (aka jumbo) builds. Default OFF.
  - `NOISEPAGE_USE_ASAN`                      : Enable ASAN, a fast memory error detector. Default OFF.
  - `NOISEPAGE_USE_JEMALLOC`                  : Link with jemalloc instead of system malloc. Default OFF.
  - `NOISEPAGE_USE_JUMBOTESTS`                : Enable jumbotests instead of unittests as part of ALL target. Default OFF.
  - `NOISEPAGE_USE_LOGGING`                   : Enable logging. Default ON.

### CMake targets to know

- You should know these targets.
  - `noisepage`: Building will build the `noisepage` binary and all its dependencies. Running will run the NoisePage DBMS.
  - `tpl`: Building will build the `tpl` binary and all its dependencies. Running will run a `tpl` interpreter.
  - `unittest`: Building will build all of our tests, execute all of the tests, and summarize the results.
  - `runbenchmark`: Building will build all of our benchmarks, execute all of the benchmarks, and summarize the results.
  - `format`: Building will run the formatter `clang-format` on the codebase with our rules. Use this every time right before you commit and right before you make a pull request!
  - `check-format`: Building will check if the codebase is correctly formatted according to `clang-format` with our rules.
  - `check-clang-tidy`: Building will check if the codebase passes the `clang-tidy` static analyzer tests with our rules.
  - `check-lint`: Building will check if the codebase passes the `build-support/cpplint.py` checks.
  - `check-censored`: Building will check if the codebase contains any bad words. This is a grep for forbidden words like `shared_ptr`.
  - `check-tpl`: Building will check if the TPL tests in `sample_tpl/` all pass.

If you run into issues, you may need your default `python` to point to a `python3` install. For example, add this to your `~/.zshrc`: `alias python=python3`

## Development

### Workflow

1. Check out the latest version of the NoisePage repository.
   - `git checkout master`
   - `git pull upstream master`
2. Create a new branch.
   - `git checkout -b my_new_branch`
3. Work on your code. Add features, add documentation, add tests, ~~add~~ remove bugs, and so on.
4. Push your code **to your fork, not to cmu-db**.
   - If you followed the CLion setup instructions, `git remote get-url origin` should return your fork's URL.
   - Make sure you run tests locally! See below.
   - `git push -u origin my_new_branch`
5. Go to GitHub and open a [new pull request](https://github.com/cmu-db/noisepage/compare).
6. When a pull request is opened, this triggers our Continuous Integration environment on Jenkins.
   - Jenkins will clone the repo, apply your changes, and make sure that formatting, linting, tests, etc pass.
   - Code has to pass all the checks for it to be merged!
7. When your pull request passes all of the checks, post on Slack in `#pr-czar`.

### Running tests locally

#### unittests

You can run the `unittest` or `jumbotests` suite, they are equivalent. Inside your build folder, after running the build system generator:
- `ninja unittest`: Compile and run each individual test in the `test/` folder.
- `ninja jumbotests`: Like `unittest`, but tests are grouped by `test/foo/` folders. Faster to compile.

If you are running individual tests manually from the command line, you should do so with
```
ctest -L TEST_NAME
```
The `ctest` runner calls `build-support/run-test.sh`, which handles setting up other environment variables for you.

You can also run the test binaries manually, but you will need to set the environment variables yourself. Right now, the variables that should be set are:
- `LSAN_OPTIONS=suppressions=/absolute/path/to/noisepage/build-support/data/lsan_suppressions.txt`

#### junit tests

TODO(WAN): write this. possibly after updating the junit thing to make it a little easier for people to pick up.