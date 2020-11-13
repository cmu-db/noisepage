# Development Environment Troubleshooting

**Table of Contents**

- [Platform Agnostic](#platform-agnostic)
- [OSX](#osx)
- [Ubuntu](#ubuntu)

## Platform Agnostic

**Issue:** The installation script `packages.sh` fails during installation of the `pyarrow` Python package with a massive, inscrutable error message that turns the entirety of your terminal window red.

**Fix:** We have observed this behavior when attempting to install `pyarrow` under an unsupported version of Python. Currently, `pyarrow` supports Python versions up through Python 3.8. Therefore, if you are using a later version of Python, you may have to switch to an earlier version in order to make the install succeed.

## OSX

**Issue**: Initial project configuration with `cmake` fails, reporting that the C++ compiler `clang++` is broken because it cannot compile a simple test program. Further details in the error message report a linker failure:

```
ld: library not found for -lc++
```

**Fix:** During its initial checks on the validity of the C++ compiler, CMake is failing to locate the C++ standard library. All recent releases of OSX come bundled with `libc++` installed by default: `libc++.dylib` located in `/usr/lib`. To ensure that CMake can locate the standard library and prevent the linker error, just add `/usr/lib` to your `LIBRARY_PATH` environment variable. For example:

```
export LIBRARY_PATH=$LIBRARY_PATH:/usr/lib:/usr/local/opt/libpqxx/lib/
```

The fix is analogous if you encounter a similar error when building from within CLion.

**Issue:** When attempting to build the project from within CLion, the build fails at the first attempt to link any of the targets or sub-targets with the error: 

```
Undefined symbols for architecture x86_64:
```

**Fix:** There a number of issues that may result in an undefined symbol error. One potential cause is attempting to specify the C and C++ compiler that you want CLion to use with a custom toolchain. 

You might be attempting to force CLion to use the compilers from LLVM instead of the default system compiler by specifying a toolchain other than the default and selecting the `clang` and `clang++` binaries from the `/usr/local/Cellar/llvm@8/8.0.1_3/bin/` directory. However, in certain circumstances CLion will automatically resolve both of these paths to `clang-8`, a binary in the same directory that provides only a C frontend (where we require C++). Thus, as soon as the linker is invoked from the context of `clang-8` during a build, it will choke on the mangled C++ symbols.

The fix for this is to specify the C and C++ compilers with environment variables instead of using the toolchain selection UI from within CLion. In your build configurations, set the `CC` and `CXX` environment variables and point them at the `clang` and `clang++` binaries in `/usr/local/Cellar/llvm@8/8.0.1_3/bin/`, respectively.

**Issue:** In debug builds, launching the database from within CLion is immediately followed by a program crash with AddressSanitizer reporting a container overflow on an instance of `std::string` (or perhaps another STL container).

**Fix:** There is a known issue with AddressSanitizer false positives on OSX that stems from use of the libc++ standard library implementation. Because of this, we must disable AddressSanitizer's container overflow checks. If you are running NoisePage from the commandline, setting an environment variable should be sufficient to address this issue:

```
export ASAN_OPTIONS=detect_container_overflow=0
```

or, if you don't want to make the changes persistent for your shell session:

```
ASAN_OPTIONS=detect_container_overflow=0 ./noisepage [...]
```

When building and running within CLion, however, there appears to be some quirk of the interaction between the IDE and the AddressSanitizer runtime that results in ASan not respecting any environment variables we set in the `Run / Debug` configurations, meaning that the program will continue to crash as a result of this false positive even when the environment variable is set. To circumvent this, navigate to _Preferences_ > _Dynamic Analysis Tools_ > _Sanitizers_ and add the option `detect_container_overflow=0` to the AddressSanitizer entry. Launching NoisePage from CLion with AddressSanitizer enabled should now succeed.

**Issue:** When an AddressSanitizer error is encountered when running or debugging within CLion, the IDE complains that "the LLVM symbolizer cannot be found."

**Fix:** Set the path the the `llvm-symbolizer` binary as an environment variable in your `Debug` build configuration. For example:

```
ASAN_SYMBOLIZER_PATH=/usr/local/opt/llvm@8/Toolchains/LLVM8.0.1.xctoolchain/usr/bin/llvm-symbolizer
```

Notice that the provided path includes the filename for the binary itself.

## Ubuntu

Record troubleshooting tips specific to Ubuntu here.