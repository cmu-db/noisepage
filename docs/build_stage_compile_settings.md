# Compile Settings for Build Stages
This doc was last updated: 11/3/20

This doc describes the different compiler options for the Jenkins builds. If a cell is left blank it means that the option is not specified thus uses the default.

## Terrier build
http://jenkins.db.cs.cmu.edu:8080/job/terrier/

| STAGE | OS/MACHINE | COMPILER | DNOISEPAGE_UNITY_BUILD | DNOISEPAGE_TEST_PARALLELISM | DCMAKE_CXX_COMPILER_LAUNCHER | DNOISEPAGE_USE_ASAN | DNOISEPAGE_BUILD_BENCHMARKS | DCMAKE_BUILD_TYPE | DNOISEPAGE_USE_JUMBOTESTS | DNOISEPAGE_USE_JEMALLOC | DNOISEPAGE_GENERATE_COVERAGE | DNOISEPAGE_BUILD_TESTS |
|-|-|-|-|-|-|-|-|-|-|-|-|:-:|
| Default Values |  |  | OFF | 1 |  | OFF | ON | Debug | OFF | OFF | OFF | ON |
| Debug/ASAN/unittest | macos | clang-8.0 | ON |  |  | ON | OFF | Debug |  |  |  |  |
| Debug/ASAN/jumbotests | ubuntu | gcc-9.3 | ON | $(nproc) | ccache | ON | OFF | Debug | ON |  |  |  |
| Debug/Coverage/unittest | ubuntu | gcc-9.3 | OFF |  | ccache |  | OFF | Debug |  |  | ON |  |
| Debug/ASAN/jumbotests | ubuntu | clang-8.0 | ON | $(nproc) | ccache | ON | OFF | Debug | ON |  |  |  |
| Release/unittest | macos | clang-8.0 | ON |  |  |  | OFF | Release |  |  |  |  |
| Release/jumbotests | ubuntu | gcc-9.3 | ON | $(nproc) | ccache |  | OFF | Release | ON |  |  |  |
| Release/jumbotests | ubuntu | clang-8.0 | ON | $(nproc) | ccache |  | OFF | Release | ON |  |  |  |
| Debug/e2etest/oltpbench | macos | clang-8.0 | ON |  |  | ON | OFF | Debug |  |  |  | OFF |
| Debug/e2etest/oltpbench | ubuntu | gcc-9.3 | ON |  | ccache | ON | OFF | Debug |  |  |  | OFF |
| End-to-End Performance | benchmark | gcc-9.3 | ON |  | ccache |  | OFF | Release |  | ON |  | OFF |
| Microbenchmark | benchmark | gcc-9.3 | ON |  | ccache |  | ON | Release |  | ON |  | OFF |


## Terrier Nightly build
http://jenkins.db.cs.cmu.edu:8080/job/terrier-nightly/

| STAGE | OS/MACHINE | COMPILER | DNOISEPAGE_UNITY_BUILD | DNOISEPAGE_TEST_PARALLELISM | DCMAKE_CXX_COMPILER_LAUNCHER | DNOISEPAGE_USE_ASAN | DNOISEPAGE_BUILD_BENCHMARKS | DCMAKE_BUILD_TYPE | DNOISEPAGE_USE_JUMBOTESTS | DNOISEPAGE_USE_JEMALLOC | DNOISEPAGE_GENERATE_COVERAGE | DNOISEPAGE_BUILD_TESTS |
|-|-|-|-|-|-|-|-|-|-|-|-|:-:|
| Default Values |  |  | OFF | 1 |  | OFF | ON | Debug | OFF | OFF | OFF | ON |
| Artifact Stats | ubuntu | gcc-9.3 | ON | $(nproc) |  |  | OFF | Release |  | ON |  | OFF |
| Performance | benchmark | gcc-9.3 | ON | $(nproc) | ccache |  | OFF | Release |  | ON |  | OFF |
| Microbenchmark | benchmark | gcc-9.3 | ON | $(nproc) | ccache |  |  | Release |  | ON |  | OFF |