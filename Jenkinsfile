pipeline {
    agent none
    options {
        buildDiscarder(logRotator(daysToKeepStr: '30'))
    }
    stages {
        stage('Build') {
            parallel {

                stage('macOS 10.13/Apple clang-1000.10.44.4/llvm-6.0.1 (Debug/ASAN)') {
                    agent { label 'macos' }
                    environment {
                        ASAN_OPTIONS="detect_container_overflow=0"
                        LLVM_DIR="/usr/local/Cellar/llvm@6/6.0.1"
                    }
                    steps {
                        sh 'echo y | ./script/installation/packages.sh'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DTERRIER_USE_ASAN=ON .. && make gflags_ep -j 4 && make googletest_ep -j 4 && make gbenchmark_ep -j 4 && make -j4'
                        sh 'cd build && make check-clang-tidy -j4'
                        sh 'cd build && make unittest -j4'
                    }
                }

                stage('Ubuntu Bionic/gcc-7.3.0/llvm-6.0.0 (Debug/ASAN)') {
                    agent {
                        docker {
                            image 'ubuntu:bionic'
                            args '--cap-add sys_ptrace'
                        }
                    }
                    steps {
                        sh 'echo y | sudo ./script/installation/packages.sh'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DTERRIER_USE_ASAN=ON .. && make gflags_ep -j 4 && make googletest_ep -j 4 && make gbenchmark_ep -j 4 && make -j4'
                        sh 'cd build && make check-clang-tidy -j4'
                        sh 'cd build && make unittest -j4'
                    }
                }

                stage('macOS 10.13/Apple clang-1000.10.44.4/llvm-6.0.1 (Release/unittest)') {
                    agent { label 'macos' }
                    environment {
                        ASAN_OPTIONS="detect_container_overflow=0"
                        LLVM_DIR="/usr/local/Cellar/llvm@6/6.0.1"
                    }
                    steps {
                        sh 'echo y | ./script/installation/packages.sh'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DTERRIER_USE_ASAN=OFF .. && make gflags_ep -j 4 && make googletest_ep -j 4 && make gbenchmark_ep -j 4 && make -j4'
                        sh 'cd build && make unittest -j4'
                    }
                }

                stage('Ubuntu Bionic/gcc-7.3.0/llvm-6.0.0 (Release/unittest)') {
                    agent {
                        docker {
                            image 'ubuntu:bionic'
                        }
                    }
                    steps {
                        sh 'echo y | sudo ./script/installation/packages.sh'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_WARNING_LEVEL=Production .. && make gflags_ep -j 4 && make googletest_ep -j 4 && make gbenchmark_ep -j 4 && make -j4'
                        sh 'cd build && make unittest -j4'
                    }
                }

                stage('Ubuntu Bionic/gcc-7.3.0/llvm-6.0.0 (Release/benchmark)') {
                    agent { label 'benchmark' }
                    steps {
                        sh 'echo y | sudo ./script/installation/packages.sh'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_WARNING_LEVEL=Production .. && make gflags_ep -j 4 && make googletest_ep -j 4 && make gbenchmark_ep -j 4 && make -j4'
                        sh 'cd build && make runbenchmark -j4'
                        sh 'cd script/micro_bench && ./run_micro_bench.py'
                        archiveArtifacts 'script/micro_bench/*.json'
                        junit 'script/micro_bench/*.xml'
                    }
                }
            }
        }
    }
}
