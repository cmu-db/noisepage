pipeline {
    agent none
    options {
        buildDiscarder(logRotator(daysToKeepStr: '30'))
    }
    stages {
        stage('Build') {
            parallel {

                stage('macos-10.14/AppleClang-1001.0.46.4 (Debug/ASAN/unittest)') {
                    agent { label 'macos' }
                    environment {
                        ASAN_OPTIONS="detect_container_overflow=0"
                        LLVM_DIR="/usr/local/Cellar/llvm/8.0.1"
                    }
                    steps {
                        sh 'echo y | ./script/installation/packages.sh'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DTERRIER_USE_ASAN=ON .. && make -j4'
                        sh 'cd build && make check-clang-tidy'
                        sh 'cd build && gtimeout 1h make unittest'
                        sh 'cd build && python ../script/testing/junit/run_junit.py'
                    }
                }

                stage('ubuntu-18.04/gcc-7.3.0 (Debug/ASAN/unittest)') {
                    agent {
                        docker {
                            image 'ubuntu:bionic'
                            args '--cap-add sys_ptrace'
                        }
                    }
                    steps {
                        sh 'echo y | sudo ./script/installation/packages.sh'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DTERRIER_USE_ASAN=ON .. && make -j4'
                        sh 'cd build && make check-clang-tidy'
                        sh 'cd build && timeout 1h make unittest'
                        sh 'cd build && python ../script/testing/junit/run_junit.py'
                    }
                }

                stage('ubuntu-18.04/clang-8.0.0 (Debug/ASAN/unittest)') {
                    agent {
                        docker {
                            image 'ubuntu:bionic'
                            args '--cap-add sys_ptrace'
                        }
                    }
                    environment {
                        CC="/usr/bin/clang-8"
                        CXX="/usr/bin/clang++-8"
                    }
                    steps {
                        sh 'echo y | sudo ./script/installation/packages.sh'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DTERRIER_USE_ASAN=ON .. && make -j4'
                        sh 'cd build && make check-clang-tidy'
                        sh 'cd build && timeout 1h make unittest'
                        sh 'cd build && python ../script/testing/junit/run_junit.py'
                    }
                }

                stage('macOS-10.14/AppleClang-1001.0.46.4 (Release/unittest)') {
                    agent { label 'macos' }
                    environment {
                        ASAN_OPTIONS="detect_container_overflow=0"
                        LLVM_DIR="/usr/local/Cellar/llvm/8.0.1"
                    }
                    steps {
                        sh 'echo y | ./script/installation/packages.sh'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DTERRIER_USE_ASAN=OFF .. && make -j4'
                        sh 'cd build && gtimeout 1h make unittest'
                        sh 'cd build && python ../script/testing/junit/run_junit.py --build_type=release'
                    }
                }

                stage('ubuntu-18.04/gcc-7.3.0 (Release/unittest)') {
                    agent {
                        docker {
                            image 'ubuntu:bionic'
                        }
                    }
                    steps {
                        sh 'echo y | sudo ./script/installation/packages.sh'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DTERRIER_USE_ASAN=OFF .. && make -j4'
                        sh 'cd build && timeout 1h make unittest'
                        sh 'cd build && python ../script/testing/junit/run_junit.py --build_type=release'
                    }
                }

                stage('ubuntu-18.04/clang-8.0.0 (Release/unittest)') {
                    agent {
                        docker {
                            image 'ubuntu:bionic'
                        }
                    }
                    environment {
                        CC="/usr/bin/clang-8"
                        CXX="/usr/bin/clang++-8"
                    }
                    steps {
                        sh 'echo y | sudo ./script/installation/packages.sh'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DTERRIER_USE_ASAN=OFF .. && make -j4'
                        sh 'cd build && timeout 1h make unittest'
                        sh 'cd build && python ../script/testing/junit/run_junit.py --build_type=release'
                    }
                }

                stage('ubuntu-18.04/gcc-7.3.0 (Release/benchmark)') {
                    agent { label 'benchmark' }
                    steps {
                        sh 'echo y | sudo ./script/installation/packages.sh'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DTERRIER_USE_ASAN=OFF -DTERRIER_USE_JEMALLOC=ON .. && make -j4'
                        sh 'cd build && timeout 1h make runbenchmark'
                        sh 'cd script/micro_bench && ./run_micro_bench.py'
                        archiveArtifacts 'script/micro_bench/*.json'
                        junit 'script/micro_bench/*.xml'
                    }
                }
            }
        }
    }
}
