pipeline {
    agent none
    stages {
        stage('Build') {
            parallel {
                // begin gcc builds
                stage('Ubuntu Bionic/gcc-7.3.0/llvm-6.0.0 (Debug/ASAN)') {
                    agent { docker { image 'ubuntu:bionic' } }
                    steps {
                        sh 'sudo /bin/sh -c "echo y | /repo/script/installation/packages.sh"'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DTERRIER_USE_ASAN=ON .. && make -j4'
                        sh 'cd build && make unittest -j4'
                    }
                }

                stage('Ubuntu Bionic/gcc-7.3.0/llvm-6.0.0 (Debug/Coverage)') {
                    agent { docker { image 'ubuntu:bionic' } }
                    steps {
                        sh 'sudo /bin/sh -c "echo y | /repo/script/installation/packages.sh"'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DTERRIER_GENERATE_COVERAGE=ON .. && make -j4'
                        sh 'cd build && make unittest -j4'
                    }
                }

                stage('Ubuntu Bionic/gcc-7.3.0/llvm-6.0.0 (Release/unittest)') {
                    agent { docker { image 'ubuntu:bionic' } }
                    steps {
                        sh 'sudo /bin/sh -c "echo y | /repo/script/installation/packages.sh"'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_WARNING_LEVEL=Production .. && make -j4'
                        sh 'cd build && make unittest -j4'
                    }
                }

                stage('Ubuntu Bionic/gcc-7.3.0/llvm-6.0.0 (Release/benchmark)') {
                    agent { docker { image 'ubuntu:bionic' } }
                    steps {
                        sh 'sudo /bin/sh -c "echo y | /repo/script/installation/packages.sh"'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_WARNING_LEVEL=Production .. && make -j4'
                        sh 'cd build && make runbenchmark -j4'
                    }
                }
                // end gcc builds

                // begin clang builds
                stage('macOS 10.13/Apple LLVM version 9.1.0 (Debug)') {
                    agent { label 'macos' }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_PREFIX_PATH=`llvm-config-3.7 --prefix` -DCMAKE_BUILD_TYPE=Debug -DUSE_SANITIZER=Address -DCOVERALLS=False .. && make -j4'
                        sh 'cd build && ASAN_OPTIONS=detect_container_overflow=0 make check -j4'
                        sh 'cd build && make install'
                        sh 'cd build && bash ../script/testing/psql/psql_test.sh'
                        sh 'cd build && python ../script/validators/jdbc_validator.py'
                        sh 'cd build && ASAN_OPTIONS=detect_container_overflow=0 python ../script/testing/junit/run_junit.py'
                    }
                }
                // end clang builds
            }
        }
    }
}
