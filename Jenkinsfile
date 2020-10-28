pipeline {
    agent none
    options {
        buildDiscarder(logRotator(daysToKeepStr: '30'))
        parallelsAlwaysFailFast()
    }
    stages {
        stage('Ready For CI') {
            agent {
                docker {
                    image 'noisepage:focal'
                    args '-v /jenkins/ccache:/home/jenkins/.ccache'
                }
            }
            when {
                not {
                    branch 'master'
                }
            }
            steps {
                script {
                   ready_for_build = sh script: 'python3 ./build-support/check_github_labels.py', returnStatus: true
                   if(ready_for_build != 0) {
                        currentBuild.result = 'ABORTED'
                        error('Not ready for CI. Please add ready-for-ci tag in Github when you are ready to build your PR.')
                   }
                }
            }
            post {
                cleanup {
                    deleteDir()
                }
            }
        }
        stage('Check') {
            parallel {
                stage('macos-10.14/clang-8.0 (Debug/format/lint/censored)') {
                    agent { label 'macos' }
                    environment {
                        LIBRARY_PATH="$LIBRARY_PATH:/usr/local/opt/libpqxx/lib/"
                        LLVM_DIR=sh(script: "brew --prefix llvm@8", label: "Fetching LLVM path", returnStdout: true).trim()
                        CC="${LLVM_DIR}/bin/clang"
                        CXX="${LLVM_DIR}/bin/clang++"
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh 'echo y | ./script/installation/packages.sh build'
                        sh 'cd apidoc && doxygen -u Doxyfile.in && doxygen Doxyfile.in 2>warnings.txt && if [ -s warnings.txt ]; then cat warnings.txt; false; fi'
                        sh 'mkdir build'
                        sh 'cd build && cmake -GNinja ..'
                        sh 'cd build && timeout 20m ninja check-format'
                        sh 'cd build && timeout 20m ninja check-lint'
                        sh 'cd build && timeout 20m ninja check-censored'
                    }
                    post {
                        cleanup {
                            deleteDir()
                        }
                    }
                }

                stage('ubuntu-20.04/gcc-9.3 (Debug/format/lint/censored)') {
                    agent {
                        docker {
                            image 'noisepage:focal'
                        }
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh 'echo y | sudo ./script/installation/packages.sh build'
                        sh 'cd apidoc && doxygen -u Doxyfile.in && doxygen Doxyfile.in 2>warnings.txt && if [ -s warnings.txt ]; then cat warnings.txt; false; fi'
                        sh 'mkdir build'
                        sh 'cd build && cmake -GNinja ..'
                        sh 'cd build && timeout 20m ninja check-format'
                        sh 'cd build && timeout 20m ninja check-lint'
                        sh 'cd build && timeout 20m ninja check-censored'
                    }
                    post {
                        cleanup {
                            deleteDir()
                        }
                    }
                }

                stage('ubuntu-20.04/clang-8.0 (Debug/format/lint/censored)') {
                    agent {
                        docker {
                            image 'noisepage:focal'
                        }
                    }
                    environment {
                        CC="/usr/bin/clang-8"
                        CXX="/usr/bin/clang++-8"
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh 'echo y | sudo ./script/installation/packages.sh build'
                        sh 'cd apidoc && doxygen -u Doxyfile.in && doxygen Doxyfile.in 2>warnings.txt && if [ -s warnings.txt ]; then cat warnings.txt; false; fi'
                        sh 'mkdir build'
                        sh 'cd build && cmake -GNinja ..'
                        sh 'cd build && timeout 20m ninja check-format'
                        sh 'cd build && timeout 20m ninja check-lint'
                        sh 'cd build && timeout 20m ninja check-censored'
                    }
                    post {
                        cleanup {
                            deleteDir()
                        }
                    }
                }
            }
        }

        stage('Test') {
            parallel {
                stage('macos-10.14/clang-8.0 (Debug/ASAN/unittest)') {
                    agent { label 'macos' }
                    environment {
                        ASAN_OPTIONS="detect_container_overflow=0"
                        LIBRARY_PATH="$LIBRARY_PATH:/usr/local/opt/libpqxx/lib/"
                        LLVM_DIR=sh(script: "brew --prefix llvm@8", label: "Fetching LLVM path", returnStdout: true).trim()
                        CC="${LLVM_DIR}/bin/clang"
                        CXX="${LLVM_DIR}/bin/clang++"
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh 'echo y | ./script/installation/packages.sh all'
                        sh 'mkdir build'
                        sh 'cd build && cmake -GNinja -DNOISEPAGE_UNITY_BUILD=ON -DNOISEPAGE_TEST_PARALLELISM=1 -DCMAKE_BUILD_TYPE=Debug -DNOISEPAGE_USE_ASAN=ON -DNOISEPAGE_BUILD_BENCHMARKS=OFF -DNOISEPAGE_USE_JUMBOTESTS=OFF .. && ninja'
                        sh 'cd build && ninja check-clang-tidy'
                        sh 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15721'
                        sh 'cd build && gtimeout 1h ninja unittest'
                        sh 'cd build && gtimeout 1h ninja check-tpl'
                        sh 'cd build && gtimeout 20m python3 ../script/testing/junit/run_junit.py --build-type=debug --query-mode=simple'
                        sh 'cd build && gtimeout 20m python3 ../script/testing/junit/run_junit.py --build-type=debug --query-mode=extended'
                    }
                    post {
                        always {
                            archiveArtifacts(artifacts: 'build/Testing/**/*.xml', fingerprint: true)
                            xunit reduceLog: false, tools: [CTest(deleteOutputFiles: false, failIfNotNew: false, pattern: 'build/Testing/**/*.xml', skipNoTestFiles: false, stopProcessingIfError: false)]
                        }
                        cleanup {
                            deleteDir()
                        }
                    }
                }

                stage('ubuntu-20.04/gcc-9.3 (Debug/ASAN/jumbotests)') {
                    agent {
                        docker {
                            image 'noisepage:focal'
                            args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache'
                        }
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh 'echo y | sudo ./script/installation/packages.sh all'
                        sh 'mkdir build'
                        sh 'cd build && cmake -GNinja -DNOISEPAGE_UNITY_BUILD=ON -DNOISEPAGE_TEST_PARALLELISM=$(nproc) -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DNOISEPAGE_USE_ASAN=ON -DNOISEPAGE_BUILD_BENCHMARKS=OFF -DNOISEPAGE_USE_JUMBOTESTS=ON .. && ninja'
                        sh 'cd build && ninja check-clang-tidy'
                        sh 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15721'
                        sh 'cd build && timeout 1h ninja jumbotests'
                        sh 'cd build && timeout 1h ninja check-tpl'
                        sh 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=debug --query-mode=simple'
                        sh 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=debug --query-mode=extended'
                    }
                    post {
                        always {
                            archiveArtifacts(artifacts: 'build/Testing/**/*.xml', fingerprint: true)
                            xunit reduceLog: false, tools: [CTest(deleteOutputFiles: false, failIfNotNew: false, pattern: 'build/Testing/**/*.xml', skipNoTestFiles: false, stopProcessingIfError: false)]
                        }
                        cleanup {
                            deleteDir()
                        }
                    }
                }

                stage('ubuntu-20.04/gcc-9.3 (Debug/Coverage/unittest)') {
                    agent {
                        docker {
                            image 'noisepage:focal'
                            args '-v /jenkins/ccache:/home/jenkins/.ccache'
                        }
                    }
                    environment {
                        CODECOV_TOKEN=credentials('codecov-token')
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh 'echo y | sudo ./script/installation/packages.sh all'
                        sh 'mkdir build'
                        sh 'cd build && cmake -GNinja -DNOISEPAGE_UNITY_BUILD=OFF -DNOISEPAGE_TEST_PARALLELISM=1 -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DNOISEPAGE_USE_ASAN=OFF -DNOISEPAGE_BUILD_BENCHMARKS=OFF -DNOISEPAGE_GENERATE_COVERAGE=ON .. && ninja'
                        sh 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15721'
                        sh 'cd build && timeout 1h ninja unittest'
                        sh 'cd build && timeout 1h ninja check-tpl'
                        sh 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=debug --query-mode=simple'
                        sh 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=debug --query-mode=extended'
                        sh 'cd build && lcov --directory . --capture --output-file coverage.info'
                        sh 'cd build && lcov --remove coverage.info \'/usr/*\' --output-file coverage.info'
                        sh 'cd build && lcov --remove coverage.info \'*/build/*\' --output-file coverage.info'
                        sh 'cd build && lcov --remove coverage.info \'*/third_party/*\' --output-file coverage.info'
                        sh 'cd build && lcov --remove coverage.info \'*/benchmark/*\' --output-file coverage.info'
                        sh 'cd build && lcov --remove coverage.info \'*/test/*\' --output-file coverage.info'
                        sh 'cd build && lcov --remove coverage.info \'*/src/main/*\' --output-file coverage.info'
                        sh 'cd build && lcov --remove coverage.info \'*/src/include/common/error/*\' --output-file coverage.info'
                        sh 'cd build && lcov --list coverage.info'
                        sh 'cd build && curl -s https://codecov.io/bash > ./codecov.sh'
                        sh 'cd build && chmod a+x ./codecov.sh'
                        sh 'cd build && /bin/bash ./codecov.sh -X gcov'
                    }
                    post {
                        always {
                            archiveArtifacts(artifacts: 'build/Testing/**/*.xml', fingerprint: true)
                            xunit reduceLog: false, tools: [CTest(deleteOutputFiles: false, failIfNotNew: false, pattern: 'build/Testing/**/*.xml', skipNoTestFiles: false, stopProcessingIfError: false)]
                        }
                        cleanup {
                            deleteDir()
                        }
                    }
                }

                stage('ubuntu-20.04/clang-8.0 (Debug/ASAN/jumbotests)') {
                    agent {
                        docker {
                            image 'noisepage:focal'
                            args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache'
                        }
                    }
                    environment {
                        CC="/usr/bin/clang-8"
                        CXX="/usr/bin/clang++-8"
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh 'echo y | sudo ./script/installation/packages.sh all'
                        sh 'mkdir build'
                        sh 'cd build && cmake -GNinja -DNOISEPAGE_UNITY_BUILD=ON -DNOISEPAGE_TEST_PARALLELISM=$(nproc) -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DNOISEPAGE_USE_ASAN=ON -DNOISEPAGE_BUILD_BENCHMARKS=OFF -DNOISEPAGE_USE_JUMBOTESTS=ON .. && ninja'
                        sh 'cd build && ninja check-clang-tidy'
                        sh 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15721'
                        sh 'cd build && timeout 1h ninja jumbotests'
                        sh 'cd build && timeout 1h ninja check-tpl'
                        sh 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=debug --query-mode=simple'
                        sh 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=debug --query-mode=extended'
                    }
                    post {
                        always {
                            archiveArtifacts(artifacts: 'build/Testing/**/*.xml', fingerprint: true)
                            xunit reduceLog: false, tools: [CTest(deleteOutputFiles: false, failIfNotNew: false, pattern: 'build/Testing/**/*.xml', skipNoTestFiles: false, stopProcessingIfError: false)]
                        }
                        cleanup {
                            deleteDir()
                        }
                    }
                }

                stage('macos-10.14/clang-8.0 (Release/unittest)') {
                    agent { label 'macos' }
                    environment {
                        ASAN_OPTIONS="detect_container_overflow=0"
                        LIBRARY_PATH="$LIBRARY_PATH:/usr/local/opt/libpqxx/lib/"
                        LLVM_DIR=sh(script: "brew --prefix llvm@8", label: "Fetching LLVM path", returnStdout: true).trim()
                        CC="${LLVM_DIR}/bin/clang"
                        CXX="${LLVM_DIR}/bin/clang++"
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh 'echo y | ./script/installation/packages.sh all'
                        sh 'mkdir build'
                        sh 'cd build && cmake -GNinja -DNOISEPAGE_UNITY_BUILD=ON -DNOISEPAGE_TEST_PARALLELISM=1 -DCMAKE_BUILD_TYPE=Release -DNOISEPAGE_USE_ASAN=OFF -DNOISEPAGE_BUILD_BENCHMARKS=OFF -DNOISEPAGE_USE_JUMBOTESTS=OFF .. && ninja'
                        sh 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15721'
                        sh 'cd build && gtimeout 1h ninja unittest'
                        sh 'cd build && gtimeout 1h ninja check-tpl'
                        sh 'cd build && gtimeout 20m python3 ../script/testing/junit/run_junit.py --build-type=release --query-mode=simple'
                        sh 'cd build && gtimeout 20m python3 ../script/testing/junit/run_junit.py --build-type=release --query-mode=extended'
                    }
                    post {
                        always {
                            archiveArtifacts(artifacts: 'build/Testing/**/*.xml', fingerprint: true)
                            xunit reduceLog: false, tools: [CTest(deleteOutputFiles: false, failIfNotNew: false, pattern: 'build/Testing/**/*.xml', skipNoTestFiles: false, stopProcessingIfError: false)]
                        }
                        cleanup {
                            deleteDir()
                        }
                    }
                }

                stage('ubuntu-20.04/gcc-9.3 (Release/jumbotests)') {
                    agent {
                        docker {
                            image 'noisepage:focal'
                            args '-v /jenkins/ccache:/home/jenkins/.ccache'
                        }
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh 'echo y | sudo ./script/installation/packages.sh all'
                        sh 'mkdir build'
                        sh 'cd build && cmake -GNinja -DNOISEPAGE_UNITY_BUILD=ON -DNOISEPAGE_TEST_PARALLELISM=$(nproc) -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DNOISEPAGE_USE_ASAN=OFF -DNOISEPAGE_BUILD_BENCHMARKS=OFF -DNOISEPAGE_USE_JUMBOTESTS=ON .. && ninja'
                        sh 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15721'
                        sh 'cd build && timeout 1h ninja jumbotests'
                        sh 'cd build && timeout 1h ninja check-tpl'
                        sh 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=release --query-mode=simple'
                        sh 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=release --query-mode=extended'
                    }
                    post {
                        always {
                            archiveArtifacts(artifacts: 'build/Testing/**/*.xml', fingerprint: true)
                            xunit reduceLog: false, tools: [CTest(deleteOutputFiles: false, failIfNotNew: false, pattern: 'build/Testing/**/*.xml', skipNoTestFiles: false, stopProcessingIfError: false)]
                        }
                        cleanup {
                            deleteDir()
                        }
                    }
                }

                stage('ubuntu-20.04/clang-8.0 (Release/jumbotests)') {
                    agent {
                        docker {
                            image 'noisepage:focal'
                            args '-v /jenkins/ccache:/home/jenkins/.ccache'
                        }
                    }
                    environment {
                        CC="/usr/bin/clang-8"
                        CXX="/usr/bin/clang++-8"
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh 'echo y | sudo ./script/installation/packages.sh all'
                        sh 'mkdir build'
                        sh 'cd build && cmake -GNinja -DNOISEPAGE_UNITY_BUILD=ON -DNOISEPAGE_TEST_PARALLELISM=$(nproc) -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DNOISEPAGE_USE_ASAN=OFF -DNOISEPAGE_BUILD_BENCHMARKS=OFF -DNOISEPAGE_USE_JUMBOTESTS=ON .. && ninja'
                        sh 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15721'
                        sh 'cd build && timeout 1h ninja jumbotests'
                        sh 'cd build && timeout 1h ninja check-tpl'
                        sh 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=release --query-mode=simple'
                        sh 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=release --query-mode=extended'
                    }
                    post {
                        always {
                            archiveArtifacts(artifacts: 'build/Testing/**/*.xml', fingerprint: true)
                            xunit reduceLog: false, tools: [CTest(deleteOutputFiles: false, failIfNotNew: false, pattern: 'build/Testing/**/*.xml', skipNoTestFiles: false, stopProcessingIfError: false)]
                        }
                        cleanup {
                            deleteDir()
                        }
                    }
                }
            }
        }

        stage('End-to-End Debug') {
            parallel{
                stage('macos-10.14/clang-8.0 (Debug/e2etest/oltpbench)') {
                    agent { label 'macos' }
                    environment {
                        ASAN_OPTIONS="detect_container_overflow=0"
                        LLVM_DIR=sh(script: "brew --prefix llvm@8", label: "Fetching LLVM path", returnStdout: true).trim()
                        CC="${LLVM_DIR}/bin/clang"
                        CXX="${LLVM_DIR}/bin/clang++"
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh 'echo y | ./script/installation/packages.sh all'
                        sh 'mkdir build'
                        sh 'cd build && cmake -GNinja -DNOISEPAGE_UNITY_BUILD=ON -DCMAKE_BUILD_TYPE=Debug -DNOISEPAGE_USE_ASAN=ON .. && ninja noisepage'
                        sh 'cd build && gtimeout 10m python3 ../script/testing/oltpbench/run_oltpbench.py  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/tatp.json --build-type=debug'
                        sh 'cd build && gtimeout 10m python3 ../script/testing/oltpbench/run_oltpbench.py  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/tatp_wal_disabled.json --build-type=debug'
                        sh 'cd build && gtimeout 10m python3 ../script/testing/oltpbench/run_oltpbench.py  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/smallbank.json --build-type=debug'
                        sh 'cd build && gtimeout 10m python3 ../script/testing/oltpbench/run_oltpbench.py  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/ycsb.json --build-type=debug'
                        sh 'cd build && gtimeout 5m python3 ../script/testing/oltpbench/run_oltpbench.py  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/noop.json --build-type=debug'
                        sh 'cd build && gtimeout 30m python3 ../script/testing/oltpbench/run_oltpbench.py --config-file=../script/testing/oltpbench/configs/end_to_end_debug/tpcc.json --build-type=debug'
                        sh 'cd build && gtimeout 30m python3 ../script/testing/oltpbench/run_oltpbench.py --config-file=../script/testing/oltpbench/configs/end_to_end_debug/tpcc_parallel_disabled.json --build-type=debug'
                        // TODO: Need to fix OLTP-Bench's TPC-C to support scalefactor correctly
                        // sh 'cd build && gtimeout 1h python3 ../script/testing/oltpbench/run_oltpbench.py tpcc 45,43,4,4,4 --build-type=debug'
                    }
                    post {
                        cleanup {
                            deleteDir()
                        }
                    }
                }
                stage('ubuntu-20.04/gcc-9.3 (Debug/e2etest/oltpbench)') {
                    agent {
                        docker {
                            image 'noisepage:focal'
                            args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache'
                        }
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh 'echo y | sudo ./script/installation/packages.sh all'
                        sh 'mkdir build'
                        sh 'cd build && cmake -GNinja -DNOISEPAGE_UNITY_BUILD=ON -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Debug -DNOISEPAGE_USE_ASAN=ON .. && ninja noisepage'
                        sh 'cd build && timeout 10m python3 ../script/testing/oltpbench/run_oltpbench.py  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/tatp.json --build-type=debug'
                        sh 'cd build && timeout 10m python3 ../script/testing/oltpbench/run_oltpbench.py  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/tatp_wal_disabled.json --build-type=debug'
                        sh 'cd build && timeout 10m python3 ../script/testing/oltpbench/run_oltpbench.py  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/smallbank.json --build-type=debug'
                        sh 'cd build && timeout 10m python3 ../script/testing/oltpbench/run_oltpbench.py  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/ycsb.json --build-type=debug'
                        sh 'cd build && timeout 5m python3 ../script/testing/oltpbench/run_oltpbench.py  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/noop.json --build-type=debug'
                        sh 'cd build && timeout 30m python3 ../script/testing/oltpbench/run_oltpbench.py --config-file=../script/testing/oltpbench/configs/end_to_end_debug/tpcc.json --build-type=debug'
                        sh 'cd build && timeout 30m python3 ../script/testing/oltpbench/run_oltpbench.py --config-file=../script/testing/oltpbench/configs/end_to_end_debug/tpcc_parallel_disabled.json --build-type=debug'
                        // TODO: Need to fix OLTP-Bench's TPC-C to support scalefactor correctly
                        // sh 'cd build && timeout 1h python3 ../script/testing/oltpbench/run_oltpbench.py tpcc 45,43,4,4,4 --build-type=debug --query-mode=simple --scale-factor=0.01 --loader-threads=4'
                    }
                    post {
                        cleanup {
                            deleteDir()
                        }
                    }
                }
            }
        }
        stage('End-to-End Performance') {
            agent { label 'benchmark' }
            steps {
                sh 'echo $NODE_NAME'
                sh 'echo y | sudo ./script/installation/packages.sh all'
                sh 'mkdir build'
                sh 'cd build && cmake -GNinja -DNOISEPAGE_UNITY_BUILD=ON -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DNOISEPAGE_USE_ASAN=OFF -DNOISEPAGE_USE_JEMALLOC=ON .. && ninja noisepage'
                sh 'cd build && timeout 10m python3 ../script/testing/oltpbench/run_oltpbench.py --config-file=../script/testing/oltpbench/configs/end_to_end_performance/tatp.json --build-type=release'
                sh 'cd build && timeout 10m python3 ../script/testing/oltpbench/run_oltpbench.py --config-file=../script/testing/oltpbench/configs/end_to_end_performance/tatp_wal_disabled.json --build-type=release'
                sh 'cd build && timeout 10m python3 ../script/testing/oltpbench/run_oltpbench.py --config-file=../script/testing/oltpbench/configs/end_to_end_performance/tatp_wal_ramdisk.json --build-type=release'
                sh 'cd build && timeout 30m python3 ../script/testing/oltpbench/run_oltpbench.py --config-file=../script/testing/oltpbench/configs/end_to_end_performance/tpcc.json --build-type=release'
                sh 'cd build && timeout 30m python3 ../script/testing/oltpbench/run_oltpbench.py --config-file=../script/testing/oltpbench/configs/end_to_end_performance/tpcc_wal_disabled.json --build-type=release'
                sh 'cd build && timeout 30m python3 ../script/testing/oltpbench/run_oltpbench.py --config-file=../script/testing/oltpbench/configs/end_to_end_performance/tpcc_wal_ramdisk.json --build-type=release'
            }
             post {
                 cleanup {
                     deleteDir()
                 }
             }
        }
        stage('Microbenchmark') {
            agent { label 'benchmark' }
            steps {
                sh 'echo $NODE_NAME'
                sh 'echo y | sudo ./script/installation/packages.sh all'
                sh 'mkdir build'
                sh 'cd build && cmake -GNinja -DNOISEPAGE_UNITY_BUILD=ON -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DNOISEPAGE_USE_ASAN=OFF -DNOISEPAGE_USE_JEMALLOC=ON -DNOISEPAGE_BUILD_TESTS=OFF .. && ninja all'
            }
            post {
                cleanup {
                    deleteDir()
                }
            }
        }
    }
}
