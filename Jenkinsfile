def utils // common build functions are loaded from Jenkinsfile-utils into this object
String utilsFileName  = 'Jenkinsfile-utils'

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
                stage('ubuntu-20.04/gcc-9.3 (Debug/format/lint/censored)') {
                    agent {
                        docker {
                            image 'noisepage:focal'
                        }
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh script: './build-support/print_docker_info.sh', label: 'Print image information.'
                        sh script: 'echo y | sudo ./script/installation/packages.sh build', label: 'Installing packages'
                        sh 'cd apidoc && doxygen -u Doxyfile.in && doxygen Doxyfile.in 2>warnings.txt && if [ -s warnings.txt ]; then cat warnings.txt; false; fi'
                        sh 'mkdir build'
                        sh 'cd build && cmake -GNinja ..'
                        sh 'cd build && timeout 20m ninja check-format'
                        sh 'cd build && timeout 20m ninja check-lint'
                        sh 'cd build && timeout 20m ninja check-censored'
                        sh 'cd build && ninja check-clang-tidy'
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
                        sh script: './build-support/print_docker_info.sh', label: 'Print image information.'
                        sh script: 'echo y | sudo ./script/installation/packages.sh build', label: 'Installing packages'
                        sh 'cd apidoc && doxygen -u Doxyfile.in && doxygen Doxyfile.in 2>warnings.txt && if [ -s warnings.txt ]; then cat warnings.txt; false; fi'
                        sh 'mkdir build'
                        sh 'cd build && cmake -GNinja ..'
                        sh 'cd build && timeout 20m ninja check-format'
                        sh 'cd build && timeout 20m ninja check-lint'
                        sh 'cd build && timeout 20m ninja check-censored'
                        sh 'cd build && ninja check-clang-tidy'
                    }
                    post {
                        cleanup {
                            deleteDir()
                        }
                    }
                }
            }
        }

        stage('Microbenchmark (Build only)') {
            agent {
                docker {
                    image 'noisepage:focal'
                    args '-v /jenkins/ccache:/home/jenkins/.ccache'
                }
            }
            steps {
                sh 'echo $NODE_NAME'
                sh script: './build-support/print_docker_info.sh', label: 'Print image information.'

                script{
                    utils = utils ?: load(utilsFileName)
                    utils.noisePageBuild(isBuildTests:false, isBuildBenchmarks:true)
                }
            }
            post {
                cleanup {
                    deleteDir()
                }
            }
        }

        stage('Test') {
            parallel {
                stage('ubuntu-20.04/gcc-9.3 (Debug/ASAN/jumbotests)') {
                    agent {
                        docker {
                            image 'noisepage:focal'
                            args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache'
                        }
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh script: './build-support/print_docker_info.sh', label: 'Print image information.'

                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(useASAN:true, isJumboTest:true)
                        }

                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15721', label: 'Kill PID(15721)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15722', label: 'Kill PID(15722)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15723', label: 'Kill PID(15723)'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh script: 'cd build/bin && PYTHONPATH=../.. timeout 20m python3 -m script.testing.replication.tests_simple --build-type=debug', label: 'Replication (Simple)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 20m python3 -m script.testing.junit --build-type=debug --query-mode=simple', label: 'UnitTest (Simple)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 60m python3 -m script.testing.junit --build-type=debug --query-mode=simple -a "compiled_query_execution=True" -a "bytecode_handlers_path=./bytecode_handlers_ir.bc"', label: 'UnitTest (Simple, Compiled Execution)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 20m python3 -m script.testing.junit --build-type=debug --query-mode=extended', label: 'UnitTest (Extended)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 60m python3 -m script.testing.junit --build-type=debug --query-mode=extended -a "compiled_query_execution=True" -a "bytecode_handlers_path=./bytecode_handlers_ir.bc"', label: 'UnitTest (Extended, Compiled Execution)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 20m python3 -m script.testing.junit --build-type=debug --query-mode=extended -a "pipeline_metrics_enable=True" -a "pipeline_metrics_sample_rate=100" -a "counters_enable=True" -a "query_trace_metrics_enable=True"', label: 'UnitTest (Extended with pipeline metrics, counters, and query trace metrics)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 60m python3 -m script.testing.junit --build-type=debug --query-mode=extended -a "pipeline_metrics_enable=True" -a "pipeline_metrics_sample_rate=100" -a "counters_enable=True" -a "query_trace_metrics_enable=True" -a "compiled_query_execution=True" -a "bytecode_handlers_path=./bytecode_handlers_ir.bc"', label: 'UnitTest (Extended, Compiled Execution with pipeline metrics, counters, and query trace metrics)'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh 'cd build && timeout 1h ninja check-tpl'
                        sh 'cd build && timeout 1h ninja jumbotests'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
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
                            label 'dgb'
                            args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache'
                        }
                    }
                    environment {
                        CODECOV_TOKEN=credentials('codecov-token')
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh script: './build-support/print_docker_info.sh', label: 'Print image information.'

                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(isCodeCoverage:true)
                        }

                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15721', label: 'Kill PID(15721)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15722', label: 'Kill PID(15722)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15723', label: 'Kill PID(15723)'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh script: 'cd build && PYTHONPATH=.. timeout 20m python3 -m script.testing.junit --build-type=debug --query-mode=simple', label: 'UnitTest (Simple)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 60m python3 -m script.testing.junit --build-type=debug --query-mode=simple -a "compiled_query_execution=True" -a "bytecode_handlers_path=./bytecode_handlers_ir.bc"', label: 'UnitTest (Simple, Compiled Execution)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 20m python3 -m script.testing.junit --build-type=debug --query-mode=extended', label: 'UnitTest (Extended)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 60m python3 -m script.testing.junit --build-type=debug --query-mode=extended -a "compiled_query_execution=True" -a "bytecode_handlers_path=./bytecode_handlers_ir.bc"', label: 'UnitTest (Extended, Compiled Execution)'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh 'cd build && timeout 1h ninja check-tpl'
                        sh 'cd build && timeout 1h ninja unittest'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'

                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.cppCoverage()
                        }

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
                        sh script: './build-support/print_docker_info.sh', label: 'Print image information.'

                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(useASAN:true, isJumboTest:true)
                        }

                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15721', label: 'Kill PID(15721)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15722', label: 'Kill PID(15722)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15723', label: 'Kill PID(15723)'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh script: 'cd build/bin && PYTHONPATH=../.. timeout 20m python3 -m script.testing.replication.tests_simple --build-type=debug', label: 'Replication (Simple)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 20m python3 -m script.testing.junit --build-type=debug --query-mode=simple', label: 'UnitTest (Simple)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 60m python3 -m script.testing.junit --build-type=debug --query-mode=simple -a "compiled_query_execution=True" -a "bytecode_handlers_path=./bytecode_handlers_ir.bc"', label: 'UnitTest (Simple, Compiled Execution)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 20m python3 -m script.testing.junit --build-type=debug --query-mode=extended', label: 'UnitTest (Extended)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 60m python3 -m script.testing.junit --build-type=debug --query-mode=extended -a "compiled_query_execution=True" -a "bytecode_handlers_path=./bytecode_handlers_ir.bc"', label: 'UnitTest (Extended, Compiled Execution)'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh 'cd build && timeout 1h ninja check-tpl'
                        sh 'cd build && timeout 1h ninja jumbotests'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
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
                            args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache'
                        }
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh script: './build-support/print_docker_info.sh', label: 'Print image information.'

                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(buildType:utils.RELEASE_BUILD, isJumboTest:true)
                        }

                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15721', label: 'Kill PID(15721)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15722', label: 'Kill PID(15722)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15723', label: 'Kill PID(15723)'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh script: 'cd build/bin && PYTHONPATH=../.. timeout 20m python3 -m script.testing.replication.tests_simple --build-type=release', label: 'Replication (Simple)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 20m python3 -m script.testing.junit --build-type=release --query-mode=simple', label: 'UnitTest (Simple)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 60m python3 -m script.testing.junit --build-type=release --query-mode=simple -a "compiled_query_execution=True" -a "bytecode_handlers_path=./bytecode_handlers_ir.bc"', label: 'UnitTest (Simple, Compiled Execution)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 20m python3 -m script.testing.junit --build-type=release --query-mode=extended', label: 'UnitTest (Extended)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 60m python3 -m script.testing.junit --build-type=release --query-mode=extended -a "compiled_query_execution=True" -a "bytecode_handlers_path=./bytecode_handlers_ir.bc"', label: 'UnitTest (Extended, Compiled Execution)'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh 'cd build && timeout 1h ninja check-tpl'
                        sh 'cd build && timeout 1h ninja jumbotests'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
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
                            args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache'
                        }
                    }
                    environment {
                        CC="/usr/bin/clang-8"
                        CXX="/usr/bin/clang++-8"
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh script: './build-support/print_docker_info.sh', label: 'Print image information.'

                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(buildType:utils.RELEASE_BUILD, isJumboTest:true)
                        }

                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15721', label: 'Kill PID(15721)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15722', label: 'Kill PID(15722)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15723', label: 'Kill PID(15723)'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh script: 'cd build/bin && PYTHONPATH=../.. timeout 20m python3 -m script.testing.replication.tests_simple --build-type=release', label: 'Replication (Simple)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 20m python3 -m script.testing.junit --build-type=release --query-mode=simple', label: 'UnitTest (Simple)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 60m python3 -m script.testing.junit --build-type=release --query-mode=simple -a "compiled_query_execution=True" -a "bytecode_handlers_path=./bytecode_handlers_ir.bc"', label: 'UnitTest (Simple, Compiled Execution)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 20m python3 -m script.testing.junit --build-type=release --query-mode=extended', label: 'UnitTest (Extended)'
                        sh script: 'cd build && PYTHONPATH=.. timeout 60m python3 -m script.testing.junit --build-type=release --query-mode=extended -a "compiled_query_execution=True" -a "bytecode_handlers_path=./bytecode_handlers_ir.bc"', label: 'UnitTest (Extended, Compiled Execution)'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh 'cd build && timeout 1h ninja check-tpl'
                        sh 'cd build && timeout 1h ninja jumbotests'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
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

        stage('End-to-End') {
            parallel {
                stage('Debug') {
                    agent {
                        docker {
                            image 'noisepage:focal'
                            args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache'
                        }
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh script: './build-support/print_docker_info.sh', label: 'Print image information.'

                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(useASAN:true, isBuildTests:false)
                        }

                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'

                        sh script: '''
                        cd build
                        PYTHONPATH=.. timeout 10m python3 -m script.testing.oltpbench  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/tatp.json --build-type=debug
                        ''', label:'OLTPBench (TATP)'

                        sh script: '''
                        cd build
                        PYTHONPATH=.. timeout 10m python3 -m script.testing.oltpbench  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/tatp_wal_disabled.json --build-type=debug
                        ''', label: 'OLTPBench (No WAL)'

                        sh script: '''
                        cd build
                        PYTHONPATH=.. timeout 10m python3 -m script.testing.oltpbench  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/smallbank.json --build-type=debug
                        ''', label:'OLTPBench (Smallbank)'

                        sh script: '''
                        cd build
                        PYTHONPATH=.. timeout 10m python3 -m script.testing.oltpbench  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/ycsb.json --build-type=debug
                        ''', label: 'OLTPBench (YCSB)'

                        sh script: '''
                        cd build
                        PYTHONPATH=.. timeout 5m python3 -m script.testing.oltpbench  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/noop.json --build-type=debug
                        ''', label: 'OLTPBench (NOOP)'

                        // TODO: Need to fix OLTP-Bench's TPC-C to support scalefactor correctly
                        sh script: '''
                        cd build
                        PYTHONPATH=.. timeout 30m python3 -m script.testing.oltpbench --config-file=../script/testing/oltpbench/configs/end_to_end_debug/tpcc.json --build-type=debug
                        ''', label: 'OLTPBench (TPCC)'

                        sh script: '''
                        cd build
                        PYTHONPATH=.. timeout 30m python3 -m script.testing.oltpbench --config-file=../script/testing/oltpbench/configs/end_to_end_debug/tpcc_parallel_disabled.json --build-type=debug
                        ''', label: 'OLTPBench (No Parallel)'

                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                    }
                    post {
                        cleanup {
                            deleteDir()
                        }
                    }
                }
                stage('Performance') {
                    agent { label 'benchmark' }
                    environment {
                        //Do not change.
                        //Performance Storage Service(Django) authentication information. The credentials can only be changed on Jenkins webpage
                        PSS_CREATOR = credentials('pss-creator')
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh script: './build-support/print_docker_info.sh', label: 'Print image information.'

                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(buildType:utils.RELEASE_BUILD, isBuildTests:false)
                        }

                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'

                        sh script:'''
                        cd build
                        PYTHONPATH=.. timeout 10m python3 -m script.testing.oltpbench --config-file=../script/testing/oltpbench/configs/end_to_end_performance/tatp.json --build-type=release --publish-results=prod --publish-username=${PSS_CREATOR_USR} --publish-password=${PSS_CREATOR_PSW}
                        ''', label: 'OLTPBench (TATP)'

                        sh script:'''
                        cd build
                        PYTHONPATH=.. timeout 10m python3 -m script.testing.oltpbench --config-file=../script/testing/oltpbench/configs/end_to_end_performance/tatp_wal_disabled.json --build-type=release --publish-results=prod --publish-username=${PSS_CREATOR_USR} --publish-password=${PSS_CREATOR_PSW}
                        ''', label: 'OLTPBench (TATP No WAL)'

                        sh script:'''
                        cd build
                        PYTHONPATH=.. timeout 10m python3 -m script.testing.oltpbench --config-file=../script/testing/oltpbench/configs/end_to_end_performance/tatp_wal_ramdisk.json --build-type=release --publish-results=prod --publish-username=${PSS_CREATOR_USR} --publish-password=${PSS_CREATOR_PSW}
                        ''', label: 'OLTPBench (TATP RamDisk WAL)'

                        sh script:'''
                        cd build
                        PYTHONPATH=.. timeout 30m python3 -m script.testing.oltpbench --config-file=../script/testing/oltpbench/configs/end_to_end_performance/tpcc.json --build-type=release --publish-results=prod --publish-username=${PSS_CREATOR_USR} --publish-password=${PSS_CREATOR_PSW}
                        ''', label: 'OLTPBench (TPCC HDD WAL)'

                        sh script:'''
                        cd build
                        PYTHONPATH=.. timeout 30m python3 -m script.testing.oltpbench --config-file=../script/testing/oltpbench/configs/end_to_end_performance/tpcc_wal_disabled.json --build-type=release --publish-results=prod --publish-username=${PSS_CREATOR_USR} --publish-password=${PSS_CREATOR_PSW}
                        ''', label: 'OLTPBench (TPCC No WAL)'

                        sh script:'''
                        cd build
                        PYTHONPATH=.. timeout 30m python3 -m script.testing.oltpbench --config-file=../script/testing/oltpbench/configs/end_to_end_performance/tpcc_wal_ramdisk.json --build-type=release --publish-results=prod --publish-username=${PSS_CREATOR_USR} --publish-password=${PSS_CREATOR_PSW}
                        ''', label: 'OLTPBench (TPCC RamDisk WAL)'
                    }
                    post {
                        cleanup {
                            deleteDir()
                        }
                    }
                }
            }
        }

        stage('Self-Driving') {
            parallel {
                stage('Workload Forecasting'){
                    agent {
                        docker {
                            image 'noisepage:focal'
                            args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache'
                        }
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh script: './build-support/print_docker_info.sh', label: 'Print image information.'

                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(buildType:utils.RELEASE_BUILD, isBuildTests:false)
                        }

                        // This scripts runs TPCC benchmark with query trace enabled. It also uses SET command to turn
                        // on query trace.
                        // --pattern_iter determines how many times a sequence of TPCC phases is run. Set to 3 so that
                        // enough trace could be generated for training and testing.
                        sh script :'''
                        cd build
                        PYTHONPATH=.. python3 -m script.self_driving.forecasting.forecaster_standalone --generate_data --pattern_iter=3
                        ''', label: 'Generate trace and perform training'

                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'

                        sh script :'''
                        cd build
                        PYTHONPATH=.. python3 -m script.self_driving.forecasting.forecaster_standalone --model_save_path=model.pickle --models=LSTM
                        ''', label: 'Generate trace and perform training'

                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'

                        sh script: '''
                        cd build
                        PYTHONPATH=.. python3 -m script.self_driving.forecasting.forecaster_standalone --test_file=query_trace.csv --model_load_path=model.pickle --test_model=LSTM
                        ''', label: 'Perform inference on the trained model'

                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                    }
                    post {
                        cleanup {
                            deleteDir()
                        }
                    }
                }
                stage('Modeling'){
                    agent {
                        docker {
                            image 'noisepage:focal'
                            label 'dgb'
                            args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache'
                        }
                    }
                    environment {
                        CODECOV_TOKEN=credentials('codecov-token')
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh script: './build-support/print_docker_info.sh', label: 'Print image information.'

                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(buildType:utils.RELEASE_BUILD, isBuildTests:false, isBuildSelfDrivingE2ETests: true)
                        }

                        // This scripts runs TPCC benchmark with query trace enabled. It also uses SET command to turn
                        // on query trace.
                        // --pattern_iter determines how many times a sequence of TPCC phases is run. Set to 3 so that
                        // enough trace could be generated for training and testing.
                        sh script :'''
                        cd build
                        PYTHONPATH=.. python3 -m script.self_driving.forecasting.forecaster_standalone --generate_data --pattern_iter=3
                        ''', label: 'Forecasting model training data generation'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'

                        // This scripts runs TPCC benchmark with pipeline metrics enabled.
                        sh script :'''
                        cd build
                        PYTHONPATH=.. python3 -m script.self_driving.forecasting.forecaster_standalone --generate_data --record_pipeline_metrics --pattern_iter=1
                        mkdir concurrent_runner_input
                        mv pipeline.csv concurrent_runner_input
                        ''', label: 'Interference model training data generation'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'

                        // The parameters to the execution_runners target are (arbitrarily picked to complete tests within a reasonable time / picked to exercise all OUs).
                        // Specifically, the parameters chosen are:
                        // - execution_runner_rows_limit=100, which sets the maximal number of rows/tuples processed to be 100 (small table)
                        // - rerun=0, which skips rerun since we are not testing benchmark performance here
                        // - warm_num=1, which also tests the warm up phase for the execution_runners.
                        // With the current set of parameters, the input generation process will finish under 10min
                        sh script :'''
                        cd build/bin
                        ../benchmark/execution_runners --execution_runner_rows_limit=100 --rerun=0 --warm_num=1
                        ''', label: 'OU model training data generation'

                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'

                        // Recompile the c++ binaries in Debug mode to generate code coverage. We had to compile in
                        // Release mode first to efficiently generate the data required by the tests
                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(isCodeCoverage:true, isBuildTests:false, isBuildSelfDrivingE2ETests: true)
                        }

                        sh script: '''
                        cd build
                        export BUILD_ABS_PATH=`pwd`
                        timeout 10m ninja self_driving_e2e_test
                        ''', label: 'Running self-driving end-to-end test'

                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'

                        // We need `coverage combine` because coverage files are generated separately for each test and
                        // then moved into the build root by `run-test.sh`
                        sh script :'''
                        cd build
                        coverage combine
                        ''', label: 'Combine Python code coverage'

                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.cppCoverage()
                        }

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
    }
}
