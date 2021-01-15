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
                        
                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(useASAN:true, isJumboTest:true)
                        }
                        
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15721', label: 'Kill PID(15721)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15722', label: 'Kill PID(15722)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15723', label: 'Kill PID(15723)'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh 'cd build && timeout 1h ninja jumbotests'
                        sh 'cd build && timeout 1h ninja check-tpl'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh script: 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=debug --query-mode=simple', label: 'UnitTest (Simple)'
                        sh script: 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=debug --query-mode=extended', label: 'UnitTest (Extended)'
                        sh script: 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=debug --query-mode=extended -a "pipeline_metrics_enable=True" -a "pipeline_metrics_interval=0" -a "counters_enable=True" -a "query_trace_metrics_enable=True"', label: 'UnitTest (Extended with pipeline metrics, counters, and query trace metrics)'
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
                            args '-v /jenkins/ccache:/home/jenkins/.ccache'
                        }
                    }
                    environment {
                        CODECOV_TOKEN=credentials('codecov-token')
                    }
                    steps {
                        sh 'echo $NODE_NAME'
                        
                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(isCodeCoverage:true)
                        }

                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15721', label: 'Kill PID(15721)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15722', label: 'Kill PID(15722)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15723', label: 'Kill PID(15723)'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh 'cd build && timeout 1h ninja unittest'
                        sh 'cd build && timeout 1h ninja check-tpl'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh script: 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=debug --query-mode=simple', label: 'UnitTest (Simple)'
                        sh script: 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=debug --query-mode=extended', label: 'UnitTest (Extended)'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
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

                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(useASAN:true, isJumboTest:true)
                        }

                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15721', label: 'Kill PID(15721)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15722', label: 'Kill PID(15722)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15723', label: 'Kill PID(15723)'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh 'cd build && timeout 1h ninja jumbotests'
                        sh 'cd build && timeout 1h ninja check-tpl'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh script: 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=debug --query-mode=simple', label: 'UnitTest (Simple)'
                        sh script: 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=debug --query-mode=extended', label: 'UnitTest (Extended)'
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
                            args '-v /jenkins/ccache:/home/jenkins/.ccache'
                        }
                    }
                    steps {
                        sh 'echo $NODE_NAME'

                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(buildType:utils.RELEASE_BUILD, isJumboTest:true)
                        }

                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15721', label: 'Kill PID(15721)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15722', label: 'Kill PID(15722)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15723', label: 'Kill PID(15723)'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh 'cd build && timeout 1h ninja jumbotests'
                        sh 'cd build && timeout 1h ninja check-tpl'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh script: 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=release --query-mode=simple', label: 'UnitTest (Simple)'
                        sh script: 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=release --query-mode=extended', label: 'UnitTest (Extended)'
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
                            args '-v /jenkins/ccache:/home/jenkins/.ccache'
                        }
                    }
                    environment {
                        CC="/usr/bin/clang-8"
                        CXX="/usr/bin/clang++-8"
                    }
                    steps {
                        sh 'echo $NODE_NAME'

                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(buildType:utils.RELEASE_BUILD, isJumboTest:true)
                        }

                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15721', label: 'Kill PID(15721)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15722', label: 'Kill PID(15722)'
                        sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15723', label: 'Kill PID(15723)'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh 'cd build && timeout 1h ninja jumbotests'
                        sh 'cd build && timeout 1h ninja check-tpl'
                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                        sh script: 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=release --query-mode=simple', label: 'UnitTest (Simple)'
                        sh script: 'cd build && timeout 20m python3 ../script/testing/junit/run_junit.py --build-type=release --query-mode=extended', label: 'UnitTest (Extended)'
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

                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(useASAN:true, isBuildTests:false)
                        }

                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'

                        sh script: '''
                        cd build
                        timeout 10m python3 ../script/testing/oltpbench/run_oltpbench.py  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/tatp.json --build-type=debug
                        ''', label:'OLTPBench (TATP)'

                        sh script: '''
                        cd build
                        timeout 10m python3 ../script/testing/oltpbench/run_oltpbench.py  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/tatp_wal_disabled.json --build-type=debug
                        ''', label: 'OLTPBench (No WAL)'

                        sh script: '''
                        cd build
                        timeout 10m python3 ../script/testing/oltpbench/run_oltpbench.py  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/smallbank.json --build-type=debug
                        ''', label:'OLTPBench (Smallbank)'

                        sh script: '''
                        cd build
                        timeout 10m python3 ../script/testing/oltpbench/run_oltpbench.py  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/ycsb.json --build-type=debug
                        ''', label: 'OLTPBench (YCSB)'

                        sh script: '''
                        cd build
                        timeout 5m python3 ../script/testing/oltpbench/run_oltpbench.py  --config-file=../script/testing/oltpbench/configs/end_to_end_debug/noop.json --build-type=debug
                        ''', label: 'OLTPBench (NOOP)'

                        // TODO: Need to fix OLTP-Bench's TPC-C to support scalefactor correctly
                        sh script: '''
                        cd build
                        timeout 30m python3 ../script/testing/oltpbench/run_oltpbench.py --config-file=../script/testing/oltpbench/configs/end_to_end_debug/tpcc.json --build-type=debug
                        ''', label: 'OLTPBench (TPCC)'

                        sh script: '''
                        cd build
                        timeout 30m python3 ../script/testing/oltpbench/run_oltpbench.py --config-file=../script/testing/oltpbench/configs/end_to_end_debug/tpcc_parallel_disabled.json --build-type=debug
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
                        
                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(buildType:utils.RELEASE_BUILD, isBuildTests:false)
                        }

                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'

                        sh script:'''
                        cd build
                        timeout 10m python3 ../script/testing/oltpbench/run_oltpbench.py --config-file=../script/testing/oltpbench/configs/end_to_end_performance/tatp.json --build-type=release --publish-results=prod --publish-username=${PSS_CREATOR_USR} --publish-password=${PSS_CREATOR_PSW}
                        ''', label: 'OLTPBench (TATP)'

                        sh script:'''
                        cd build
                        timeout 10m python3 ../script/testing/oltpbench/run_oltpbench.py --config-file=../script/testing/oltpbench/configs/end_to_end_performance/tatp_wal_disabled.json --build-type=release --publish-results=prod --publish-username=${PSS_CREATOR_USR} --publish-password=${PSS_CREATOR_PSW}
                        ''', label: 'OLTPBench (TATP No WAL)'

                        sh script:'''
                        cd build
                        timeout 10m python3 ../script/testing/oltpbench/run_oltpbench.py --config-file=../script/testing/oltpbench/configs/end_to_end_performance/tatp_wal_ramdisk.json --build-type=release --publish-results=prod --publish-username=${PSS_CREATOR_USR} --publish-password=${PSS_CREATOR_PSW}
                        ''', label: 'OLTPBench (TATP RamDisk WAL)'

                        sh script:'''
                        cd build
                        timeout 30m python3 ../script/testing/oltpbench/run_oltpbench.py --config-file=../script/testing/oltpbench/configs/end_to_end_performance/tpcc.json --build-type=release --publish-results=prod --publish-username=${PSS_CREATOR_USR} --publish-password=${PSS_CREATOR_PSW}
                        ''', label: 'OLTPBench (TPCC HDD WAL)'

                        sh script:'''
                        cd build
                        timeout 30m python3 ../script/testing/oltpbench/run_oltpbench.py --config-file=../script/testing/oltpbench/configs/end_to_end_performance/tpcc_wal_disabled.json --build-type=release --publish-results=prod --publish-username=${PSS_CREATOR_USR} --publish-password=${PSS_CREATOR_PSW}
                        ''', label: 'OLTPBench (TPCC No WAL)'

                        sh script:'''
                        cd build
                        timeout 30m python3 ../script/testing/oltpbench/run_oltpbench.py --config-file=../script/testing/oltpbench/configs/end_to_end_performance/tpcc_wal_ramdisk.json --build-type=release --publish-results=prod --publish-username=${PSS_CREATOR_USR} --publish-password=${PSS_CREATOR_PSW}
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

                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(buildType:utils.RELEASE_BUILD, isBuildTests:false)
                        }

                        // This scripts runs TPCC benchmark with query trace enabled. It also uses SET command to turn
                        // on query trace.
                        // --pattern_iter determines how many times a sequence of TPCC phases is run. Set to 3 so that
                        // enough trace could be generated for training and testing.
                        sh script :'''
                        cd script/forecasting
                        ./forecaster.py --gen_data --pattern_iter=3 --model_save_path=model.pickle --models=LSTM
                        ''', label: 'Generate trace and perform training'

                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'

                        sh script: '''
                        cd script/forecasting
                        ./forecaster.py --test_file=query_trace.csv --model_load_path=model.pickle --test_model=LSTM
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
                            args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache'
                        }
                    }
                    steps {
                        sh 'echo $NODE_NAME'

                        script{
                            utils = utils ?: load(utilsFileName)
                            utils.noisePageBuild(buildType:utils.RELEASE_BUILD, isBuildTests:false, isBuildSelfDrivingTests: true)
                        }

                        // The parameters to the mini_runners target are (arbitrarily picked to complete tests within a reasonable time / picked to exercise all OUs).
                        // Specifically, the parameters chosen are:
                        // - mini_runner_rows_limit=100, which sets the maximal number of rows/tuples processed to be 100 (small table)
                        // - rerun=0, which skips rerun since we are not testing benchmark performance here
                        // - warm_num=1, which also tests the warm up phase for the mini_runners.
                        // With the current set of parameters, the input generation process will finish under 10min
                        sh script :'''
                        cd build/bin
                        ../benchmark/mini_runners --mini_runner_rows_limit=100 --rerun=0 --warm_num=1
                        ''', label: 'Mini-trainer input generation'

                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'

                        sh script: '''
                        cd build
                        export BUILD_ABS_PATH=`pwd`
                        timeout 10m ninja self_driving_test
                        ''', label: 'Running self-driving test'

                        sh script: 'sudo lsof -i -P -n | grep LISTEN || true', label: 'Check ports.'
                    }
                    post {
                        cleanup {
                            deleteDir()
                        }
                    }
                }
            }
        }
        stage('Microbenchmark') {
            agent { label 'benchmark' }
            steps {
                sh 'echo $NODE_NAME'

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
    }
}