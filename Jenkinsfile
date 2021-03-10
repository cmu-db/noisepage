def utils // common build functions are loaded from Jenkinsfile-utils into this object
String utilsFileName  = 'Jenkinsfile-utils'

pipeline {
    agent none
    options {
        buildDiscarder(logRotator(daysToKeepStr: '30'))
        parallelsAlwaysFailFast()
    }
    stages {

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
                        cleanup {
                            deleteDir()
                        }
                    }
                }
            }
        }
    }
}
