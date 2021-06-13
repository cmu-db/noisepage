#!/usr/bin/env groovy

// General structure of this file:
//
// SECTION: Stage functions.
// - stagePre()     : Function that should be invoked at the start of every stageFoo() function.
// - stagePost()    : Function that should be invoked at the end of every stageFoo() function.
// - stageFoo()     : A Jenkins stage.
//
// SECTION: Utility functions.
// Random helper functions.
//
// You should probably know about Groovy's elvis operator ?:,
// where a ?: b means
//      if (a) { return a; } else { return b; }

// SECTION: Stage functions.

/** This should be invoked before every stage. */
void stagePre() {
    sh script: 'echo $NODE_NAME', label: 'Print node name.'
    sh script: './build-support/print_docker_info.sh', label: 'Print image information.'
}

/** This should be invoked after every stage. */
void stagePost() {
    // No-op.
}

/** Test if the GitHub "ready-for-ci" label is present. Otherwise, abort the build. */
void stageGithub() {
    stagePre()
    ready_for_build = sh script: 'python3 ./build-support/check_github_labels.py', returnStatus: true, label: 'Test Github labels.'
    if (0 != ready_for_build) {
        currentBuild.result = 'ABORTED'
        error('Not ready for CI. Please add ready-for-ci tag in Github when you are ready to build your PR.')
    }
    stagePost()
}

/** Test if the codebase passes basic checks: format, documentation, lint, clang-tidy. */
void stageCheck() {
    stagePre()
    installPackages('build')
    sh 'cd apidoc && doxygen -u Doxyfile.in && doxygen Doxyfile.in 2>warnings.txt && if [ -s warnings.txt ]; then cat warnings.txt; false; fi'
    sh 'mkdir build'
    sh 'cd build && cmake -GNinja ..'
    sh 'cd build && timeout 20m ninja check-format'
    sh 'cd build && timeout 20m ninja check-lint'
    sh 'cd build && timeout 20m ninja check-censored'
    if (env.BRANCH_NAME != 'master') {
        sh 'cd build && ninja check-clang-tidy'
    } else {
        sh 'cd build && ninja check-clang-tidy-full'
    }
    stagePost()
}

/** Build and run the default "ninja" target.  */
void stageBuildDefault(Map args = [:]) {
    stagePre()
    installPackages()
    buildNoisePage(args)
    stagePost()
}

/** Run the C++ unit tests, TPL tests, optionally generate coverage. */
void stageTest(Boolean runPipelineMetrics, Map args = [:]) {
    stagePre()
    installPackages()
    buildNoisePage(args)

    sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15721', label: 'Kill port (15721)'
    sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15722', label: 'Kill port (15722)'
    sh script: 'cd build && timeout 10s sudo python3 -B ../script/testing/kill_server.py 15723', label: 'Kill port (15723)'

    buildType = (args.cmake.toUpperCase().contains("CMAKE_BUILD_TYPE=RELEASE")) ? "release" : "debug"

    sh script: "cd build && PYTHONPATH=.. timeout 20m python3 -m script.testing.junit --build-type=$buildType --query-mode=simple", label: 'UnitTest (Simple)'
    sh script: "cd build && PYTHONPATH=.. timeout 60m python3 -m script.testing.junit --build-type=$buildType --query-mode=simple -a 'compiled_query_execution=True' -a 'bytecode_handlers_path=./bytecode_handlers_ir.bc'", label: 'UnitTest (Simple, Compiled Execution)'
    sh script: "cd build && PYTHONPATH=.. timeout 20m python3 -m script.testing.junit --build-type=$buildType --query-mode=extended", label: 'UnitTest (Extended)'
    sh script: "cd build && PYTHONPATH=.. timeout 60m python3 -m script.testing.junit --build-type=$buildType --query-mode=extended -a 'compiled_query_execution=True' -a 'bytecode_handlers_path=./bytecode_handlers_ir.bc'", label: 'UnitTest (Extended, Compiled Execution)'

    if (runPipelineMetrics) {
        sh script: "cd build && PYTHONPATH=.. timeout 20m python3 -m script.testing.junit --build-type=$buildType --query-mode=extended -a 'pipeline_metrics_enable=True' -a 'pipeline_metrics_sample_rate=100' -a 'counters_enable=True' -a 'query_trace_metrics_enable=True'", label: 'UnitTest (Extended with pipeline metrics, counters, and query trace metrics)'
        sh script: "cd build && PYTHONPATH=.. timeout 60m python3 -m script.testing.junit --build-type=$buildType --query-mode=extended -a 'pipeline_metrics_enable=True' -a 'pipeline_metrics_sample_rate=100' -a 'counters_enable=True' -a 'query_trace_metrics_enable=True' -a 'compiled_query_execution=True' -a 'bytecode_handlers_path=./bytecode_handlers_ir.bc'", label: 'UnitTest (Extended, Compiled Execution with pipeline metrics, counters, and query trace metrics)'
    }

    sh 'cd build && timeout 1h ninja check-tpl'

    if (args.cmake.toUpperCase().contains("NOISEPAGE_USE_JUMBOTESTS=ON")) {
        sh 'cd build && export BUILD_ABS_PATH=`pwd` && timeout 1h ninja jumbotests'
    } else {
        sh 'cd build && export BUILD_ABS_PATH=`pwd` && timeout 1h ninja unittest'
    }

    if (args.cmake.toUpperCase().contains("NOISEPAGE_GENERATE_COVERAGE=ON")) {
        uploadCoverage()
    }

    stagePost()
}

/** Run OLTPBench tests in debug mode. */
void stageOltpbenchDebug() {
    stagePre()
    installPackages()
    buildNoisePage([buildCommand:'ninja noisepage', cmake:
        '-DCMAKE_BUILD_TYPE=Debug -DNOISEPAGE_UNITY_BUILD=ON -DNOISEPAGE_USE_ASAN=ON'
    ])

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

    sh script: '''
    cd build
    PYTHONPATH=.. timeout 30m python3 -m script.testing.oltpbench --config-file=../script/testing/oltpbench/configs/end_to_end_debug/tpcc.json --build-type=debug
    ''', label: 'OLTPBench (TPCC)'

    sh script: '''
    cd build
    PYTHONPATH=.. timeout 30m python3 -m script.testing.oltpbench --config-file=../script/testing/oltpbench/configs/end_to_end_debug/tpcc_parallel_disabled.json --build-type=debug
    ''', label: 'OLTPBench (No Parallel)'

    stagePost()
}

/** Run OLTPBench tests in release mode, additionally publishing results. */
void stageOltpbenchRelease() {
    stagePre()
    installPackages()
    buildNoisePage([buildCommand:'ninja noisepage', cmake:
        '-DCMAKE_BUILD_TYPE=Release -DNOISEPAGE_UNITY_BUILD=ON -DNOISEPAGE_USE_JEMALLOC=ON'
    ])

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

    stagePost()
}

void stageForecasting() {
    stagePre()
    installPackages()
    buildNoisePage([buildCommand:'ninja noisepage', cmake:
        '-DCMAKE_BUILD_TYPE=Release -DNOISEPAGE_UNITY_BUILD=ON -DNOISEPAGE_USE_JEMALLOC=ON'
    ])

    // The forecaster_standalone script runs TPC-C with query trace enabled.
    // The forecaster_standalone script uses SET to enable query trace.
    // --pattern_iter determines how many times to run a sequence of TPC-C phases.
    // --pattern_iter is set to 3 (magic number) to generate enough data for training and testing.
    sh script :'''
    cd build
    PYTHONPATH=.. python3 -m script.self_driving.forecasting.forecaster_standalone --generate_data --pattern_iter=3
    ''', label: 'Generate training data for forecasting model.'

    sh script :'''
    cd build
    PYTHONPATH=.. python3 -m script.self_driving.forecasting.forecaster_standalone --model_save_path=model.pickle --models=LSTM
    ''', label: 'Train the model.'

    sh script: '''
    cd build
    PYTHONPATH=.. python3 -m script.self_driving.forecasting.forecaster_standalone --test_file=query_trace.csv --model_load_path=model.pickle --test_model=LSTM
    ''', label: 'Perform inference on the trained model.'

    stagePost()
}

void stageModeling() {
    stagePre()
    installPackages()

    // Build the noisepage DBMS and the execution_runners binary in release mode for efficient data generation.
    buildNoisePage([buildCommand:'ninja noisepage', cmake:
        '-DCMAKE_BUILD_TYPE=Release -DNOISEPAGE_UNITY_BUILD=ON -DNOISEPAGE_USE_JEMALLOC=ON'
    ])
    buildNoisePageTarget("execution_runners")

    // The forecaster_standalone script runs TPC-C with query trace enabled.
    // The forecaster_standalone script uses SET to enable query trace.
    // --pattern_iter determines how many times to run a sequence of TPC-C phases.
    // --pattern_iter is set to 3 (magic number) to generate enough data for training and testing.
    sh script :'''
    cd build
    PYTHONPATH=.. python3 -m script.self_driving.forecasting.forecaster_standalone --generate_data --pattern_iter=3
    ''', label: 'Generate training data for forecasting model.'

    // This script runs TPC-C with pipeline metrics enabled, saving to build/concurrent_runner_input/pipeline.csv.
    sh script :'''
    cd build
    PYTHONPATH=.. python3 -m script.self_driving.forecasting.forecaster_standalone --generate_data --record_pipeline_metrics_with_counters --pattern_iter=1
    mkdir concurrent_runner_input
    mv pipeline.csv concurrent_runner_input
    ''', label: 'Interference model training data generation'

    // The parameters to the execution_runners target are arbitrarily picked to complete tests within 10 minutes while
    // still exercising all OUs and generating a reasonable amount of training data.
    //
    // Specifically, the parameters chosen are:
    // - execution_runner_rows_limit=100, which sets the max number of rows/tuples processed to be 100 (small table).
    // - rerun=0, which skips rerun since we are not testing benchmark performance here.
    // - warm_num=1, which also tests the warm up phase for the execution_runners.
    sh script :'''
    cd build/bin
    ../benchmark/execution_runners --execution_runner_rows_limit=100 --rerun=0 --warm_num=1
    ''', label: 'OU model training data generation'

    // Recompile the noisepage DBMS in Debug mode with code coverage.
    buildNoisePage([buildCommand:'ninja noisepage', cmake:
        '-DCMAKE_BUILD_TYPE=Debug -DNOISEPAGE_GENERATE_COVERAGE=ON'
    ])

    // Run the self_driving_e2e_test.
    sh script: '''
    cd build
    export BUILD_ABS_PATH=`pwd`
    timeout 10m ninja self_driving_e2e_test
    ''', label: 'Running self-driving end-to-end test'

    // We need `coverage combine` because coverage files are generated separately for each test and then moved into the
    // the build root by `run-test.sh`
    sh script :'''
    cd build
    coverage combine
    ''', label: 'Combine Python code coverage.'

    uploadCoverage()

    stagePost()
}

void stageArchive() {
    archiveArtifacts(artifacts: 'build/Testing/**/*.xml', fingerprint: true)
    xunit reduceLog: false, tools: [CTest(deleteOutputFiles: false, failIfNotNew: false, pattern: 'build/Testing/**/*.xml', skipNoTestFiles: false, stopProcessingIfError: false)]
}

void stageNightlyArtifact() {
    stagePre()
    installPackages()
    buildNoisePage([useCache: false, shouldRecordTime:true, buildCommand:'ninja noisepage', cmake:
        '-DCMAKE_BUILD_TYPE=Release -DNOISEPAGE_UNITY_BUILD=ON -DNOISEPAGE_USE_JEMALLOC=ON'
    ])

    sh script: '''
    cd build
    PYTHONPATH=.. python3 -m script.testing.artifact_stats --publish-results=prod --publish-username=${PSS_CREATOR_USR} --publish-password=${PSS_CREATOR_PSW}
    ''', label: 'Artifact Stats'

    stagePost()
}

void stageNightlyPerformance() {
    stagePre()
    installPackages()
    buildNoisePage([buildCommand:'ninja noisepage', cmake:
        '-DCMAKE_BUILD_TYPE=Release -DNOISEPAGE_UNITY_BUILD=ON -DNOISEPAGE_USE_JEMALLOC=ON'
    ])

    // catchError: set the overall stage to fail, but continue to execute subsequent steps.
    catchError(stageResult: 'Failure'){
        sh script:'''
        cd build
        PYTHONPATH=.. timeout 3h python3 -m script.testing.oltpbench --config-file=../script/testing/oltpbench/configs/nightly/nightly.json --build-type=release --publish-results=prod --publish-username=${PSS_CREATOR_USR} --publish-password=${PSS_CREATOR_PSW}
        ''', label: 'OLTPBench (HDD WAL)'
    }
    catchError(stageResult: 'Failure'){
        sh script:'''
        cd build
        PYTHONPATH=.. timeout 3h python3 -m script.testing.oltpbench --config-file=../script/testing/oltpbench/configs/nightly/nightly_ramdisk.json --build-type=release --publish-results=prod --publish-username=${PSS_CREATOR_USR} --publish-password=${PSS_CREATOR_PSW}
        ''', label: 'OLTPBench (RamDisk WAL)'
    }
    catchError(stageResult: 'Failure'){
        sh script:'''
        cd build
        PYTHONPATH=.. timeout 3h python3 -m script.testing.oltpbench --config-file=../script/testing/oltpbench/configs/nightly/nightly_wal_disabled.json --build-type=release --publish-results=prod --publish-username=${PSS_CREATOR_USR} --publish-password=${PSS_CREATOR_PSW}
        ''', label: 'OLTPBench (No WAL)'
    }

    archiveArtifacts(artifacts: 'build/oltp_result/**/*.*', excludes: 'build/oltp_result/**/*.csv', fingerprint: true)
    stagePost()
}

void stageNightlyMicrobenchmark() {
    stagePre()
    installPackages()
    buildNoisePage([buildCommand:'ninja', cmake:
        '-DCMAKE_BUILD_TYPE=Release -DNOISEPAGE_BUILD_BENCHMARKS=ON -DNOISEPAGE_UNITY_BUILD=ON -DNOISEPAGE_USE_JEMALLOC=ON -DNOISEPAGE_USE_LOGGING=OFF'
    ])

    // The micro_bench configuration has to be consistent because we currently check against previous runs with the same config
    //  # of Threads: 4
    //  WAL Path: Ramdisk
    sh script:'''
    cd script/testing
    PYTHONPATH=../.. python3 -m script.testing.microbench --num-threads=4 --benchmark-path $(pwd)/../../build/benchmark --logfile-path=/mnt/ramdisk/benchmark.log --publish-results=prod --publish-username=${PSS_CREATOR_USR} --publish-password=${PSS_CREATOR_PSW}
    ''', label:'Microbenchmark'

    archiveArtifacts 'script/testing/*.json'
    junit 'script/testing/*.xml'
    stagePost()
}

// SECTION: Utility functions.

/** Install the packages. installType = {build, all}. */
void installPackages(String installType='all') {
    sh script:"echo y | sudo ./script/installation/packages.sh $installType", label: 'Installing packages.'
}

/** Create a build folder, set up CMake flags, and build NoisePage. */
void buildNoisePage(Map args = [:]) {
    // Disable most options by default. Callers should be explicit.
    Map config = [
        useCache: true,
        shouldRecordTime: false,
        buildCommand: 'ninja',
        cmake: '',
    ]

    config << args

    String cmakeCmd = 'cmake -GNinja'
    if (config.useCache) {
        cmakeCmd += ' -DCMAKE_CXX_COMPILER_LAUNCHER=ccache'
    }
    cmakeCmd += ' '
    cmakeCmd += config.cmake
    cmakeCmd += ' ..'

    String buildCmd = config.buildCommand

    buildScript = '''
        mkdir -p build
        cd build
    '''
    buildScript += "$cmakeCmd"
    if (config.shouldRecordTime) {
        buildScript += """
        /usr/bin/time -o /tmp/noisepage-compiletime.txt -f %e sh -c \"$buildCmd\"
        """
    } else {
        buildScript += """
        $buildCmd
        """
    }

    sh script:buildScript, label: 'Build NoisePage.'
}

/** Build the specified target. buildNoisePage() MUST have been called! */
void buildNoisePageTarget(String target) {
    sh script:"""
    cd build
    ninja $target
    """
}

/** Collect and process coverage information from the build directory; upload coverage to Codecov. */
void uploadCoverage() {
    sh script :'''
    cd build
    lcov --directory . --capture --output-file coverage.info
    lcov --remove coverage.info \'/usr/*\' --output-file coverage.info
    lcov --remove coverage.info \'*/build/*\' --output-file coverage.info
    lcov --remove coverage.info \'*/third_party/*\' --output-file coverage.info
    lcov --remove coverage.info \'*/benchmark/*\' --output-file coverage.info
    lcov --remove coverage.info \'*/test/*\' --output-file coverage.info
    lcov --remove coverage.info \'*/src/main/*\' --output-file coverage.info
    lcov --remove coverage.info \'*/src/include/common/error/*\' --output-file coverage.info
    lcov --list coverage.info
    curl -s https://codecov.io/bash | bash -s -- -X gcov
    ''', label: 'Process code coverage and upload to Codecov.'
}

return this
