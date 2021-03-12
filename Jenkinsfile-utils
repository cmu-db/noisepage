#!/usr/bin/env groovy
ENABLED = 'ON'
DISABLED = 'OFF'
UBUNTU = 'ubuntu'
DEBUG_BUILD = 'Debug'
RELEASE_BUILD = 'Release'
/**
 * noisePageBuild will create a build directory and compile and build the
 * noisepage binary in that directory. The options passed into the method
 * determine the compilation and build options. Refer to the defaultArgs
 * for the different options that can be passed in.
 */
void noisePageBuild(Map args = [:]) {
    Map defaultArgs = [
        useCache: true,
        buildType: DEBUG_BUILD,
        os: UBUNTU,
        isBuildTests: true,
        useASAN: false,
        isBuildBenchmarks: false,
        isCodeCoverage: false,
        isJumboTest: false,
        isRecordTime: false,
        isBuildSelfDrivingE2ETests: false,
    ]
    Map config = defaultArgs << args
    String compileCmd = generateCompileCmd(config)
    String buildCmd = generateBuildCmd(config)
    String buildScript = generateBuildScript(compileCmd, buildCmd, config.isRecordTime)

    sh script:'echo y | sudo ./script/installation/packages.sh all', label: 'Installing packages'

    sh script:buildScript, label: 'Build'
}

/**
 * generateCompileCmd creates the cmake command string. It is based on the
 * config passed into the function. The config arguments are the same as the
 * defaultArgs in noisePageBuild.
 */
String generateCompileCmd(Map config = [:]) {
    Map cmakeArgs = [
        '-DCMAKE_BUILD_TYPE': config.buildType,
        '-DNOISEPAGE_UNITY_BUILD': ENABLED,
        '-DNOISEPAGE_TEST_PARALLELISM': 1,
        '-DNOISEPAGE_USE_ASAN': DISABLED,
        '-DNOISEPAGE_USE_JEMALLOC': DISABLED,
        '-DNOISEPAGE_BUILD_TESTS': ENABLED,
        '-DNOISEPAGE_GENERATE_COVERAGE': DISABLED,
        '-DNOISEPAGE_BUILD_BENCHMARKS': DISABLED,
        '-DNOISEPAGE_USE_JUMBOTESTS': DISABLED,
        '-DNOISEPAGE_BUILD_SELF_DRIVING_E2E_TESTS': DISABLED,
    ]

    if (config.useCache) {
        // currently ccache is only configured for ubuntu images
        // For more info: https://github.com/cmu-db/noisepage/pull/830
        cmakeArgs['-DCMAKE_CXX_COMPILER_LAUNCHER'] = 'ccache'
    }

    if (config.useASAN) {
        cmakeArgs['-DNOISEPAGE_USE_ASAN'] = ENABLED
    }

    if (config.buildType == RELEASE_BUILD && !config.isBuildTests && !config.useASAN) {
        cmakeArgs['-DNOISEPAGE_USE_JEMALLOC'] = ENABLED
    }

    if (config.isCodeCoverage) {
        cmakeArgs['-DNOISEPAGE_GENERATE_COVERAGE'] = ENABLED
        cmakeArgs['-DNOISEPAGE_UNITY_BUILD'] = DISABLED
        // unity builds can throw off the accuracy of code coverage
    }

    if (config.isBuildTests) {
        // Different OS have different commands to get number of cpus
        if (config.os == UBUNTU) {
            cmakeArgs['-DNOISEPAGE_TEST_PARALLELISM'] = config.os == UBUNTU ? "\$(nproc)" : 1
        }

        if (config.isJumboTest) {
            cmakeArgs['-DNOISEPAGE_USE_JUMBOTESTS'] = ENABLED
        }
    } else {
        cmakeArgs['-DNOISEPAGE_BUILD_TESTS'] = DISABLED
    }

    if (config.isBuildBenchmarks || config.isBuildSelfDrivingE2ETests) {
        cmakeArgs['-DNOISEPAGE_BUILD_BENCHMARKS'] = ENABLED
    }

    if (config.isBuildSelfDrivingE2ETests) {
        cmakeArgs['-DNOISEPAGE_BUILD_SELF_DRIVING_E2E_TESTS'] = ENABLED
    }

    String compileCmd = 'cmake -GNinja'
    cmakeArgs.each { arg, value -> compileCmd += " $arg=$value" }
    compileCmd += ' ..'
    return compileCmd
}

/*
generateBuildCmd creates the build command string based on the config passed
in. The config arguments are the same as the defaultArgs in noisePageBuild.
*/
String generateBuildCmd(Map config = [:]) {
    String buildCmd = 'ninja'
    if (config.isBuildSelfDrivingE2ETests) {
        buildCmd += ' execution_runners noisepage'
    }
    else if (!config.isBuildBenchmarks && !config.isBuildTests) {
        buildCmd += ' noisepage'
    }
    return buildCmd
}

/**
 * generateBuildScript creates the full script string, including the commands to
 * create the directory. It even allows us to wrap the compile command in a timer
 * if we want to time how long the build takes. This time will be output to a
 * file.
 */
String generateBuildScript(String compileCmd, String buildCmd, Boolean isRecordTime) {
    String script = '''
    mkdir -p build
    cd build
    '''
    if (isRecordTime) {
        script += """
        /usr/bin/time -o /tmp/noisepage-compiletime.txt -f %e sh -c \"$compileCmd
        $buildCmd\""""
    } else {
        script += """
        $compileCmd
        $buildCmd"""
    }
    return script
}

/**
 * cppCoverage will collect the c++ code coverage information in the build directory, remove unrelated files from the
 * coverage data, and report the coverage to codecov to show on GitHub.
 */
void cppCoverage() {
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
    ''', label: 'Clean up c++ code coverage and report'
}

return this
