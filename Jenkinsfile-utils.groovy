
def noisePageBuild(Map args = [:]){
    def defaultArgs = [
        useCache: true,
        buildType: "Debug",
        os: "ubuntu",
        isBuildTests: true,
        useASAN: false,
        isBuildBenchmarks: false,
        isCodeCoverage: false,
        isJumboTest: false
    ]
    def Map config = defaultArgs << args
    def String compile_cmd = "cmake -GNinja -DNoisePage_UNITY_BUILD=ON -DCMAKE_BUILD_TYPE=$config.buildType"
    
    if(config.isCodeCoverage){
        compile_cmd += " -DNOISEPAGE_GENERATE_COVERAGE=ON"
    }

    if(config.useCache){
        compile_cmd += " -DCMAKE_CXX_COMPILER_LAUNCHER=ccache"
    }

    if(config.os=='ubuntu' && config.isBuildTests){
        compile_cmd += " -DNOISEPAGE_TEST_PARALLELISM=\$(nproc)"
    }

    if(config.useASAN){
        compile_cmd += " -DNOISEPAGE_USE_ASAN=ON"
    }

    if(!config.isBuildBenchmarks){
        compile_cmd += " -DNOISEPAGE_BUILD_BENCHMARKS=OFF"
    }

    if(config.buildType=='Release' && !config.isBuildTests){
        compile_cmd += " -DNOISEPAGE_USE_JEMALLOC=ON"
    }

    if(config.isJumboTest){
        compile_cmd += " -DNOISEPAGE_USE_JUMBOTESTS=ON"
    }

    compile_cmd += " .."

    def String build_cmd = "ninja"
    if(!config.isBuildBenchmarks && !config.isBuildTests){
        build_cmd += " noisepage"
    }

    sh script: compile_cmd, label: 'Compiling'
    sh script: build_cmd, label: 'Building'
}
return this