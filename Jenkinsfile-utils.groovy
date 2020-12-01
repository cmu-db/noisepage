
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
    def String compileCmd = generateCompileCmd(config)
    def String buildCmd = generateBuildCmd(config)

    sh script:"""
    mkdir build
    cd build
    $compileCmd
    $buildCmd """, label: "Compile & Build"
}

def generateCompileCmd(Map config = [:]) {
    def String compileCmd = "cmake -GNinja -DCMAKE_BUILD_TYPE=$config.buildType"
    
    if(config.isCodeCoverage){
        compileCmd += " -DNOISEPAGE_GENERATE_COVERAGE=ON"
    }else{
        compileCmd += " -DNOISEPAGE_UNITY_BUILD=ON"
    }

    if(config.os != "macos" && config.useCache){
        compileCmd += " -DCMAKE_CXX_COMPILER_LAUNCHER=ccache"
    }

    if(config.os == "ubuntu" && config.isBuildTests){
        compileCmd += " -DNOISEPAGE_TEST_PARALLELISM=\$(nproc)"
    }

    if(config.useASAN){
        compileCmd += " -DNOISEPAGE_USE_ASAN=ON"
    }

    if(!config.isBuildBenchmarks){
        compileCmd += " -DNOISEPAGE_BUILD_BENCHMARKS=OFF"
    }

    if(!config.isBuildTests){
        compileCmd += " -DNOISEPAGE_BUILD_TESTS=OFF"
    }

    if(config.buildType=="Release" && !config.isBuildTests){
        compileCmd += " -DNOISEPAGE_USE_JEMALLOC=ON"
    }

    if(config.isJumboTest){
        compileCmd += " -DNOISEPAGE_USE_JUMBOTESTS=ON"
    }

    compileCmd += " .."
    return compileCmd
}

def generateBuildCmd(Map config = [:]){
    def String buildCmd = "ninja"
    if(!config.isBuildBenchmarks && !config.isBuildTests){
        buildCmd += " noisepage"
    }
    return buildCmd
}


return this