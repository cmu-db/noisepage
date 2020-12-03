/*
noisePageBuild will create a build directory and compile and build the
noisepage binary in that directory. The options passed into the method
determine the compilation and build options. Refer to the defaultArgs
for the different options that can be passed in.
*/
def noisePageBuild(Map args = [:]){
    def defaultArgs = [
        useCache: true,
        buildType: "Debug",
        os: "ubuntu",
        isBuildTests: true,
        useASAN: false,
        isBuildBenchmarks: false,
        isCodeCoverage: false,
        isJumboTest: false,
        isRecordTime: false
    ]
    def Map config = defaultArgs << args
    def String compileCmd = generateCompileCmd(config)
    def String buildCmd = generateBuildCmd(config)
    def String buildScript = generateBuildScript(compileCmd, buildCmd, config.isRecordTime)

    sh script:buildScript, label: "Compile & Build"
}

/*
generateCompileCmd creates the cmake command string. It is based on the
config passed into the function. The config arguments are the same as the
defaultArgs in noisePageBuild.
*/
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

/*
generateBuildCmd creates the build command string based on the config passed
in. The config arguments are the same as the defaultArgs in noisePageBuild.
*/
def generateBuildCmd(Map config = [:]){
    def String buildCmd = "ninja"
    if(!config.isBuildBenchmarks && !config.isBuildTests){
        buildCmd += " noisepage"
    }
    return buildCmd
}

/*
generateBuildScript creates the full script string, including the commands to
create the directory. It even allows us to wrap the compile command in a timer
if we want to time how long the build takes. This time will be output to a
file.
*/
def generateBuildScript(compileCmd, buildCmd, isRecordTime){
    def String script = '''
    mkdir build
    cd build
    '''
    if(isRecordTime){
        script += """
        /usr/bin/time -o /tmp/compiletime.txt -f %e sh -c \"$compileCmd
        $buildCmd\""""
    }else{
        script += """
        $compileCmd
        $buildCmd"""
    }
    return script
}


return this