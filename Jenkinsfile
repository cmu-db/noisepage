// Common build functions will be loaded into the "utils" object in every stage.
// The build functions are loaded with Groovy's elvis operator ?:, where
//      a ?: b means if (a) { return a; } else { return b; }
// and in this case, utils = utils ?: load(utilsFileName) means "if utils is truthy, return utils, else, load utils".
// This has to be done in every stage to support the Jenkins "restart from stage" feature.
def utils
String utilsFileName  = 'Jenkinsfile-utils.groovy'

pipeline {
    agent none
    options {
        buildDiscarder(logRotator(daysToKeepStr: '30'))
        parallelsAlwaysFailFast()
    }
    stages {
        stage('Ready For CI') {
            agent       { docker { image 'noisepage:focal' } }
            when        { not { branch 'master' } }
            steps       { script { utils = utils ?: load(utilsFileName) ; utils.stageGithub() } }
            post        { cleanup { deleteDir() } }
        }
        stage('Check') {
            parallel {
                stage('ubuntu-20.04/gcc-9.3 (Debug/format/lint/censored)') {
                    agent       { docker { image 'noisepage:focal' } }
                    steps       { script { utils = utils ?: load(utilsFileName) ; utils.stageCheck() } }
                    post        { cleanup { deleteDir() } }
                }
                stage('ubuntu-20.04/clang-8.0 (Debug/format/lint/censored)') {
                    agent       { docker { image 'noisepage:focal' } }
                    environment { CC="/usr/bin/clang-8" ; CXX="/usr/bin/clang++-8" }
                    steps       { script { utils = utils ?: load(utilsFileName) ; utils.stageCheck() } }
                    post        { cleanup { deleteDir() } }
                }
            }
        }

        stage('Build-only checks') {
            parallel {
                stage('Benchmarks (debug build only)') {
                    agent       { docker { image 'noisepage:focal' ; args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache' } }
                    steps       { script { utils = utils ?: load(utilsFileName) ; utils.stageBuildDefault([
                        buildCommand: 'ninja',
                        cmake: '-DCMAKE_BUILD_TYPE=Debug -DNOISEPAGE_BUILD_BENCHMARKS=ON',
                    ] ) } }
                    post        { cleanup { deleteDir() } }
                }
                stage('Logging disabled (release build only)') {
                    agent       { docker { image 'noisepage:focal' ; args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache' } }
                    steps       { script { utils = utils ?: load(utilsFileName) ; utils.stageBuildDefault([
                        buildCommand: 'ninja',
                        cmake: '-DCMAKE_BUILD_TYPE=Release -DNOISEPAGE_BUILD_BENCHMARKS=ON -DNOISEPAGE_BUILD_TESTS=ON -NOISEPAGE_BUILD_SELF_DRIVING_E2E_TESTS=ON -DNOISEPAGE_USE_LOGGING=OFF'
                    ] ) } }
                    post        { cleanup { deleteDir() } }
                }
            }
        }

        stage('Self-Driving') {
            parallel {
                stage('Workload Forecasting'){
                    agent       { docker { image 'noisepage:focal' ; args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache' } }
                    steps       { script { utils = utils ?: load(utilsFileName) ; utils.stageForecasting() } }
                    post        { cleanup { deleteDir() } }
                }
                stage('Modeling'){
                    agent       { docker { image 'noisepage:focal' ; label 'dgb' ; args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache' } }
                    environment { CODECOV_TOKEN=credentials('codecov-token') }
                    steps       { script { utils = utils ?: load(utilsFileName) ; utils.stageModeling() } }
                    post        { always { script { utils = utils ?: load(utilsFileName) ; utils.stageArchive() } } ; cleanup { deleteDir() } }
                }
            }
        }
    }
}
