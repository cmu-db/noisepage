// Common build functions will be loaded into the "utils" object in the Ready For CI stage.
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

        stage('Microbenchmark (Build only)') {
            agent       { docker { image 'noisepage:focal' ; args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache' } }
            steps       { script { utils = utils ?: load(utilsFileName) ; utils.stageMicrobenchmark() } }
            post        { cleanup { deleteDir() } }
        }

        stage('Test') {
            parallel {
                stage('ubuntu-20.04/gcc-9.3 (Debug/ASAN/jumbotests)') {
                    agent       { docker { image 'noisepage:focal' ; args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache' } }
                    steps       { script { utils = utils ?: load(utilsFileName) ; utils.stageTest(true, [cmake:
                        [NOISEPAGE_BUILD_TYPE:'Debug', NOISEPAGE_BUILD_TESTS:'ON', NOISEPAGE_USE_ASAN:'ON', NOISEPAGE_USE_JUMBOTESTS:'ON']]
                    ) } }
                    post        { always { script { utils = utils ?: load(utilsFileName) ; utils.stageArchive() } } ; cleanup { deleteDir() } }
                }

                stage('ubuntu-20.04/gcc-9.3 (Debug/Coverage/unittest)') {
                    agent       { docker { image 'noisepage:focal' ; label 'dgb' ; args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache' } }
                    environment { CODECOV_TOKEN=credentials('codecov-token') }
                    steps       { script { utils = utils ?: load(utilsFileName) ; utils.stageTest(false, [cmake:
                        [NOISEPAGE_BUILD_TYPE:'Debug', NOISEPAGE_BUILD_TESTS:'ON', NOISEPAGE_GENERATE_COVERAGE:'ON']]
                    ) } }
                    post        { always { script { utils = utils ?: load(utilsFileName) ; utils.stageArchive() } } ; cleanup { deleteDir() } }
                }

                stage('ubuntu-20.04/clang-8.0 (Debug/ASAN/jumbotests)') {
                    agent       { docker { image 'noisepage:focal' ; args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache' } }
                    environment { CC="/usr/bin/clang-8" ; CXX="/usr/bin/clang++-8" }
                    steps       { script { utils = utils ?: load(utilsFileName) ; utils.stageTest(false, [cmake:
                        [NOISEPAGE_BUILD_TYPE:'Debug', NOISEPAGE_BUILD_TESTS:'ON', NOISEPAGE_USE_ASAN:'ON', NOISEPAGE_USE_JUMBOTESTS:'ON']]
                    ) } }
                    post        { always { script { utils = utils ?: load(utilsFileName) ; utils.stageArchive() } } ; cleanup { deleteDir() } }
                }

                stage('ubuntu-20.04/gcc-9.3 (Release/jumbotests)') {
                    agent       { docker { image 'noisepage:focal' ; args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache' } }
                    steps       { script { utils = utils ?: load(utilsFileName) ; utils.stageTest(false, [cmake:
                        [NOISEPAGE_BUILD_TYPE:'Release', NOISEPAGE_BUILD_TESTS:'ON', NOISEPAGE_USE_JEMALLOC:'ON', NOISEPAGE_USE_JUMBOTESTS:'ON']]
                    ) } }
                    post        { always { script { utils = utils ?: load(utilsFileName) ; utils.stageArchive() } } ; cleanup { deleteDir() } }
                }

                stage('ubuntu-20.04/clang-8.0 (Release/jumbotests)') {
                    agent       { docker { image 'noisepage:focal' ; args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache' } }
                    environment { CC="/usr/bin/clang-8" ; CXX="/usr/bin/clang++-8" }
                    steps       { script { utils = utils ?: load(utilsFileName) ; utils.stageTest(false, [cmake:
                        [NOISEPAGE_BUILD_TYPE:"Release", NOISEPAGE_BUILD_TESTS:'ON', NOISEPAGE_USE_JEMALLOC:'ON', NOISEPAGE_USE_JUMBOTESTS:'ON']]
                    ) } }
                    post        { always { script { utils = utils ?: load(utilsFileName) ; utils.stageArchive() } } ; cleanup { deleteDir() } }
                }
            }
        }

        stage('End-to-End') {
            parallel {
                stage('Debug') {
                    agent       { docker { image 'noisepage:focal' ; args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache' } }
                    steps       { script { utils = utils ?: load(utilsFileName) ; utils.stageOltpbenchDebug() } }
                    post        { cleanup { deleteDir() } }
                }
                stage('Performance') {
                    agent       { label 'benchmark' }
                    environment { PSS_CREATOR = credentials('pss-creator') /* Performance Storage Service (Django) auth credentials. Can only be changed from Jenkins webpage. */ }
                    steps       { script { utils = utils ?: load(utilsFileName) ; utils.stageOltpbenchRelease() } }
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
