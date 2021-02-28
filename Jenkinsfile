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

                        sh script: '''
                        cd build
                        timeout 30m ninja self_driving_e2e_test
                        ''', label: 'Running self-driving end-to-end test'

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
    }
}
