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

    }
}
