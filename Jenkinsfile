pipeline {
    agent none
    options {
        buildDiscarder(logRotator(daysToKeepStr: '30'))
    }
    stages {
        stage('Build') {
            parallel {
                stage('ubuntu-18.04/gcc-7.3.0 (Release/benchmark)') {
                    agent { label 'benchmark' }
                    steps {
                        sh 'echo $NODE_NAME'
                        sh 'echo y | sudo ./script/installation/packages.sh'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DTERRIER_USE_ASAN=OFF -DTERRIER_USE_JEMALLOC=ON .. && make -j$(nproc) all'
                        // sh 'cd build && timeout 1h make runbenchmark'
                        sh 'cd script/micro_bench && timeout 1h ./run_micro_bench.py --run'
                        archiveArtifacts 'script/micro_bench/*.json'
                        junit 'script/micro_bench/*.xml'
                    }
                }
            }
        }
    }
}
