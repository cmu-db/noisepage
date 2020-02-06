pipeline {
    agent none
    options {
        buildDiscarder(logRotator(daysToKeepStr: '30'))
        parallelsAlwaysFailFast()
    }
    stages {
        stage('Benchmark') {
            agent { label 'benchmark' }
            steps {
                sh 'echo $NODE_NAME'
                sh 'echo y | sudo ./script/installation/packages.sh'
                sh 'mkdir build'
                sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DTERRIER_USE_ASAN=OFF -DTERRIER_USE_JEMALLOC=ON .. && make -j$(nproc) all'
                // We want to use a ramdisk on Jenkins to avoid the overhead of disk writes.
                sh 'cd script/micro_bench && timeout 1h ./run_micro_bench.py --run --num-threads=4 --logfile-path=/mnt/ramdisk/benchmark.log'
                archiveArtifacts 'script/micro_bench/*.json'
                junit 'script/micro_bench/*.xml'
            }
            post {
                cleanup {
                    deleteDir()
                }
            }
        }
    }
}
