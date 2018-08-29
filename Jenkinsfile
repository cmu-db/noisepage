pipeline {
    agent none
    stages {
        stage('Build') {
            parallel {

                stage('Ubuntu Bionic/gcc-7.3.0/llvm-6.0.0 (Debug/ASAN)') {
                    agent {
                        docker { 
                            image 'ubuntu:bionic'
                            args '--cap-add sys_ptrace'
                        }
                    }
                    steps {
                        sh 'echo y | sudo ./script/installation/packages.sh'
                        sh 'sudo apt-get install -q -y curl'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DTERRIER_USE_ASAN=ON .. && make -j4'
                        sh 'cd build && make unittest -j4'
                    }
                }

                stage('Ubuntu Bionic/gcc-7.3.0/llvm-6.0.0 (Debug/Coverage)') {
                    agent {
                        docker { 
                            image 'ubuntu:bionic'
                            args '--cap-add sys_ptrace'
                        }
                    }
                    steps {
                        sh 'echo y | sudo ./script/installation/packages.sh'
                        sh 'sudo apt-get install -q -y curl'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DTERRIER_GENERATE_COVERAGE=ON .. && make -j4'
                        sh 'cd build && make unittest -j4'
                    }
                }

                stage('Ubuntu Bionic/gcc-7.3.0/llvm-6.0.0 (Release/unittest)') {
                    agent {
                        docker { 
                            image 'ubuntu:bionic'
                        }
                    }
                    steps {
                        sh 'echo y | sudo ./script/installation/packages.sh'
                        sh 'sudo apt-get install -q -y curl'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_WARNING_LEVEL=Production .. && make -j4'
                        sh 'cd build && make unittest -j4'
                    }
                }

                stage('Ubuntu Bionic/gcc-7.3.0/llvm-6.0.0 (Release/benchmark)') {
                    agent {
                        docker { 
                            image 'ubuntu:bionic'
                        }
                    }
                    steps {
                        sh 'echo y | sudo ./script/installation/packages.sh'
                        sh 'sudo apt-get install -q -y curl'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_WARNING_LEVEL=Production .. && make -j4'
                        sh 'cd build && make runbenchmark -j4'
                        sh 'cd script/micro_bench && ./run_micro_bench.py'
                    }
                }
            }
        }
    }
}
