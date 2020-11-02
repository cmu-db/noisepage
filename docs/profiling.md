# Profiling

**Table of Contents**

- [dev machines](#dev-machines)
- [EC2](#ec2)

## Dev Machines

TODO(WAN): write me. OLTPBench is possible to setup with sufficient ant proxy magic.

## EC2

### Choosing Instances:
- Create 2 Ubuntu 20.04 server instances on c5.9xlarge boxes
  - Specify CPU options such that you use 1 thread per core
  - Up storage to 60 GB
  - Keep General Purpose SSD
- Name one instance Terrier and name one instance OLTPBench
- Update the security group on the Terrier instance 
  - Add inbound rule allowing for custom TCP protocol on port 15721

### OLTPBench Machine Setup:
- Update the box and install packages
  - ```bash
    sudo apt update
    sudo apt-get install ant 
    sudo apt-get install postgresql-client-common
    sudo apt-get install postgresql-client-12
    ```
- Clone Matt's fork of oltpbench:
   - ```bash 
      git clone https://github.com/mbutrovich/oltpbench.git
      ```
- Checkout the noisepage branch:
   - ```bash
      git checkout noisepage
      ```
- Run setup scripts:
   - ```bash
      ant bootstrap
      ant resolve
      ant build
      ```
- Update the noisepage TPCC config file:
   - ```bash
     vim config/noisepage_tpcc_config.xml
     ``` 
   - Change `localhost` to IP address of Terrier machine in `DBUrl`
   - Choose `scalefactor`, `loaderThreads`, and `terminals`
   - Add `<warmup>10</warmup>` in `work` to start caching
   - Choose default weights or change to `100,0,0,0,0` if you want just NewOrder transactions

### Terrier Machine Setup:
- Update the box and clone Terrier
   - ```bash
     sudo apt update
     git clone https://github.com/cmu-db/terrier.git
     ```
- Update Ubuntu version in `script/installation/packages.sh`
   - Change value of `$VERSION` check from `18.04` to `20.04`
- Run `packages.sh`
  - ```bash
    sudo bash script/installation/packages.sh
    ```
- Choose query mode between `Interpret` and `Compiled`
  - If you want Compiled mode change change the line  
    ```exec_query->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);```   
    to  
    ```exec_query->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Compiled);```  
    in  
    `src/traffic_cop/traffic_cop.cpp`
- Build Terrier in `Release` mode with `Debug` symbols
  - ```bash
    mkdir RelWithDebug
    cd RelWithDebug
    cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo -DTERRIER_USE_ASAN=OFF -DTERRIER_USE_JEMALLOC=ON
    make -j18 terrier tpl
    ```
- If you wish to log to ramdisk, set up `ramdisk` for WAL file storage
  - ```bash
    sudo mkdir /mnt/ramdisk
    sudo mount -t tmpfs -o rw,size=32G tmpfs /mnt/ramdisk 
    ```
- If you wish to use [`bcc` profiling tools](https://github.com/iovisor/bcc/blob/master/INSTALL.md#ubuntu---binary) follow the installation steps described at the link

### Running Scripts:

**Terrier Machine:**
- Note: you may want to use a tmux session in case you get disconnected from the instance
- Startup Terrier (if you do not want to log to Ramdisk, remove the log_file_path flag below)  
   - ```bash
     sudo rm /mnt/ramdisk/wal.log 
     ./terrier -connection_thread_count=200 -wal_file_path=/mnt/ramdisk/wal.log
     ```
- Get debug symbols from used packages in Terrier using the RUNNING_PID_OF_TERRIER
   - ```bash
     echo "deb http://ddebs.ubuntu.com $(lsb_release -cs) main restricted universe multiverse
     deb http://ddebs.ubuntu.com $(lsb_release -cs)-updates main restricted universe multiverse
     deb http://ddebs.ubuntu.com $(lsb_release -cs)-proposed main restricted universe multiverse" | \
     sudo tee -a /etc/apt/sources.list.d/ddebs.list
     sudo apt install ubuntu-dbgsym-keyring
     sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys F2EDC64DC5AEE1F6B9C621F0C8CAB6595FDFF622
     sudo apt-get update
     sudo apt install debian-goodies
     find-dbgsym-packages $(pidof terrier)
     ```
- Get debug symbols from remaining packages by taking last line of `OUTPUT` from previous command and doing:
  - ```bash
    sudo apt-get install **[OUTPUT]**
    ```
**OLTPBench Machine:**
- Load tables
   - ```bash
     ant execute -Dconfig=./config/noisepage_tpcc_config.xml -Dbenchmark=tpcc -Dexecute=false -Dload=true -Dcreate=true -Dextra="-histograms"
     ```
**Terrier Machine:**
- Do one of the following (NOTE: **perf** is the preferred method as it provides accurate symbols)
  - Attach perf profiler: ``/usr/bin/perf record --freq=997 --call-graph dwarf -q -p `pgrep terrier` ``
  - Attach bcc profiler: ``sudo profile-bpfcc -adf -F 99 -p `pgrep terrier` > ~/tpcc.profile``
  - Profile offcputime with bcc: ``sudo offcputime-bpfcc -df -p `pgrep terrier` > ~/tpcc.offcpu``

**OLTPBench Machine:**
- Run workload
   - ```bash
     ant execute -Dconfig=./config/noisepage_tpcc_config.xml -Dbenchmark=tpcc -Dexecute=true -Dload=false -Dcreate=false -Dextra="-histograms"
     ```

### Visualizing Output:

Choose a path based on the tool used

**PERF:**
- In the directory where you recorded the profiling results on the **Terrier Machine**, produce a perf report
  - `perf script > out.perf`
- Clone the FlameGraph git repo and navigate to the directory
  - `git clone https://github.com/brendangregg/FlameGraph.git`
- Copy the output file to the FlameGraph directory and navigate to it
  - `sudo cp out.perf ~/FlameGraph/`
  - `cd ~/FlameGraph`
- Fold the stack samples into single lines
  - `sudo ./stackcollapse-perf.pl out.perf > out.folded`
- Produce a FlameGraph SVG
  - `./flamegraph.pl out.folded > kernel.svg`
- Copy the SVG to your **local machine** and open it in a browser window

**BCC:**
- Copy over the file produced from your command above to your local machine
- Open [speedscope.app](speedscope.app) and choose the output file


### Important Notes:
- If you stop and restart the Terrier instance, you will have to delete and recreate the ramdisk storage you created:
  - ```bash
    sudo rm -rf /mnt/ramdisk
    sudo mkdir /mnt/ramdisk
    sudo mount -t tmpfs -o rw,size=32G tmpfs /mnt/ramdisk 
    ```
- Make sure to at least stop your instances after you are done profiling to avoid wasting credits
