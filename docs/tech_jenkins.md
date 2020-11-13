# Jenkins

Jenkins operation is driven by `Jenkinsfile` in the repository.  In order to create (or re-create) the project on the Jenkins server, the following initial steps must be performed:

1. Login to your GitHub account and then use it to login to the Jenkins server.

1. Select [New Pipeline](http://jenkins.db.cs.cmu.edu:8080/blue/organizations/jenkins/create-pipeline) in the Blue Ocean interface and answer the following:

   1. Select "**GitHub**" for "Where do you store your code?"

   1. Select "**cmu-db**" for "Which organization does the repository belong to?"

   1. Select the "**terrier**" repository and "**Create Pipeline**"

1. Once the pipeline is created, navigate to the [project page](http://jenkins.db.cs.cmu.edu:8080/job/terrier/), and select "**Configure**" from the left sidebar and adjust the following:

* **Branch Sources**
   * **GitHub**
      * **Behaviors** -> **Add**
         * **Discover pull requests from forks**
            * **Strategy**: Merging the pull request with the current target branch revision
            * **Trust**: Contributors
         * **Discover pull requests from origin**
            * **Strategy**: Merging the pull request with the current target branch revision

4. Click "**Save**" at the bottom of the page.

## Preparing new nodes to host Jenkins workers
Our Jenkins environment uses the [Self-Organizing Swarm Modules](https://plugins.jenkins.io/swarm) plugin which makes it very easy to bootstrap new Jenkins worker nodes, whether physical or virtual machines.  The process for the various environments is as follows:

1. a script for configuring Linux systems that will host Docker containers can be found in `script/jenkins/docker-agent-setup.sh` in the repository.  Note that it contains some assumptions about the environment, including the location of the prepared Docker image described below.

1. a script for configuring Linux systems running builds on bare metal for benchmarking purposes can be found in `script/jenkins/bench-agent-setup.sh` in the repository.  Note that it also contains some assumptions about the environment that it will run in.

Preparing macOS hosts to run Jenkins agent virtual machines is a bit more of a manually intensive process and involves:
1. installation of VMware Fusion on the host
1. creation of a template image that will be used to instantiate actual worker VMs, including installation of:
   1. Command Line Tools for Xcode
   1. the `swarm.sh` script (`script/jenkins/swarm.sh` in the repository)
1. installation of VM supervisor script (`script/jenkins/vmware_startup.sh` in the repository) on the host(s) that will run the worker VMs


## Preparing Docker images to run as Jenkins workers
The following steps (required, unless specified otherwise) describe the preparation of images from the Docker hub as build workers since the default images are so minimal:

1. pull and run an image of the type you want to configure, e.g.,
   ```
   docker run -it --name jenkins-prep ubuntu:bionic
   ```
1. install any prerequisite packages that are needed for `script/installation/packages.sh` to run, e.g.
   ```
   apt-get -qq update && apt-get -qq -y --no-install-recommends install python-dev lsb-release sudo tzdata && apt-get -qq -y clean
   ```
   Some trial and error might be required to determine this list of prerequisites for a particular platform.
1. add a user and group `jenkins` and grant all `sudo` privileges
   ```
   groupadd -g 99 jenkins && useradd -m -u 99 -g 99 -c jenkins jenkins && echo 'jenkins ALL=(ALL)       NOPASSWD: ALL' > /etc/sudoers.d/jenkins
   ```
   **NOTE: the numbers specified above must exactly match the actual uid and gid of the jenkins user on the host system in order for the Docker containers to work when invoked from Jenkins**
1. *(optional)* prepopulate the image with prerequisite software packages from `packages.sh` to avoid delays fetching software or errors from unavailability of mirrors.  This can be accomplished by creating a copy of that script in the container and running it there.
1. exit the container and save your work, e.g.,
   ```
   docker commit jenkins-prep ubuntu:bionic
   ```
