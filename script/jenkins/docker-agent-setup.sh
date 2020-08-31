#!/bin/sh -e
# one-time setup script for ensuring that jenkins can run on Linux
# workers meant to start Docker containers
#
# it is meant to be run on the host that will start
# containers, not within the containers themselves!!!
#
# NOTE: there are some hardcoded assumptions that it will be run in
# the PDL Narwhal Emulab environment...
#

# you probably want to do something like this beforehand:
# apt-get update && apt-get -y dist-upgrade && apt-get clean && apt-get -y --purge autoremove

# java is required for the swarm agent
apt-get update && apt-get -y install default-jdk-headless

# add the jenkins user and put the swarm.sh script in place
groupadd -g 99 jenkins
useradd -m -s/bin/bash -u 99 -g jenkins -c jenkins jenkins && echo 'jenkins ALL=(ALL)       NOPASSWD: ALL' > /etc/sudoers.d/jenkins
install -m 755 -o jenkins -g jenkins /proj/CMUDB-CI/data/swarm.sh ~jenkins

# add the jenkins service and enable it
cat > /etc/systemd/system/jenkins.service << __EOF__
[Unit]
Description=Jenkins
After=network.target

[Service]
User=jenkins
Restart=always
Type=simple
ExecStart=/home/jenkins/swarm.sh -executors $(nproc) -labels "docker" -disableClientsUniqueId

[Install]
WantedBy=multi-user.target
__EOF__
systemctl daemon-reload
systemctl enable jenkins

# setup the working directory for jenkins
/share/testbed/bin/linux-fixpart all
/share/testbed/bin/linux-localfs -d /dev/sda4 -t ext4 /jenkins
chown jenkins:jenkins /jenkins

# setup the working directory for docker
/share/testbed/bin/linux-localfs -d /dev/sdb1 -t ext4 /var/lib/docker

# setup the ccache cache_dir
install -d -m 1777 /jenkins/ccache
echo 'max_size = 250G' > /jenkins/ccache/ccache.conf

# install docker community edition from upstream
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
apt-get update && apt-get -y install docker-ce docker-ce-cli containerd.io

# get the latest bionic image for jenkins
docker image load -i /proj/CMUDB-CI/data/ubuntu-bionic-jenkins-latest.tar

# make sure the jenkins user can run docker
usermod -a -G docker jenkins

# setup nat
route delete default
route add default gw 10.111.0.4
route add -net 128.2.0.0/16 gw 10.111.0.1

# now that everything is in place, start the jenkins service
systemctl start jenkins
