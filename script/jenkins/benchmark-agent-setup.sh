#!/bin/sh -e
# one-time setup script for making sure that jenkins can run on the
# bare metal Jenkins workers that will run the benchmark stage of a
# pipeline
#
# NOTE: there are some hardcoded assumptions about things like paths,
# device names, and mount points!!!
#

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
ExecStart=/home/jenkins/swarm.sh -executors 1 -labels "benchmark" -disableClientsUniqueId

[Install]
WantedBy=multi-user.target
__EOF__
systemctl daemon-reload
systemctl enable jenkins

# setup the working directory for jenkins
/share/testbed/bin/linux-fixpart all
/share/testbed/bin/linux-localfs -d /dev/sda4 -t ext4 /jenkins
chown jenkins:jenkins /jenkins

# setup the ccache cache_dir
install -d -m 1777 /jenkins/ccache
echo 'max_size = 250G' > /jenkins/ccache/ccache.conf
ln -s /jenkins/ccache /home/jenkins/.ccache

# create the ramdisk
echo "tmpfs  /mnt/ramdisk  tmpfs  nodev,nosuid,noexec,nodiratime,size=20g 0 0" >> /etc/fstab
mkdir /mnt/ramdisk
mount /mnt/ramdisk
chmod 1777 /mnt/ramdisk

# setup nat
route delete default
route add default gw 10.111.0.4
route add -net 128.2.0.0/16 gw 10.111.0.1

# now that everything is in place, start the jenkins service
systemctl start jenkins
