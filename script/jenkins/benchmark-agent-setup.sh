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
useradd -m -s/bin/bash -c jenkins jenkins && echo 'jenkins ALL=(ALL)       NOPASSWD: ALL' > /etc/sudoers.d/jenkins
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
mkfs.ext4 /dev/sda4
echo "$(blkid /dev/sda4|awk '{print $2}') /jenkins               ext4    errors=remount-ro 0       2" >> /etc/fstab
mkdir /jenkins
mount /jenkins
chown jenkins:jenkins /jenkins

# create the ramdisk
echo "tmpfs  /mnt/ramdisk  tmpfs  nodev,nosuid,noexec,nodiratime,size=20g 0 0" >> /etc/fstab
mkdir /mnt/ramdisk
mount /mnt/ramdisk
chmod 1777 /mnt/ramdisk

# setup nat
route delete default
route add default gw 10.92.0.4
route add -net 128.2.0.0/16 gw 10.92.0.1

# now that everything is in place, start the jenkins service
systemctl start jenkins
