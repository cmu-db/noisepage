#!/bin/sh
#
# manage Jenkins macOS worker VMs running under VMware Fusion
#
# the idea is to halt the worker vm after doing the build, at which
# point it is restarted from a known state
#
# this script belongs on the VMware Fusion HOST and should be invoked
# from the user's crontab like so:
# @reboot /Users/cmudb/vmware_startup.sh > /dev/null &
#

PATH=$PATH:/Applications/VMware\ Fusion.app/Contents/Public
template="/Users/cmudb/Documents/VMs/jenkins-mac-worker.vmwarevm/jenkins-mac-worker.vmx"
# we are only permitted to run two copies of macOS in VMs per section
# 2.B.iii of the macOS EULA:
# https://www.apple.com/legal/sla/docs/macOS1014.pdf
workers="worker1 worker2"

while : ; do
  running_vms=$(vmrun -T fusion list)
  for worker in ${workers}; do
    if [ -z "$(echo ${running_vms}|grep ${worker})" ]; then
      vmpath="/Users/cmudb/Documents/VMs/jenkins-mac-${worker}.vmwarevm/jenkins-mac-${worker}.vmx"
      vmrun -T fusion deletevm ${vmpath}   # ok to fail
      vmrun -T fusion clone ${template} ${vmpath} linked -snapshot=clean -cloneName=jenkins-mac-${worker}
      vmrun -T fusion start ${vmpath} nogui
    fi
  done
  sleep 2
done
