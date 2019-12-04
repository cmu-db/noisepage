#!/bin/sh
#
# trying to halt the macos worker directly from the jenkins pipeline
# doesn't work the way we want so this small stupid hack allows the
# jenkins stage to complete before we kill the vm
#
# this goes in the macos worker vm: /Users/jenkins/halt.sh, mode 755
#

(sleep 3; pkill -lf swarm-client; sudo /sbin/halt -qn) &
true
