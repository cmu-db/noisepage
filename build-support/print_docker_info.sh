#!/bin/bash

# The hash of the docker image.
# On a regular system, no clue.
IMG_NAME=$(cat /proc/self/cgroup | grep memory | cut -d/ -f 3)
# The time that the docker image was brought up.
# On a regular system, the time the system started.
IMG_TIME_CREATION=$(date --date="@$(stat -c '%Y' /proc/1/cmdline)" +'%F %T %z')
# The current time.
IMG_TIME_NOW=$(date +'%F %T %z')
# The current system information.
IMG_TOP=$(top -n 1 -b)

echo "Image: ${IMG_NAME}"
echo "Image creation time : ${IMG_TIME_CREATION}"
echo "Image current time  : ${IMG_TIME_NOW}"
echo "Image current top   : ${IMG_TOP}"