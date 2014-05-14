#!/bin/bash


GLUSTER_VOLUME=$1

# Make sure /mnt was correctly setup before running this script

if [ `hostname` == "master" ]; then
    for node in `cat /etc/hosts | awk '{print $1}'`; do 
	gluster peer probe "$node"
    done
    
      sudo mkdir -pv /mnt/$GLUSTER_VOLUME

         # ssh "node001" "sudo mkdir -p /mnt/gv2"   # if you want to add this storage to the volume
      sudo gluster volume create $GLUSTER_VOLUME master:/mnt/$GLUSTER_VOLUME
    
	# sudo gluster volume create $GLUSTER_VOLUME master:/mnt/$GLUSTER_VOLUME node001:/mnt/$GLUSTER_VOLUME # if you want to add master:/$GLUSTER_VOLUME AND node001:/$GLUSTER_VOLUME
      
      sleep 1
      sudo gluster volume start  $GLUSTER_VOLUME
fi


# Do this here to save some time for setting up glusterfs in the master node
if [ `hostname` == "master" ]; then
    for node in `cat /etc/hosts | awk '{print $1}'`; do
         ssh "$node" "mkdir -pv /gluster/$GLUSTER_VOLUME && sudo mount -t glusterfs master:/$GLUSTER_VOLUME /gluster/$GLUSTER_VOLUME"
         ssh "$node" "chown -R ubuntu:ubuntu /gluster/$GLUSTER_VOLUME"

         echo $node
         df -h | grep scratch
    done
fi
