#!/bin/bash

################################
# Run as root on the master node

GLUSTER_VOLUME=$1

# Make sure /mnt was correctly setup before running this script

if [ `hostname` == "master" ]; then
    for node in `cat /etc/hosts | awk '{print $1}'`; do 
	gluster peer probe "$node"
    done

    for node in `cat /etc/hosts | awk '{print $1}'`; do
        gluster peer probe "$node"
    done

    
      mkdir -pv /mnt/${GLUSTER_VOLUME}1

         # ssh "node001" "sudo mkdir -p /mnt/gv2"   # if you want to add this storage to the volume
      gluster volume create $GLUSTER_VOLUME master:/mnt/${GLUSTER_VOLUME}1
    
	# sudo gluster volume create $GLUSTER_VOLUME master:/mnt/$GLUSTER_VOLUME node001:/mnt/$GLUSTER_VOLUME # if you want to add master:/$GLUSTER_VOLUME AND node001:/$GLUSTER_VOLUME
      
      sleep 20
      gluster volume start  $GLUSTER_VOLUME
fi


# Do this here to save some time for setting up glusterfs in the master node
for node in `cat /etc/hosts | awk '{print $1}'`; do
         ssh "$node" "mkdir -pv /gluster/$GLUSTER_VOLUME && sudo mount -t glusterfs master:/$GLUSTER_VOLUME /gluster/$GLUSTER_VOLUME"
         ssh "$node" "chown -R ubuntu:ubuntu /gluster/$GLUSTER_VOLUME"

         echo $node
         ssh "$node" "df -h | grep ${GLUSTER_VOLUME}"
done
