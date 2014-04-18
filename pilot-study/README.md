#Pilot Study Cluster Setup

##1- Setup the cluster##

plugins, config, ports...
fix equals sign in cosmos config

##2- Run init-glusterfs.sh##

Copy and run the new version:

```
#!/bin/bash

# Make sure /mnt was correctly setup before running this script

GLUSTER_VOLUME=gv

if [ `hostname` == "master" ]; then
   	for node in `cat /etc/hosts | awk '{print $1}'`; do 
	gluster peer probe "$node"
 	done
    	
    sudo mkdir -p /mnt/gv1
    # ssh "node001" "sudo mkdir -p /mnt/gv2"   # if you want to add this storage to the volume
    	
    sudo gluster volume create ${GLUSTER_VOLUME} master:/mnt/gv1
   	
   	# sudo gluster volume create ${GLUSTER_VOLUME} master:/mnt/gv1 node001:/mnt/gv2 # if you want to add master:/gv0 AND node001:/gv1
    	
    sleep 1
    sudo gluster volume start  ${GLUSTER_VOLUME}
	
fi
	
	# Do this here to save some time
	
for setting up glusterfs in the master node for i in master $(printf "node%03d " {1..1}); 
	do
    	ssh "$i" "sudo mkdir -p /gluster/${GLUSTER_VOLUME}
    	sudo mount -t glusterfs master:/${GLUSTER_VOLUME} /gluster/${GLUSTER_VOLUME}"
    	ssh "$i" "sudo chown -R ubuntu:ubuntu /gluster/${GLUSTER_VOLUME}"
	done

```

Where the gluster volume is named gv here and there's only one worker node.

Run  on all compute nodes on order to check if the gluster volume was mounted correctly.

```
df -h
```



##3- Edit the .cosmos/config file##
Fix the gluster volume name to match the new init-glusterfs.sh configuration 

Should look like:

```
default_root_output_dir = /gluster/gv

working_directory = /mnt

```

##4- Setup AWS CLI##

The AWS cli should be configured in order to be able to copy and backup the files from and to S3.

Run and copy in the Access Key ID and the Secret Access Key, choose us-east-1 as default zone and table as default output format.

```
$ aws configure
> AWS Access Key ID: *************123ABC
> AWS Secret Access Key: *************xxx0232
> Default region name: us-east-1
> Default output forma: table

```


