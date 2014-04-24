#Pilot Study Cluster Setup

##1- Setup the cluster##

- The cluster should be launched using sge and sge_plus plugins (needs StarClusterExtenitions installed for sge_plus). 

- The ports 22 (ssh), and 80 (or any other port that you decide to use with the webinterface should be open).

- The user must be "ubuntu" since it's required by the AMI.

- Use the AMI **ami-5bd1c832** both for the master node and the worker nodes.

- Launch the cluster using: 

```
$ starcluster start -c pilot pilot # the first pilot is the configuration you specidied in the starcluster config file and the second is a name you choose to give to the cluster.
```

Once the setup is done you can access the masternode using: 

```
$ starcluster sshmaster pilot
```

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
default_root_output_dir=/gluster/gv
working_directory=/mnt
```

(These fixes should already be contained within the ```pilot-cosmos.config.ini``` file)

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

#Testing Method#
###**1- Configure starcluster**

Login to the master node

###**2- Setup glusterFS on the master and worker nodes**

Make sure the volume is correctly mounted into all the worker nodes. The easiest way to do it is to write a test file from the master node to the gluster volume and check if the file is readable on all the nodes.  In theory this should be a matter of running this script from the ```pilot-study``` directory:

```
cd ~/GenomeKey/pilot-study
./init-glusterfs.sh
```

However, the commands may need to be run manually.

###**3- Pull the latest GenomeKey from repo**

```
cd GenomeKey
git pull
````

This is currently required to get latest changes in pilot-study directory that have been made since the creation of the AMI

	
###**4- Modify the ~/.cosmos/config file**

The default cosmos configuration file has a problem with the automation script, it needs replace all the " = " with "=".
This is required by the shell script in order to be able to read the variables values from this file.  The version of the config has this fix and so it should be copied into place.

```
cd GenomeKey/pilot-study
cp pilot-cosmos.config.ini /home/ubuntu/.cosmos/config.ini
```

###**5- Install Cosmos and GenomeKey**

Run ```pip install .``` in Cosmos and GenomeKey directories respectively. The full path method seems to not work everytime, in addition the installation updates the packages installed on the virtual environment(django)--make sure you're doing that in the virtual environment (```workon cosmos```).

###**6- Configure aws CLI**

Make sure you have access to the pilot project s3 bucket.

###**7- Systematic testing**

#####*7-a- test with 3 tiny bams:*

FIXME: Genomekey fails with only one tiny bam

#####*7-b- test with 3 tiny bams with -di activated:*

Testing the ```-di``` (delete intermediate) option to check if genomekey keeps the needed files: aligned bam(s) and the gVCFs.

#####*7-c- test CosmosReset.sh:*
Currently the lines that send the emails are commented-out as outgoing mail is blocked on AWS.

#####*7-d- test automation.sh*
