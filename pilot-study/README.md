#Pilot Study Cluster Setup

##1 Setup the cluster##

TODO fill this out:
* plugins, config, ports...
* fix equals sign in cosmos config

##2 Run init-glusterfs.sh##

* Copy to the master node and run the new version ```init-glusterfs.sh``` (also in this directory):

* Where the gluster volume is named gv here and there's only one worker node.

* Run ```df -h``` on all compute nodes on order to check if the gluster volume was mounted correctly.


##3 Edit the .cosmos/config file##

* Copy the file ```cosmos-pilot.config``` (in this directory) to ```~/.cosmos/config``` on the master node.

* This config file should already contain the fixes for the gluster volume name to match the new init-glusterfs.sh configuration and should look like:

```
default_root_output_dir=/gluster/gv
working_directory=/mnt
```

##4 Setup AWS CLI##

* The AWS cli should be configured in order to be able to copy and backup the files from and to S3.

* Run and copy in the Access Key ID and the Secret Access Key, choose us-east-1 as default zone and table as default output format.

```
$ aws configure
> AWS Access Key ID: *************123ABC
> AWS Secret Access Key: *************xxx0232
> Default region name: us-east-1
> Default output forma: table
```
