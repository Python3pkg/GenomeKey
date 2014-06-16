#!/bin/bash

# make sure order of the tasks found in the stdout files is the same
diff <(grep -l "Moving files to Storage"  */*/jobinfo/cosmos_id*stdout) <(grep -l "Moving done"  */*/jobinfo/cosmos_id*stdout)

# get timestamps in seconds
paste <(grep "Moving files to Storage"  */*/jobinfo/cosmos_id*stdout|cut -d'/' -f 1) <(grep -h "Moving files to Storage"  */*/jobinfo/cosmos_id*stdout|sed 's/Moving files to Storage//g'|date +"%s" -f -) <(grep -h "Moving done"  */*/jobinfo/cosmos_id*stdout|sed 's/Moving done//g'|date +"%s" -f -) 

