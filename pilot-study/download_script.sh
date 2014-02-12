#!/bin/bash

while read F; do
	s3cmd get 's3://1000genomes/'$F .
done < 100_random_bams.index
