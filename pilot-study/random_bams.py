#!/usr/bin/env python
# simple script to read in csv and output new CSV with random number of bams

import csv, random, sys

number_bams_to_keep = 100
number_bams_to_keep_string = str(number_bams_to_keep)

input_file = sys.argv[1]
output_file_random = number_bams_to_keep_string + '_' + input_file + '_random_bams.index'
output_file_random_md5sum = number_bams_to_keep_string + '_' + input_file + '_random_bams.md5'

# read in CSV and save mapped bams to list
with open(input_file, 'rb') as f:
	reader = csv.reader(f, delimiter='\t')
	#reader = csv.reader(f)
	new_rows_list = []
	for row in reader:
		if 'mapped' in row[0] and not 'unmapped' in row[0]:
			new_rows_list.append(row[0])
			#new_rows_list.append(row[0])

# # write out CSV with only mapped bams
# with open(output_file_bams, 'wb') as f:
# 	writer = csv.writer(f)
# 	writer.writerows(new_rows_list)

# # write out CSV with random number of mapped bams
# with open(output_file_random, 'wb') as f:
# 	random_list = random.sample(new_rows_list, number_bams_to_keep)
# 	writer = csv.writer(f, delimiter='\t')
# 	#writer = csv.writer(f)
# 	writer.writerows(random_list)

# write out text file with random number of mapped bams
with open(output_file_random, 'wb') as f:
	random_list = random.sample(new_rows_list, number_bams_to_keep)
	for bam in random_list:
		f.write("%s\n" % bam)




