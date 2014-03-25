#!/usr/bin/env python
# simple script to read in csv and output new CSV with random number of bams
# also, optionally extracts pedigree information for each sample

import csv, random, sys, os.path

input_file = sys.argv[1]
basename = os.path.basename(input_file)

# default to 100
if len(sys.argv) > 2:
	number_bams_to_keep = int(sys.argv[2])
else:
	number_bams_to_keep = 100

# default to 100
if len(sys.argv) > 3:
	pedigree_file = sys.argv[3]
else:
	pedigree_file = False

number_bams_to_keep_string = str(number_bams_to_keep)

output_file_random = number_bams_to_keep_string + '_' + basename + '_random_bams.index'
output_file_random_md5sum = number_bams_to_keep_string + '_' + basename + '_random_bams.md5'
output_pedigree_filename = number_bams_to_keep_string + '_' + basename + '_g1k.ped'

# read in CSV and save mapped bams to list
with open(input_file, 'rb') as f:
	reader = csv.reader(f, delimiter='\t')
	#reader = csv.reader(f)
	new_rows_list = []
	for row in reader:
		if 'mapped' in row[0] and not 'unmapped' in row[0]:
			new_rows_list.append(row[0])

if pedigree_file:
	pedigree_dict = {}
	with open(pedigree_file, 'rb') as f:
		reader = csv.reader(f, delimiter='\t')
		headers = next(reader, None)  # returns the headers or `None` if the input is empty
		for row in reader:
			# ID in second column
			pedigree_dict[row[1]] = row
	output_pedigree = open(output_pedigree_filename, 'w')
	output_pedigree.write("\t".join(headers))
	output_pedigree.write("\n")
else:
	pedigree_list = None

# write out text file with random number of mapped bams
with open(output_file_random, 'wb') as f:
	random_list = random.sample(new_rows_list, number_bams_to_keep)
	for bam in random_list:
		if output_pedigree:
			# look up info on sample
			sample_id = bam.split('/')[1]
			# write back out to file
			output_pedigree.write("\t".join(pedigree_dict[sample_id]))
			output_pedigree.write("\n")
		f.write("%s\n" % bam)

if output_pedigree:
	output_pedigree.close()


