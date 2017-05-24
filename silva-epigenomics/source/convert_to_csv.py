"""
Extract the invocation information from the xml files of a run and write them into to one csv file per task type (e.g., sol2sanger, map)
Uses the extract_invocation_data script for finding the task types, input and output files, and invocation metrics.

TODO: currently, one CSV file is generated per task type per run. 
A single CSV file for all invocations of all task types of all runs of all run groups would be much better.

"""

__author__ = 'Carl Witt'
__email__ = 'wittcarl@deneb.uberspace.de'

import extract_invocation_data as base
import rafael_epigenomics_data as data
import csv

import os

''' ====================================================================================================================
Main Program
====================================================================================================================='''

# taq = list(filter(lambda w: w[0]=="taq", working_dirs))

def write_csv(invocations, job_information, out_file, null_string ="NA"):
	"""
	Write the invocation information (runtime, memory use, output size) for the given invocations (of a single task type)
	:param invocations: the ids (keys of the job_information dictionary) of the invocations to consider
	:param job_information: the output of parse_invocation_metrics
	:param out_file: destination
	:param null_string: the string to use when information is not available
	"""

	# assume every invocation of a task of a certain type takes the same number of input files
	num_input_files  = len(job_information[invocations[0]]['input_files'])
	#file_attributes  = ["input_file_%s_kb"%i for i in range(1, num_input_files + 1)]
	file_attributes  = ["host_name", "input_file_sum_kb"]
	usage_attributes = ['utime', 'stime', 'maxrss', 'nvcsw', 'nivcsw', 'nswap', 'minflt', ] # 'majflt', 'inblock', 'outblock', 'nsignals', 'msgsnd', 'msgrcv', 'nswap'
	load_attributes  = ["min1", "min5", "min15"]
	procs_attributes = ["total", "running", "sleeping", "waiting", "vmsize", "rss"]
	task_attributes  = ["total", "running", "sleeping", "waiting",]
	ram_attributes   = ["total", "free", "shared", "buffer",]
	swap_attributes  = ["total", "free",]
	machine_attributes_headers = load_attributes + list(map(lambda a: "procs_"+a, procs_attributes)) \
	 											 + list(map(lambda a: "task_"+a, task_attributes)) \
	 											 + list(map(lambda a: "ram_"+a, ram_attributes)) \
	 											 + list(map(lambda a: "swap_"+a, swap_attributes))

	# the csv column labels
	header = ['run_goup', 'run', 'transformation', 'mainjob_started'] + file_attributes + usage_attributes + machine_attributes_headers + ['out_size_kb', 'total_time_s', 'peak_memory_kb']
#	header = ['workflow','transformation', 'mainjob_started'] + file_attributes + usage_attributes + machine_attributes_headers + ['out_size_kb', 'total_time_s', 'peak_memory_kb']

	with open(out_file, 'w', newline='') as csvfile:

		spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
		spamwriter.writerow(header)

		for job_info in [job_information[job_id] for job_id in invocations]:

			file_sizes     = [float(file['size']) for file in job_info['input_files']]
			usage_values   = [float(job_info['usage'][attr]) for attr in usage_attributes]
#
			try:
				out_size   = sum([float(file['size']) for file in job_info['output_files']])
			except KeyError as k:
				out_size   = null_string
#
			peak_mem       = float(job_info['usage']['maxrss'])
			machine_values = []

			for machine_attrs, attrs in [("load", load_attributes), ("procs", procs_attributes), ("task", task_attributes), ("ram", ram_attributes), ("swap", swap_attributes)]:
				for attr in attrs:
					try:
						machine_values.append(job_info[machine_attrs][attr])
					except KeyError:
						machine_values.append(null_string)

#			data = [job_info["workflow"], job_info["transformation"], job_info['mainjob_started_ts']] + file_sizes + usage_values + machine_values + [out_size, job_info['total_time'], peak_mem]
			data = [job_information["run_group"], job_information["run"], job_info["transformation"], job_info['mainjob_started_ts']] + [job_info['host_name']] + [sum(file_sizes)] + usage_values + machine_values + [out_size, job_info['total_time'], peak_mem]
#			data = [job_info["transformation"], job_info['mainjob_started_ts']] + file_sizes + usage_values + machine_values + [out_size, job_info['total_time'], peak_mem]
			spamwriter.writerow(data)

def write_csvs():
	"""

	:return:
	"""
	for wf_class, working_dir in data.working_dirs:

		transformations = base.find_transformations_stampede('%s/genome-dax-0.stampede.db' % working_dir, main_jobs_only=True)

		wf_id    = working_dir.split("/")[-1]
		dax_file = "genome.dax"  # is the same for all of rafaels epigenomics workflows

		for transformation in transformations:

			print("working_dir: {0}".format(working_dir))

			# parse the input and output files for each task
			job_file_map = base.parse_input_output_files(base.abs_path(working_dir, dax_file))

			# get the XML file names of the invocations of a given task
			invocation_files = base.find_invocation_files_stampede(working_dir, transformation)

			# extract the invocation and file information from the XML files and update the job_file_map with it
			jobs = []
			for invocation_file in invocation_files:
				job_id = base.parse_invocation_metrics(base.abs_path(working_dir, invocation_file), job_file_map)
				jobs.append(job_id)

#->			# Add information about run goup and run to job info
			job_file_map["run_group"] = working_dir.split("/")[2]
			job_file_map["run"]       = wf_id

			# write the data to csv
			transformation_short = transformation.split(":")[-2]
			output_file = "../derived_data/%s_%s_%s.csv" % (wf_class, wf_id, transformation_short)
			write_csv(jobs, job_file_map, output_file)

def merge(header, h):

	"""
	Used to merge multiple headers.
	Assumes that header is the the header where eventually all information is stored.
	"""

	ret = []

	header_itr = 0
	h_itr      = 0

	while (header_itr <= len(header) and h_itr <= len(h)):

		if header_itr == len(header) and not h_itr == len(h):

			ret.append(h[h_itr])
			h_itr += 1
			continue

		if h_itr == len(h) and not header_itr == len(header):

			ret.append(header[header_itr])
			header_itr += 1
			continue

		if h_itr == len(h) and header_itr == len(header):
			break

		# If the same element occurs in both, add it and continue
		if header[header_itr] == h[h_itr]:

			ret.append(header[header_itr])
			header_itr += 1
			h_itr += 1
			continue

		# If there is an element in header, that is not in h
		# Move through header until an element is found, that is somewhere in h or no element are left in header
		if not header_itr == len(header)-1 and not header[header_itr] in h:

			ret.append(header[header_itr])
			header_itr += 1
			continue

		# Continue moving through h
		if not h_itr == len(h)-1:

			ret.append(h[h_itr])
			h_itr += 1
			continue
	return ret

def join_csvs(out_file, null_string ="NA"):
	
	header = []
	data   = []

	# Construct the common header for all csv files
	for file in os.listdir("../derived_data/"):

		# If not csv file, ignore
		if file[-4:] != ".csv":
				continue

		reader = csv.reader(open("../derived_data/" + file))

		# Read the header and merge with existing header
		h      = next(reader, None)
		header = merge(header, h)

	# Iterate over all csv files and extract the data
	for file in os.listdir("../derived_data/"):

		# If not csv file, ignore
		if file[-4:] != ".csv":
				continue

		reader = csv.reader(open("../derived_data/" + file))

		# Get header information fpr this file, to fill out missing gaps for the global data with null string 
		h      = next(reader, None)

		for row in reader:

			d = []

			header_itr = 0
			h_itr      = 0

			if not len(h) == len(row):
				print(file)

			while (header_itr < len(header) and h_itr < len(h)):
			
				if h_itr == len(h):

					d.append(null_string)
					header_itr += 1
					continue

				if header[header_itr] == h[h_itr]:

					d.append(row[h_itr])
					header_itr += 1
					h_itr += 1

				else:

					d.append(null_string)
					header_itr += 1

			data.append(d)

	with open(out_file, 'w', newline='') as csvfile:

		spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
		spamwriter.writerow(header)

		for row in data:
			spamwriter.writerow(row)

if __name__ == '__main__':
	write_csvs()
	join_csvs("../data.csv")