"""
Extract map invocation data for workflow runs on exactly the same data and compare them.
Using the same input data, we can analyse the influence of data and background load.
We have five invocations of each (input file, map task) pair,
	where the output file should be the same everywhere (since the map task is deterministic).
	0. Validate the same output size assumption
 	1. analyze the spread of runtimes (if its small, there's not much to show? if yes, we have a background load effect)
 	2. analyze the background load spread (if its small, there's not much to show)
"""

__author__ = 'Carl Witt'
__email__ = 'wittcarl@deneb.uberspace.de'

import extract_invocation_data as base

working_dirs = [
	# ("hep", "/Users/macbookdata/Studium/PhD/Publications/bsr-poster-2016/experiment/data/hep-100000/cc/pegasus/genome-dax/20160831T122313+0000"),
	# ("hep", "/Users/macbookdata/Studium/PhD/Publications/bsr-poster-2016/experiment/data/hep-100000/cc/pegasus/genome-dax/20160831T124807+0000"),
	# ("hep", "/Users/macbookdata/Studium/PhD/Publications/bsr-poster-2016/experiment/data/hep-100000/cc/pegasus/genome-dax/20160831T134956+0000"),
	("hep", "/Users/macbookdata/Studium/PhD/Publications/bsr-poster-2016/experiment/data/hep-100000/cc/pegasus/genome-dax/20160831T144949+0000"),
	("hep", "/Users/macbookdata/Studium/PhD/Publications/bsr-poster-2016/experiment/data/hep-100000/cc/pegasus/genome-dax/20160831T155619+0000"),
	# ("ilmn", "/Users/macbookdata/Studium/PhD/Publications/bsr-poster-2016/experiment/data/ilmn-100000/cc/pegasus/genome-dax/20160831T162819+0000"),
	# ("ilmn", "/Users/macbookdata/Studium/PhD/Publications/bsr-poster-2016/experiment/data/ilmn-100000/cc/pegasus/genome-dax/20160831T165523+0000"),
	# ("ilmn", "/Users/macbookdata/Studium/PhD/Publications/bsr-poster-2016/experiment/data/ilmn-100000/cc/pegasus/genome-dax/20160831T172256+0000"),
	# ("ilmn", "/Users/macbookdata/Studium/PhD/Publications/bsr-poster-2016/experiment/data/ilmn-100000/cc/pegasus/genome-dax/20160831T175026+0000"),
	# ("ilmn", "/Users/macbookdata/Studium/PhD/Publications/bsr-poster-2016/experiment/data/ilmn-100000/cc/pegasus/genome-dax/20160831T185419+0000"),

	# ("taq", "/Users/macbookdata/Studium/PhD/Publications/bsr-poster-2016/experiment/data/taq-100000/cc/pegasus/genome-dax/20160831T042527+0000"),
	# ("taq", "/Users/macbookdata/Studium/PhD/Publications/bsr-poster-2016/experiment/data/taq-100000/cc/pegasus/genome-dax/20160831T044003+0000"),
	# ("taq", "/Users/macbookdata/Studium/PhD/Publications/bsr-poster-2016/experiment/data/taq-100000/cc/pegasus/genome-dax/20160831T050311+0000"),
	# ("taq", "/Users/macbookdata/Studium/PhD/Publications/bsr-poster-2016/experiment/data/taq-100000/cc/pegasus/genome-dax/20160831T051239+0000"),
	# ("taq", "/Users/macbookdata/Studium/PhD/Publications/bsr-poster-2016/experiment/data/taq-100000/cc/pegasus/genome-dax/20160831T051246+0000"),
]

def compare_map_tasks(transformation):

	job_file_maps = []

	print("working_dirs: {0}".format(working_dirs))
	for wf_class, working_dir in working_dirs:

		wf_id = working_dir.split("/")[-1]
		dax_file = "genome.dax"  # is the same for all of rafaels epigenomics workflows

		# parse the input and output files for each task
		job_file_map = base.parse_input_output_files(base.abs_path(working_dir, dax_file))

		# get the XML file names of the invocations of a given task
		invocation_files = base.find_invocation_files_stampede(working_dir, transformation)

		# extract the invocation and file information from the XML files and update the job_file_map with it
		jobs = []
		for invocation_file in invocation_files:
			job_id = base.parse_invocation_metrics(base.abs_path(working_dir, invocation_file), job_file_map)
			jobs.append(job_id)

		# print("jobs: {0}".format(jobs))
		# print("job_file_map: {0}".format(job_file_map))

		job_file_maps.append((jobs, job_file_map))

	return job_file_maps

def merge_join(job_file_maps):
	'''
	Look up the counterparts
	:param job_file_maps:
	:return:
	'''
	jobs, reference_map = job_file_maps[0]

	for job_id in jobs:
		ref_inputs = reference_map[job_id]['input_files']
		ref_outputs = reference_map[job_id]['output_files']
		ref_usage = reference_map[job_id]['usage']
		print("job_id: {0}".format(job_id))
		for _, job_file_map in job_file_maps[1:]:
			assert ref_inputs == job_file_map[job_id]['input_files'], 'input file mismatch %s and %s' % (ref_inputs, job_file_map[job_id]['input_files'])
			assert ref_outputs == job_file_map[job_id]['output_files'], 'output file mismatch %s and %s' % (ref_outputs, job_file_map[job_id]['output_files'])
			assert ref_usage == job_file_map[job_id]['usage'], 'usage differs\n%s\n%s' % (ref_usage, job_file_map[job_id]['usage'])

# # write the data to csv
# transformation_short = transformation.split(":")[-2]
# output_file = "../derived_data/%s_%s_%s.csv" % (wf_class, wf_id, transformation_short)
# base.write_csv(jobs, job_file_map, output_file)

# file_job_maps = compare_map_tasks("genome::map:1.0")
file_job_maps = compare_map_tasks("genome::sol2sanger:1.0")
merge_join(file_job_maps)

# {'bwrite': '261669', 'nread': '0', 'size': '261669', 'nwrite': '32', 'nseek': '0', 'bseek': '0', 'bread': '0', 'name': '080415_TAQ12_MSP37_s_3_sequence.3.nocontam.map'}
# {'bwrite': '261962', 'nread': '0', 'size': '261962', 'nwrite': '32', 'nseek': '0', 'bseek': '0', 'bread': '0', 'name': '080415_TAQ12_MSP37_s_3_sequence.3.nocontam.map'}
