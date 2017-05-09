"""
Convert Pegasus provenance to MongoDB log entries, as required by the scientific workflow log and analysis server.
Like convert_to_csv, uses the extract_invocation_data script for finding the task types, input and output files, and invocation metrics.

TODO: This is redundant, it's probably much easier to generate the JSON from a comprehensive CSV file.
"""

__author__ = 'Carl Witt'
__email__ = 'wittcarl@deneb.uberspace.de'

import extract_invocation_data as base
import rafael_epigenomics_data as data
import json

def as_cf20(job_information, workflow_id, jobs = None):
	'''
	Write the job information for the given jobs in cuneiform 2.0 JSON format to the given destination file.
	:param job_information: the job file map with execution metrics (output of parse_invocation_metrics)
	:param jobs: the ids (keys of the job_information dictionary) of the invocations to consider
	:param out_file: destination
	'''

	# if no selection specified, output all jobs.
	if jobs is None: jobs = list(job_information.keys())

	jsonarray = []
	invocation_counter = 0

	for job_info in [job_information[job_id] for job_id in jobs]:

		# Log Format 3.0, not ready yet
		# invocation_start = {
		# 	"msg_type": "invoc_start",
		# 	"timestamp": 0, # the time when the log entry was generated
		# 	"invocation_id": invocation_counter, # invocation id, can be used to correlate "start" and "stop" log entries
		# 	"lam_name": job_info["transformation"], # the name of the task
		# 	"input_files": job_info['input_files'],
		# 	"machine":{
		# 		"host_name": job_info['host_name']}, # identifier of the machine of the invocation
		# 	"workflow_id": workflow_id, # a unique random string
		# 	"vsn": "cf3.0"  #the cuneiform log schema version
		# }
		# invocation_stop = invocation_start.copy()
		# invocation_stop.pop('lam_name')
		# invocation_stop.pop('input_files')

		try:
			invocation_start = {
				"timestamp": job_info['mainjob_started_ts'], # the time when the log entry was generated
				"msg_type" : "invoc", # determines the contents of the data field
				"data" : {
					"host_name" : job_info['host_name'], # identifier of the machine of the invocation
					"id" : invocation_counter, # invocation id, can be used to correlate "started" and "ok" log entries
					"lam_name" : job_info['transformation'], # the name of the task
					"status" : "started" # the state of the invocation, can be "started", "error" or "ok"
				},
				"session" : {
					"id" : workflow_id, # a unique random string
					"tstart" : 0 # unix timestamp of the session start (milliseconds)
				},
				"vsn" : "cf2.0"  #the cuneiform log schema version
			}
			invocation_stop = invocation_start.copy()
			invocation_stop['data'] = invocation_start['data'].copy()
			invocation_stop['data']['status'] = "ok"
			invocation_stop['timestamp'] = float(invocation_start['timestamp']) + job_info['total_time']
			invocation_stop['data']['info'] = {"tdur": job_info['total_time']*1000, "tstart": job_info['mainjob_started_ts'] }

			jsonarray.append(invocation_start)
			jsonarray.append(invocation_stop)

		except KeyError as e:
			print("e: {0}".format(e))
			print("job_info: {0}".format(job_info))

	return jsonarray


def write_json(working_dirs, dax_file = "genome.dax"):
	"""
    Generates a json array of log entries, one for the start and one for the end of each invocation.
    :param dax_file
	"""
	log_entries = []
	for wf_class, working_dir in working_dirs:

		# find all transformations in the workflow
		transformations = base.find_transformations_stampede('%s/genome-dax-0.stampede.db' % working_dir, main_jobs_only=True)

		# use the directory name to distinguish between several runs of the same workflow, e.g. 20160831T122313+0000
		wf_id = working_dir.split("/")[-1]
		jobs = []

		# parse the input and output files for each task
		job_file_map = base.parse_input_output_files(base.abs_path(working_dir, dax_file))

		for transformation in transformations:

			# get the XML file names of the invocations of a given task
			invocation_files = base.find_invocation_files_stampede(working_dir, transformation)

			# extract the invocation and file information from the XML files and update the job_file_map with it
			# record every job's id for which this was done
			for invocation_file in invocation_files:
				job_id = base.parse_invocation_metrics(base.abs_path(working_dir, invocation_file), job_file_map)
				jobs.append(job_id)

		# write the data to json
		log_entries.extend(as_cf20(job_file_map, wf_id, jobs))

	output_file = "../derived_data/rafael.json"
	with open(output_file, 'w', newline='') as textfile:
		textfile.write(json.dumps(log_entries))


write_json(data.working_dirs)
