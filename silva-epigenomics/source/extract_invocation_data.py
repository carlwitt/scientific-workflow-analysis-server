"""
	Collects information about task invocations in a pegasus workflow execution
		- order and size of the input and output files (if possible, order is derived from the arguments vector)
		- task runtime
		- peak memory
		- background load (load averages and number of processes)

	Uses the stampede sqlite database delivered with each run
		- for Silva's epigenomics data, this file is always named genome-dax-0.stampede.db
		- contains the DAG and basic information about the workflow run, e.g., the task types #find_transformations_stampede
		- the detailed invocation information is only in the files
			- see #find_invocation_files_stampede to relate the two
			- see #parse_invocation_metrics for extracting the information from a single invocation xml file (as generated by their kickstart tool)
	Uses the dax file of a workflow to identify input and output files of an invocation, see #parse_input_output_files

	The information collection process proceeds in two steps, this is not optimal, but simple and safe
	1. find the ordered input/output files of each job from the DAX file (the relationship is not clear from the .out files)
	2. look into the invocation descriptions (.out files) and find the file sizes and program behavior metrics
		- out_size_kb: sum over output sizes (as a proxy for disk write)

	Note on job identification:
		the derivation attribute of the invocation element in an .out file seems to be the job abs_id, identical to the id attribute of the job element in the dax file

	Note on XPath / ElementTree / Pegasus Schema: Always specifying the namespace is important, e.g., just search for a mainjob element won't match, needs to be {http://pegasus.isi.edu/schema/invocation}mainjob
		xpath ordered input data files: job/argument/filename@file
		xpath for output data files: job[@name = target]/uses[@link = output and @type = data]
		xpath for memory and duration: invocation/mainjob/usage@<various attributes> (also: page faults, context switches, etc.)

"""
__author__ = 'Carl Witt'
__email__ = 'wittcarl@deneb.uberspace.de'

import xml.etree.ElementTree
import dateutil.parser
import sqlite3


''' ====================================================================================================================
Function Definitions
====================================================================================================================='''

def parse_input_output_files(dax_filename):
	"""
	Retrieve the input/output files of an invocation.
	Detailed information about the files and the invocation metrics (execution time, peak memory, etc.) have to be looked up later in the .out files
	:param dax_filename: the abstract DAG file to
	:return: dictionary where the key is a job unique identifier - should match the invocation@derivation attribute and the value is again a dictionary containing input and output files
		{'filterContams_080415_TAQ12_MSP37_s_4_sequence_5': {'input': ['080415_TAQ12_MSP37_s_4_sequence.5.sfq'], 'output': ['080415_TAQ12_MSP37_s_4_sequence.5.nocontam.sfq']}}
	"""
	adag = xml.etree.ElementTree.parse(dax_filename).getroot()
	job_selector = "{http://pegasus.isi.edu/schema/DAX}job" # filtering for a transformation name can be done later [@name='%s']" % transformation_name
	filename_args_selector = "{http://pegasus.isi.edu/schema/DAX}argument/{http://pegasus.isi.edu/schema/DAX}filename"
	inputfile_selector = "{http://pegasus.isi.edu/schema/DAX}uses[@link='input']"
	outputfile_selector = "{http://pegasus.isi.edu/schema/DAX}uses[@link='output']"

	invocation_file_usage = {}

	for job in adag.findall(job_selector):

		job_id = job.get("id")

		# filenames as string
		inputfiles = []
		outputfiles = []

		# extract the filename arguments of the program to determine which file is assigned to which input or output slot
		fileargs = []
		for filearg in job.findall(filename_args_selector):
			fileargs.append(filearg.get("file"))

		for inputfile in job.findall(inputfile_selector):
			# ignore executables
			if inputfile.get("type") == "data":
				inputfiles.append(inputfile.get("file"))

		for outputfile in job.findall(outputfile_selector):
			# ignore executables
			if outputfile.get("type") == "data":
				outputfiles.append(outputfile.get("file"))

		# sort input files according to their order in the program's argument list

		try:
			inputfiles = sorted(inputfiles, key=lambda elem: fileargs.index(elem))
		except ValueError as value_error:
			print("Could not determine input file order for job %s: %s" % (job_id, value_error))

		# sort output files according to their order in the program's argument list
		# it might occur that the filename is not part of the program's arguments (e.g., if the output file name is derived automatically from the input file name)
		try:
			outputfiles = sorted(outputfiles, key=lambda elem: fileargs.index(elem))
		except ValueError as value_error:
			if len(outputfiles) > 1:
				print("Could not determine output file order for job %s: %s (outputfiles = %s, args = %s)" % (job_id, value_error, outputfiles, fileargs))

		invocation_file_usage[job_id] = {'input_files': inputfiles, 'output_files': outputfiles}

	return invocation_file_usage


def find_invocation_files_stampede(dir, transformation):
	'''
	retrieves the names of the files that contain detailed information about invocations of given task type (here called transformation)
	uses the stampede.db SQlite database, which is not always available
	:param dir: the directory containing the SQlite database (stampede.db) and the XML files describing the invocations
	:param transformation: fully qualified transformation name e.g., "genome::map:1.0"
	:return: list of file names, relative to the given directory (in the current pegasus log format, this means just the basename of the file)
	'''
	sqlite_query = "select stdout_file from invocation, job_instance where invocation.transformation = \"%s\" and invocation.job_instance_id = job_instance.job_instance_id " % transformation

	file_names = []
	conn = sqlite3.connect('%s/genome-dax-0.stampede.db' % dir)
	for row in conn.execute(sqlite_query):
		file_names.append(row[0])
	conn.close()

	return file_names


def find_transformations_stampede(stampede_db, main_jobs_only = False):
	'''
	Retrieves the task types from the workflow run
	:param dir: The directory that contains the stampede db and log files.
	:param main_jobs_only: Whether to exclude pegasus and dagman overhead transformations such as post, cleanup, dirmanager
	:return:
	'''
	sqlite_query = ""
	if main_jobs_only:
		sqlite_query = "SELECT transformation FROM task GROUP BY transformation"
	else:
		sqlite_query = "SELECT transformation FROM invocation GROUP BY transformation"

	transformations = []
	conn = sqlite3.connect(stampede_db)
	for row in conn.execute(sqlite_query):
		transformations.append(row[0])
	conn.close()

	return transformations


def parse_invocation_metrics(file_name, job_file_map):
	'''
	Parses the XML file describing an invocation. These files contain detailed information about the resource usage of the executed program and the files involved in their execution.
	:param file_name: The file to parse
	:param job_file_map: Output of the parse_input_output_files function, this is updated to contain the additional information
	:return: the data is returned by manipulating the job_file_map parameter, which is passed by reference
	Example result:
	job_file_map[job_id]: {
		'transformation': 'genome::map:1.0',
		'mainjob_started_ts': '1472620731.519',
		'total_time': '87.411' // utime + stime
		'host_name': 'runs',
		'input_files': [
			{'bwrite': '0', 'bread': '140833176', 'nseek': '0', 'name': 'chr21.BS.bfa', 'nwrite': '0', 'bseek': '0', 'nread': '36', 'size': '46944392'},
			{'bwrite': '0', 'bread': '4299886', 'nseek': '0', 'name': 'HEP2_MSP1_Digests_s_4_sequence.14.nocontam.bfq', 'nwrite': '0', 'bseek': '0', 'nread': '526', 'size': '2149943'}],
		 'output_files': [
		 	{'bwrite': '984994', 'bread': '0', 'nseek': '0', 'name': 'HEP2_MSP1_Digests_s_4_sequence.14.nocontam.map', 'nwrite': '121', 'bseek': '0', 'nread': '0', 'size': '984994'}],
		 'usage':
		 	{'stime': '0.225', 'nsignals': '0', 'msgsnd': '0', 'msgrcv': '0', 'utime': '106.864', 'minflt': '46609', 'inblock': '0', 'outblock': '1984', 'nswap': '0', 'nvcsw': '24', 'maxrss': '194312', 'nivcsw': '164', 'majflt': '0'}},
		 'load_average':
		 	{'min1': '8.33', 'min5': '3.94', 'min15': '6.44'},
		 'procs':
    		{'total': '906', 'running': '35', 'sleeping': '870', 'waiting': '1', 'vmsize': '16227048', 'rss': '6763072'},
         'task':
         	{'total': '933', 'running': '35', 'sleeping': '897', 'waiting': '1'}

	'''
	invocation_element = xml.etree.ElementTree.parse(file_name).getroot()

	job_id = invocation_element.get("derivation")
	transformation = invocation_element.get("transformation")
	job_file_map[job_id]['transformation'] = transformation

	# extract program behavior metrics and update the input dictionary
	mainjob = '{http://pegasus.isi.edu/schema/invocation}mainjob'
	usage = '{http://pegasus.isi.edu/schema/invocation}usage'
	machine = '{http://pegasus.isi.edu/schema/invocation}machine'
	uname = '{http://pegasus.isi.edu/schema/invocation}uname'
	linux = '{http://pegasus.isi.edu/schema/invocation}linux'
	load = '{http://pegasus.isi.edu/schema/invocation}load'
	procs = '{http://pegasus.isi.edu/schema/invocation}procs'
	task = '{http://pegasus.isi.edu/schema/invocation}task'

	# wrap parse steps in lambdas for deferred evaluation (to be able to wrap them in try catch in a loop)
	parse_steps = [('usage', lambda: invocation_element.find(mainjob).find(usage).attrib),
				   ('mainjob_started_ts', lambda: dateutil.parser.parse(invocation_element.find(mainjob).get("start")).timestamp()),
				   ('procs', lambda: invocation_element.find(machine).find(linux).find(procs).attrib),
				   ('task', lambda: invocation_element.find(machine).find(linux).find(task).attrib),
				   ('load', lambda: invocation_element.find(machine).find(linux).find(load).attrib),
				   ('host_name', lambda: invocation_element.find(machine).find(uname).get("nodename")), ]
	# perform the parse steps
	for target_attribute, expression in parse_steps:
		try:
			job_file_map[job_id][target_attribute] = expression()
		except AttributeError as e:
			print("{0} for {1}".format(e, file_name))
			job_file_map[job_id][target_attribute] = "Not Parseable"

	# compute total time
	job_file_map[job_id]['total_time'] = float(job_file_map[job_id]['usage']['utime']) + float(job_file_map[job_id]['usage']['stime'])

	# get information about all files
	file_selector = './/{http://pegasus.isi.edu/schema/invocation}file'
	files = invocation_element.findall(file_selector)

	# enrich the information about input files
	input_file_information = []
	for input_file in job_file_map[job_id]['input_files']:
		for file in files:
			# the file names here are absolute paths, the file names when passed as argument are relative file names
			if file.get("name").endswith(input_file):
				extended_file_information = file.attrib
				extended_file_information['name'] = input_file
				input_file_information.append(extended_file_information)
				break

	# enrich the information output files
	output_file_information = []
	for output_file in job_file_map[job_id]['output_files']:
		for file in files:
			# the file names here are absolute paths, the file names when passed as argument are relative file names
			if file.get("name").endswith(output_file):
				extended_file_information = file.attrib
				extended_file_information['name'] = output_file
				output_file_information.append(extended_file_information)
				break

	# return values by changing the input parameter
	job_file_map[job_id]['input_files'] = input_file_information
	job_file_map[job_id]['output_files'] = output_file_information

	return job_id


def abs_path(working_dir, filename):
	return working_dir + "/" + filename