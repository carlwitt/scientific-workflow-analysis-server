import argparse
import sys

import requests
import json

import numpy as np

import time

ip   = "192.168.24.74"
#ip   = "127.0.0.1"
port = "8080"
path = "/submit"

# Some Macros
TIME = 0
TID  = 1
NAME = 2
TYPE = 3

def gen_v2_event():
	return {
		"timestamp": 78349534523,
		"msg_type" : "invoc",
		"data" : {
			"host_name" : "b5aa573c-d81a-6fb4-8196-c18e4e64a12c",
			"id" : 16,
			"lam_name" : "tabix",
			"status" : "started" },
		"session" : {
			"id" : "762043859895",
			"tstart" : 1467978701568.0 },
		"vsn" : "cf2.0"
	}

def gen_start_event(invoc_id, task_name, timestamp, session_id, session_tstart):
	return {
		"event": "invoc_start",
		"timestamp": timestamp,
		"invoc_id": invoc_id,
		"session" : {
        	"id" : str(session_id),
        	"tstart" : session_tstart },
		"task_type": str(task_name),
		"input_files": [{ "name": "chr21.BS.bfa", "size_byte": '46944392'}],
		"machine": {"host_name": "b5aa573c-d81a-6fb4-8196-c18e4e64a12c"},
		"workflow_run": {"user": "bjoern", "id": "762043859895"},
		"vsn": "cf3.0"
	}

def gen_stop_event(invoc_id, task_name, timestamp, session_id, session_tstart):
	return {
		"event": "invoc_stop",
		"timestamp": timestamp,
		"invoc_id": invoc_id,
		"exitcode": "ok",
		"session" : {
        	"id" : str(session_id),
        	"tstart" : session_tstart },
        "task_type": str(task_name),
		"output_files": [{"name": "HEP2_MSP1_Digests_s_4_sequence.14.nocontam.map", "size_byte": "984994"}],
		"resource_usage": {
			"utime": "106.864", 
			"stime": "0.225", 
			"maxrss": "194312", 
			"minflt": "46609", 
			"majflt": "0", 
			"inblock": "0", 
			"outblock": "1984", 
			"nswap": "0", 
			"nvcsw": "24", 
			"nivcsw": "164"},
		"vsn": "cf3.0"
	}

def generate_schedule(task_types, task_count, avg_exec_time, var_exec_time, duration):

	# Draw the type of the tasks from a uniform distribution in [0, task_types)
	tasks_type      = np.random.randint(0, task_types, task_count)
	# Draw the execution times from a normal distribution N(avg, sqrt(var))
	tasks_exec_time = np.random.normal(avg_exec_time, np.sqrt(var_exec_time), task_count)

	# Check that the execution times are valid (0 <= t <= duration)
	tasks_exec_time[tasks_exec_time > duration] = duration
	tasks_exec_time[tasks_exec_time < 0.1]      = 0.1

	# Create an empty schedule as an array of tuples [timestamp, task_id, task_type, (start, stop)]
	schedule = np.empty((0, 4))

	for i in range(task_count):

		# Draw the starting time of a task from a uniform distribution in [0, duration-exec_time)
		start_time = np.random.uniform(0, duration - tasks_exec_time[i])
		end_time   = start_time + tasks_exec_time[i]

		# Add a start and a stop event to the schedule
		schedule = np.vstack((schedule, [start_time, i, tasks_type[i], 0]))
		schedule = np.vstack((schedule, [end_time, i, tasks_type[i], 1]))

	# Sort the schedule by the timestemps
	schedule = schedule[schedule[:,0].argsort()]

	return schedule

def run_schedule(schedule):

	session_id = 7
#	session_id     = np.random.randint(0, 100000000000)
	session_tstart = time.clock()
	
	# Go through all events in the schedule
	for i in range(schedule.shape[0]):
		
		t = schedule[i]

		# Wait until next event "happens"
		if (i > 0):
			time.sleep(t[TIME] - schedule[i-1][TIME])
		else:
			time.sleep(t[TIME])

		# Check if it's a start or stop event
		if (t[TYPE] == 0):
			# Send a start event
			send_message(gen_start_event(int(t[TID]), int(t[NAME]), t[TIME], int(session_id), session_tstart))
		else:
			# Send a stop event
			send_message(gen_stop_event(int(t[TID]), int(t[NAME]), t[TIME], session_id, session_tstart))

def send_message(payload):
	#headers = {"Content-type": "application/x-www-form-urlencoded"}
	requests.post("http://" + ip + ":" + port + path, data=json.dumps(payload))
	#requests.post("http://" + ip + ":" + port + path, data=payload)

def main(argv):

	parser = argparse.ArgumentParser()
	parser.add_argument("-t", "--types", default=5, type=int, help="number of different tasks (default: 5)")
	parser.add_argument("-n", "--count", default=100, type=int, help="number of tasks to be generated (default: 100)")
	parser.add_argument("-e", "--time", default=30, type=float, help="average execution time of one task [sec] (default: 30)")
	parser.add_argument("-v", "--variance", default=10, type=float, help="variance of the execution time [sec] (default: 10)")
	parser.add_argument("-d", "--duration", default=120, type=float, help="duration of generation process [sec] (default: 120)")
	args = parser.parse_args()

	# Generate a schedule and execute it
#	s = generate_schedule(args.types, args.count, args.time, args.variance, args.duration)
#	run_schedule(s)
	
	requests.post("http://" + ip + ":" + port + path, data=json.dumps(gen_v2_event()))

	# -----------------------------------------------------------------------------------------------------------------------
	# Stuff for testing

#	send_message(payload = "{'key1': 'value1', 'key2': 'value2'}")
#	send_message(payload = gen_start_event(1, "test", 1))
#	send_message(payload = gen_probe_event(1))
#	send_message(payload = gen_stop_event(1, 1)

#	send_message('{ "msg_type": "machine_probe", "timestamp": 78349534523, "host_name": "b5aa573c-d81a-6fb4-8196-c18e4e64a12c", "load_average": {"min1": "8.33", "min5": "3.94", "min15": "6.44"}, "procs": {"total": "906", "running": "35", "sleeping": "870", "waiting": "1", "vmsize": "16227048", "rss": "6763072"}, "vsn": "cf3.0" }')

	# -----------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
	main(sys.argv)