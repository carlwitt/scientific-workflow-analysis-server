import requests
import json

import numpy as np

import csv

import argparse
import sys

ip   = "192.168.24.74"
#ip   = "127.0.0.1"
port = "8080"
path = "/submit"

count = 0

def geneventsall(header, row):
	"""
	Constructs a start and a stop event for every entry in the csv file containing all the information
	"""

	global count

	events = [{},{}]

	events[0]["timestamp"] = float(row[header.index("mainjob_started")])
	events[1]["timestamp"] = float(row[header.index("mainjob_started")]) + float(row[header.index("total_time_s")])

	for e in events:

		e["msg_type"] = "invoc"
		
		e["data"]              = {}
		e["data"]["host_name"] = row[header.index("host_name")]
#		e["data"]["id"]        = row[header.index("run")]
		e["data"]["id"]        = count
		e["data"]["lam_name"]  = row[header.index("transformation")]

		e["session"]           = {}
		e["session"]["id"]     = row[header.index("run")]
		e["session"]["tstart"] = 0

		for i in range(header.index("input_file_sum_kb"), len(header)):

			e[header[i]] = row[i]

		e["vsn"] = "cf2.0"

	events[0]["data"]["status"] = "started"
	events[1]["data"]["status"] = "ok"

	events[1]["data"]["info"]           = {}
	events[1]["data"]["info"]["tdur"]   = float(row[header.index("total_time_s")])*1000
	events[1]["data"]["info"]["tstart"] = float(row[header.index("mainjob_started")])

	return events

def main(argv):

	global count

	parser = argparse.ArgumentParser()
	parser.add_argument("-p", "--path", default="../data.csv", type=str, help="Path to csv file to read from (default: ../data.csv)")
	args = parser.parse_args()
	
	reader = csv.reader(open(args.path))
	header = next(reader, None)

	for row in reader:

		events = geneventsall(header, row)
		count += 1

		print(events[0])
		print(events[1])

		requests.post("http://" + ip + ":" + port + path, data=json.dumps(events[0]))
		requests.post("http://" + ip + ":" + port + path, data=json.dumps(events[1]))

if __name__ == '__main__':
	main(sys.argv)