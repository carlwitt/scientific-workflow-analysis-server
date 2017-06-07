from collections import OrderedDict
from datetime import time, datetime
from random import random

import numpy as np
from bokeh.charts import Bar
from bokeh.charts import Scatter
from bokeh.layouts import row, column
from bokeh.models import ColumnDataSource, Slider, Div, Dropdown, SingleIntervalTicker, AdaptiveTicker, HoverTool, \
	TableColumn, DataTable, Range1d
from bokeh.models.axes import LinearAxis
from bokeh.models.layouts import WidgetBox
from bokeh.plotting import curdoc, figure
from pandas import DataFrame
from pymongo import MongoClient

from bokeh.models.widgets import CheckboxGroup, Button

#"#202020"
COLORS = ["#505050", "#FF0000", "#FFD700", "#808000", "#7CFC00", "#2E8B57", "#00CED1", "#000080", "#9932CC", 
			"#D2691E", "#a6cee3", "#1f78b4", "#b2df8a", "#33a02c", "#fb9a99", "#e31a1c", "#fdbf6f", "#ff7f00", "#cab2d6",
			"#6a3d9a", "#ffff99", "#b15928", "#696969"]

Paired12 = ['#a6cee3', '#1f78b4', '#b2df8a', '#33a02c', '#fb9a99', '#e31a1c', '#fdbf6f', '#ff7f00', '#cab2d6',
			'#6a3d9a', '#ffff99', '#b15928']

"""
For storage in the select run dropdown menu, the values are prefixed (using strings that parse as int leads to an error in the widget)
"""
def add_prefix(run_str):
	return "s_" + str(run_str)
def rem_prefix(run_str):
	return run_str[2:] if run_str.startswith("s_") else run_str

""" Used to generate the labels in the run dropdown box """
def run_format(run_id, numMessages, timestamp):
	isoFormat = datetime.fromtimestamp(timestamp/1000.0).isoformat(" ")
	return "%s id:%s (%s message%s)" % (isoFormat, run_id, numMessages, "s" if int(numMessages) > 1 else "")
""" Used to generate the title in the active tasks plot """
def run_format_short(run_id, timestamp):
	isoFormat = datetime.fromtimestamp(timestamp/1000.0).isoformat(" ")
	return "run %s, started %s" % (run_id, isoFormat)

""" Used to generate the contents in the infobox divs that display the number of messages, etc. """
def infoBoxFormat(label, value):
	# margin-left: 13px;
	return """
			<div style="margin-right: 13px; padding: 20px; border: 1px solid rgb(204, 204, 204); border-radius: 9px; text-align: center; height: 50px; font-size: 19px; background-color: rgb(244, 244, 244);">
				%s<br/>
				%s
			</div>""" % (label, value)

""" Generates the markup to display the task types names and colors """
def legendFormat(task_types):
	legendMarkup = "<div> <h3> Task Types</h3>" #style='margin-top: 10px'
	for name, props in task_types.iteritems():
		legendMarkup += "<span style='background-color: %s; width:22px; height: 15px; display: inline-block;'></span> <span style='margin-right:10px'>%s</span>\n" % (props['color'], name)
	legendMarkup += "</div>"
	return legendMarkup

'''
Retrieves a list of all runs with start time, number of messages and run id.
Returns and ordered dictionary that allows to access the run information by run id but maintains the chronological order (newest first) of the runs.
This is used to populate the run select dropdown menu.
'''
def query_runs():

	global datasource

	pipeline = [
		{"$match": { "data.host_name": "runs"}},
		{"$group": {"_id"           : "$session.id",
					"numLogEntries" : {"$sum": 1},
					"tstart"        : {"$first": "$session.tstart"},
					}},
		{"$sort": {"tstart": -1}}
	]

	documents = list(datasource.aggregate(pipeline))
	run_map   = OrderedDict()

	for run in documents:

		run_map[run['_id']] = dict(numLogEntries=run['numLogEntries'], tstart=run['tstart'])

	return run_map

def query_events(run_id):
	
	global datasource
	global attributes
	global attributes_ord

	global task_types

	global general_info

	global checkbox_group_p

	# The current order of tasks 
	current_order = OrderedDict()
	# The last time an event has been observed
	last_time     = -1
	# The data for the session 
	session_data  = {"xss" : [], "yss" : [], "colors" : [], "tasktype" : [], "running_tasks" : []}

	# Pipline to query for run events
	events_pipeline = [
		{"$match": {"session.id": run_id}},
		{"$sort": {"timestamp": 1}},
	]

	# Query for events
	l = list(datasource.aggregate(events_pipeline))

	# Number of attributes to parse
	numAttr = len(attributes)

	# Array to collect all the points to draw for the attributes and for the tasks
	darray = np.empty((0,numAttr), float)
	tarray = np.empty((0,2), float)

	current_running = []

	# Go through all events in l
	for doc in l:

		name = doc["data"]["lam_name"].split("::")[1].split(":")[0]

		# Create some new patches
		if last_time >= 0:

			y = 0

			for task in current_order:

				count = current_order[task]

				session_data["xss"].append([last_time, last_time, doc["timestamp"], doc["timestamp"]])
				session_data["yss"].append([y, y+count, y+count, y])
				session_data["colors"].append(task_types[task]["color"])
				session_data["tasktype"].append(task)
				session_data["running_tasks"].append(str(count))

				y += count

		# Create data point for current timestamp with number of runnig tasks until now
		tarray = np.append(tarray, [[doc["timestamp"], len(current_running)]], axis=0)

		# If this is a starting event, add one new event to the list of running events
		if doc["data"]["status"] == "started":

			current_running.append(doc["data"]["id"])
			general_info["tasks"] += 1

			if name in current_order:
				# There is one more active task of this type (need to draw a bigger box)
				current_order[name] += 1
			else:
				# If the task is currently not on the task stack, put it on top
				current_order[name] = 1

				# If the task has not been seen before, assign a new color
				if not name in task_types:
				#if not task_types.has_key(name):

					color = Paired12[len(task_types.keys()) % 12]
					task_types[name] = {"color": color}

		# If this is a stop event and we have seen the corresponding start event before, we remove the id from the list of running tasks
		elif doc["data"]["status"] == "ok":

			# If we have not seen the corresponding start event jet, continue with loop
			if not doc["data"]["id"] in current_running:
				continue

			current_running.remove(doc["data"]["id"])

			if current_order[name] == 1:
				# The last task of this type is finished (need to remove it from the stack)
				del(current_order[name])

			else:
				# There are still some tasks of this type running
				current_order[name] -= 1

		# Create data point for current timestamp with number of runnig tasks including new event
		tarray = np.append(tarray, [[doc["timestamp"], len(current_running)]], axis=0)

		# generate points for the loads at the three sample points
		tmp    = [float(doc[attr]) if not doc[attr] == "NA" else -1 for attr in attributes]
		darray = np.append(darray, [tmp], axis=0)

		last_time = doc["timestamp"]

	# Add the neccessary points to close the polygon if there are still some stop events that have not been seen
	if len(current_running) > 0:
		tarray = np.append(tarray, [[tarray[-1, 0], 0]], axis=0)

	tmin = min(darray[:,0])
	general_info["start_time"] = tmin

	for i in range(len(darray[:,0])):
		darray[i,0] -= tmin

	for i in range(len(tarray[:,0])):
		tarray[i,0] -= tmin

	for x in session_data["xss"]:
		for i in range(len(x)):
			x[i] -= tmin

	data = {}
#	for attr in attributes[1:]:
	for attr in attributes_ord:

#		if attributes.index(attr)-1 in checkbox_group_p.active:
		if attributes_ord.index(attr) in checkbox_group_p.active:

			# x-coordinates for every attribute seperately t
#			data["timestamp_"+attr] = darray[:,0]
#			data[attr]              = darray[:,attributes.index(attr)]

			tmpy = list(darray[:,attributes.index(attr)])

			# Remove NA's (-1)
			while True:

				idx = 0
				
				try:
					idx = tmpy.index(-1)
				except:
					break
				
				if idx == 0:
					tmpy[idx] = 0
				else:
					tmpy[idx] = tmpy[idx-1]

			data["timestamp_"+attr] = list(darray[:,0])
			data[attr]              = tmpy

		else:

			data["timestamp_"+attr] = darray[:,0]
			data[attr]              = [0 for _ in range(darray.shape[1])]

	tdata = {}
	tdata["time"]  = tarray[:,0]
	tdata["tasks"] = tarray[:,1]

	return data, tdata, session_data

def select_run(run_id):

	global source
	global task_source
	global session_source
	global current_run

	global task_types

	global general_info

	if run_id == current_run:
		return 

	task_types    = {}
	current_limit = None
	current_run   = run_id

	# Reset variables for new task
	general_info["tasks"]        = run_map[run_id]["numLogEntries"]/2
	general_info["elapsed_time"] = 0

	source.data, task_source.data, session_source.data = query_events(run_id)

	manualLegendBox.text = legendFormat(task_types)

	# update general information info boxes
	numMessages.text = infoBoxFormat("tasks", str(general_info["tasks"]))
	runID.text       = infoBoxFormat("run", str(current_run))
	startTime.text   = infoBoxFormat("start time [ms]", str(general_info["start_time"]))

# the database connection
db = MongoClient().scientificworkflowlogs
datasource = db.test

attributes    = ["timestamp", "min1", "min5", "min15", "duration", "procs_total", "procs_running", "procs_sleeping", "procs_waiting", "procs_vmsize", "procs_rss", "task_total", "task_running", "task_sleeping", "task_waiting", "ram_shared", "ram_buffer", "swap_total", "swap_free"]
attributes_p1 = ["min1", "min5", "min15", "procs_running", "procs_waiting", "task_running", "task_waiting", "ram_shared", "swap_total", "swap_free"]
attributes_p2 = ["procs_total", "procs_sleeping", "task_total", "task_sleeping", "ram_buffer"]
attributes_p3 = ["procs_vmsize", "procs_rss"]
attributes_p4 = ["duration"]

# Order attributes by plot, such that the checkboxes appear in order
attributes_ord = attributes_p1 + attributes_p2 + attributes_p3 + attributes_p4

#checkbox_group_p  = CheckboxGroup(labels=attributes[1:], active=[i for i in range(len(attributes[1:]))])
checkbox_group_p  = CheckboxGroup(labels=attributes_ord, active=[i for i in range(len(attributes_ord))])

# Redraw plots, if a checkbox was set or unset
def checkbox(attr, old, new):

	global source
	global task_source
	global current_run
	global checkbox_group_p
	global attributes
	global attributes_ord
	global datasource

	events_pipeline = [
		{"$match": {"session.id": current_run}},
		{"$sort": {"timestamp": 1}},
	]

	# Query for events
	l = list(datasource.aggregate(events_pipeline))

	# Number of attributes to parse
	numAttr = len(attributes)

	# Array to collect all the points to draw for the attributes and for the tasks
	darray = np.empty((0,numAttr), float)
	tarray = np.empty((0,2), float)

	current_running = []

	# Go through all events in l
	for doc in l:

		# Create data point for current timestamp with number of runnig tasks until now
		tarray = np.append(tarray, [[doc["timestamp"], len(current_running)]], axis=0)

		# If this is a starting event, add one new event to the list of running events
		if doc["data"]["status"] == "started":

			current_running.append(doc["data"]["id"])
			general_info["tasks"] += 1

		# If this is a stop event and we have seen the corresponding start event before, we remove the id from the list of running tasks
		elif doc["data"]["status"] == "ok":

			# If we have not seen the corresponding start event jet, continue with loop
			if not doc["data"]["id"] in current_running:
				continue

			current_running.remove(doc["data"]["id"])

		# Create data point for current timestamp with number of runnig tasks including new event
		tarray = np.append(tarray, [[doc["timestamp"], len(current_running)]], axis=0)

		# generate points for the loads at the three sample points
		tmp    = [float(doc[attr]) if not doc[attr] == "NA" else -1 for attr in attributes]
		darray = np.append(darray, [tmp], axis=0)

	# Add the neccessary points to close the polygon if there are still some stop events that have not been seen
	if len(current_running) > 0:
		tarray = np.append(tarray, [[tarray[-1, 0], 0]], axis=0)
	
	data = {}

	for attr in attributes_ord:

		if attributes_ord.index(attr) in checkbox_group_p.active:

			data["timestamp_"+attr] = darray[:,0]
			data[attr]              = darray[:,attributes.index(attr)]

		else:

			data["timestamp_"+attr] = darray[:,0]
			data[attr]              = [0 for _ in range(darray.shape[1])]

	source.data, task_source.data, x = query_events(current_run)

checkbox_group_p.on_change("active", checkbox)

# get a list of all scientific workflow runs with id, number of log messages and start timestamp.
run_map = query_runs()

# select the latest run by default
current_run = run_map.keys()[0]

general_info = {"tasks": 0, "start_time" : 0, "elapsed_time": 0}

task_types = {}

# active tasks history plot dimensions
PLOT_WIDTH = 1400
PLOT_HEIGHT = 600

dict = {}

for attr in attributes[1:]:

	dict["timestamp_"+attr] = []
	dict[attr] = []

# The data sources for the main plot and the task boxes
source      = ColumnDataSource(dict)
task_source = ColumnDataSource({"tasks" : [], "time" : []})

# The data source for the plo displaying the active tasks over time
session_source = ColumnDataSource({"xss": [], "yss": [], "colors": [], "tasktype": [], "running_tasks": []})

source.data, task_source.data, session_source.data = query_events(current_run)

# a dropdown menu to select a run for which data shall be visualized
run_menu = [(run_format(k, v['numLogEntries'], v['tstart']), add_prefix(k)) for k, v in run_map.iteritems() if v['tstart'] is not None]  # use None for separator
dropdown = Dropdown(label="run", button_type="warning", menu=run_menu)
dropdown.on_click(lambda newValue: select_run(rem_prefix(newValue)))

manualLegendBox = Div(text=legendFormat(task_types), width=PLOT_WIDTH, height=80)

# info boxes to display the number of log messages in this run and the elapsed wall clock time
numMessages = Div(text=infoBoxFormat("Messages", 0), width=200, height=100)
runID       = Div(text=infoBoxFormat("run", str(current_run)), width=400, height=100)
startTime   = Div(text=infoBoxFormat("start time [ms]", str(general_info["start_time"])), width=200, height=100)

# Set the properties of the first plot
p = figure(plot_height=PLOT_HEIGHT, plot_width=PLOT_WIDTH,
		   #tools="pan,xpan,wheel_zoom,xwheel_zoom,ywheel_zoom,xbox_zoom,reset",
		   tools="xpan,xwheel_zoom,xbox_zoom,reset,save",
		   toolbar_location="above",
		   x_axis_type="linear", y_axis_location="right", y_axis_type=None,
		   webgl=True)
p.x_range.range_padding = 0
p.y_range.range_padding = 0
p_yaxis                 = LinearAxis(ticker=AdaptiveTicker(min_interval=1.0))
p.add_layout(p_yaxis, 'right')

# The extra y axis for the task plot in the background
p.extra_y_ranges = {"tasks": Range1d(start=min(task_source.data["tasks"]), end=max(task_source.data["tasks"]))}
p.add_layout(LinearAxis(y_range_name="tasks"), "left")

# Set the properties of the second plot
p2 = figure(plot_height=PLOT_HEIGHT, plot_width=PLOT_WIDTH,
		   #tools="pan,xpan,wheel_zoom,xwheel_zoom,ywheel_zoom,xbox_zoom,reset",
		   tools="xpan,xwheel_zoom,xbox_zoom,reset,save",
		   toolbar_location="above",
		   x_axis_type="linear", y_axis_location="right", y_axis_type=None,
		   x_range=p.x_range,
		   webgl=True)
p2.x_range.range_padding = 0
p2.y_range.range_padding = 0
p2_yaxis                 = LinearAxis(ticker=AdaptiveTicker(min_interval=1.0))
p2.add_layout(p2_yaxis, 'right')

# The extra y axis for the task plot in the background
p2.extra_y_ranges = {"tasks": Range1d(start=min(task_source.data["tasks"]), end=max(task_source.data["tasks"]))}
p2.add_layout(LinearAxis(y_range_name="tasks"), "left")

# Set the properties of the third plot
p3 = figure(plot_height=PLOT_HEIGHT, plot_width=PLOT_WIDTH,
		   #tools="pan,xpan,wheel_zoom,xwheel_zoom,ywheel_zoom,xbox_zoom,reset",
		   tools="xpan,xwheel_zoom,xbox_zoom,reset,save",
		   toolbar_location="above",
		   x_axis_type="linear", y_axis_location="right", y_axis_type=None,
		   x_range=p.x_range,
		   webgl=True)
p3.x_range.range_padding = 0
p3.y_range.range_padding = 0
#p3.xaxis.axis_label      = "time since start [ms]"
p3.add_layout(LinearAxis(ticker=AdaptiveTicker(min_interval=1.0)), 'right')

# The extra y axis for the task plot in the background
p3.extra_y_ranges = {"tasks": Range1d(start=min(task_source.data["tasks"]), end=max(task_source.data["tasks"]))}
p3.add_layout(LinearAxis(y_range_name="tasks"), "left")

# Set the properties of the third plot
p4 = figure(plot_height=PLOT_HEIGHT, plot_width=PLOT_WIDTH,
		   #tools="pan,xpan,wheel_zoom,xwheel_zoom,ywheel_zoom,xbox_zoom,reset",
		   tools="xpan,xwheel_zoom,xbox_zoom,reset,save",
		   toolbar_location="above",
		   x_axis_type="linear", y_axis_location="right", y_axis_type=None,
		   x_range=p.x_range,
		   webgl=True)
p4.x_range.range_padding = 0
p4.y_range.range_padding = 0
p4.xaxis.axis_label      = "time since start [ms]"
p4.add_layout(LinearAxis(ticker=AdaptiveTicker(min_interval=1.0)), 'right')

# The extra y axis for the task plot in the background
p4.extra_y_ranges = {"tasks": Range1d(start=min(task_source.data["tasks"]), end=max(task_source.data["tasks"]))}
p4.add_layout(LinearAxis(y_range_name="tasks"), "left")

# Plot the running tasks as patch in the background
p.patch(x="time", y="tasks", color=COLORS[0], source=task_source, line_width=3, alpha=1, legend="tasks", y_range_name="tasks")
p2.patch(x="time", y="tasks", color=COLORS[0], source=task_source, line_width=3, alpha=1, legend="tasks", y_range_name="tasks")
p3.patch(x="time", y="tasks", color=COLORS[0], source=task_source, line_width=0, alpha=1, legend="tasks", y_range_name="tasks")
p4.patch(x="time", y="tasks", color=COLORS[0], source=task_source, line_width=0, alpha=1, legend="tasks", y_range_name="tasks")

# Draw everyhing for the first plot
for attr in attributes[1:]:

	if attr in attributes_p2 or attr in attributes_p3 or attr in attributes_p4:
		continue

	p.line(x="timestamp_"+attr, y=attr, line_color=COLORS[attributes.index(attr)], source=source, line_width=3, alpha=0.5, legend=attr)

# Draw everyhing for the second plot
for attr in attributes_p2:
	p2.line(x="timestamp_"+attr, y=attr, line_color=COLORS[attributes.index(attr)], source=source, line_width=3, alpha=0.5, legend=attr)

# Draw everyhing for the third plot
for attr in attributes_p3:
	p3.line(x="timestamp_"+attr, y=attr, line_color=COLORS[attributes.index(attr)], source=source, line_width=3, alpha=05, legend=attr)

for attr in attributes_p4:
	p4.line(x="timestamp_"+attr, y=attr, line_color=COLORS[attributes.index(attr)], source=source, line_width=3, alpha=05, legend=attr)

# Add legends to all of the plots
p.legend.location  = "top_left"
p2.legend.location = "top_left"
p3.legend.location = "top_left"
p4.legend.location = "top_left"

t = figure(plot_height=PLOT_HEIGHT, plot_width=PLOT_WIDTH,
		   #tools="xpan,xwheel_zoom,xbox_zoom,reset,hover", ##### ,hover # toolbar_location="right", #this is ignored for some reason.
		   tools="xpan,xwheel_zoom,xbox_zoom,reset, hover,save",
		   toolbar_location="above",
		   x_axis_type="linear", y_axis_location="right", y_axis_type=None,
		   x_range=p.x_range,
		   webgl=True)

t.x_range.range_padding = 0
t.y_range.range_padding = 0
t.xaxis.axis_label = "time since start [ms]"
yaxis              = LinearAxis(ticker=AdaptiveTicker(min_interval=1.0))
t.add_layout(yaxis, 'left')
t.yaxis.axis_label = "Running Tasks"

t.patches(xs="xss", ys="yss", color="colors", source=session_source, line_width=0, alpha=1)

hover = t.select_one(HoverTool)
hover.point_policy = "follow_mouse"
hover.tooltips = [
	("task type", "@tasktype"),
	("#tasks", "@running_tasks"),
]

# Function that clears all checkboxes and calls checkbox to redraw the plots
def clear():
	checkbox_group_p.active = []
	checkbox("", "", "")

# Button to clear all checkboxes
clear_button = Button(label="clear all", width=20)
clear_button.on_click(clear)

# Function that sets all checkboxes and calls checkbox to redraw the plots
def select_all():
	checkbox_group_p.active = [i for i in range(len(attributes[1:]))]
	checkbox("", "", "")

# Button to select all checkboxes
all_button = Button(label="select all", width=20)
all_button.on_click(select_all)

layout = column(
	#row(WidgetBox(dropdown, width=405, height=100), WidgetBox(width=500),WidgetBox(runID)),
	row(WidgetBox(dropdown, width=410, height=100)),
	row(runID, startTime),
#	row(runID, numMessages),  
	row(column(p,p2,p3,p4,t, manualLegendBox), column(checkbox_group_p, all_button, clear_button)),
)

curdoc().add_root(layout)
curdoc().title = "run Dashboard"

curdoc().add_periodic_callback(lambda: select_run(current_run), 300)
