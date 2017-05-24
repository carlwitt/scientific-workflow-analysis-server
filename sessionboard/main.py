'''
 Scientific Workflow Server Polling Dashboard

 Author: Carl Witt cpw@posteo.de

 Delivers a static visual summary of the log entries associated to a scientific workflow (e.g., cuneiform) session.
 The client is designed to replace its current display with a newly delivered one in regular intervals.
 Design aspects:
	- sessions overview must be refreshed periodically
	- data for a session must be refreshed periodically
		- use a common construct? => first prototype, then refactor!

	- transferring visual information like plot width for aggregation (no need to deliver and render thousands of data points on hundreds of pixels)
	- nothing changed message? => performance optimization is really not adequate here.

	The cuneiform data has no timestamp, because its ObjectId in the MongoDB can serve that purpose.
	However, for logs imported externally into the MongoDB, we have to use the timestamp and not the ObjectID.
	This might be resolved by either providing a timestamp for all entries, or

 History
  The first version used to the ColumnDataSource.stream method to push data to the client.
  This was facing performance and stability issues. For instance, adding multiple columns to the data set
  during one update operation caused the renderer to crash without usable error messages. Firefox was getting slow
  very quickly and performance was not satisfactory for a few thousand messages.
  Moreover, it could happen that the server kept polling the database after the client went away, resulting in zombi activity.


 TODO: add a mapping from data series name to color (to be used in other visualizations, like time share and bottleneck)

'''
from collections import OrderedDict
from datetime import time, datetime

import numpy as np
from bokeh.layouts import row, column
from bokeh.models import ColumnDataSource, Slider, Div, Dropdown, SingleIntervalTicker, AdaptiveTicker, HoverTool, \
	TableColumn, DataTable
from bokeh.models.axes import LinearAxis
from bokeh.models.layouts import WidgetBox
from bokeh.plotting import curdoc, figure
from pandas import DataFrame
from pymongo import MongoClient

from bokeh.models.widgets import Select

# brewer palette "paired"
Paired12 = ['#a6cee3', '#1f78b4', '#b2df8a', '#33a02c', '#fb9a99', '#e31a1c', '#fdbf6f', '#ff7f00', '#cab2d6',
			'#6a3d9a', '#ffff99', '#b15928']

def highlight_task(attr, old, new):

	global hsource
	global placeholder
	global focus

	patches = {
		"xss": [],
		"yss": [],
		"colors": []
	}

	focus = new

	if new == placeholder:
		hsource.data = patches
		return

	for idx in range(len(source.data["tasktype"])):
		if not source.data["tasktype"][idx] == new:
			patches["xss"].append(source.data["xss"][idx])
			patches["yss"].append(source.data["yss"][idx])
			patches["colors"].append((idx, "#FFFFFF"))

	hsource.data = patches

# =====================================================================================================================
# Business Logic Methods
# =====================================================================================================================

'''
Retrieves a list of all sessions with start time, number of messages and session id.
Returns and ordered dictionary that allows to access the session information by session id but maintains the chronological order (newest first) of the sessions.
This is used to populate the session select dropdown menu.
'''
def query_sessions():
	global datasource
	pipeline = [
		{"$group": {"_id": "$session.id",
					"numLogEntries": {"$sum": 1},
					"tstart": {"$first": "$session.tstart"},
					}},
		{"$sort": {"tstart": -1}}
	]
	documents = list(datasource.aggregate(pipeline))
	session_map = OrderedDict()
	for session in documents:
		session_map[session['_id']] = dict(numLogEntries=session['numLogEntries'], tstart=session['tstart'])
	return session_map

''''
	Draw the boxes for all tasks that were running between last_event_time and timestamp
'''
def draw(timestamp):

	global source
#	global hsource
#	global placeholder
#	global focus
	
	global general_info
	global current_order

	# If there are multiple events at the same time, don't draw, until all are collected in the task stack
	if general_info["last_event_time"] == timestamp:
		return

	# Create an empty dictionary where we collect the new points
	new_data = dict()

	# The y coordinate of the current task
	y = 0

	for task in current_order:
		# Go through the task stack

		count = current_order[task]

		# Create the four corners of the box for the current task:
		#
		#   (last_event_time, y+count) --- (timestamp, y+count)
		#          |                                  |
		#   (last_event_time, y) --------------- (timestamp, y)

		new_data["xss"]           = [[datetime.fromtimestamp(general_info["last_event_time"]), datetime.fromtimestamp(general_info["last_event_time"]), timestamp, timestamp]]
		new_data["yss"]           = [[y, y + count, y + count, y]]
		# Assign the correct color to the box
		new_data["colors"]        = [task_types[task]["color"]]
		# Remember additional information about the task the box belongs to
		new_data["tasktype"]      = task
		new_data["running_tasks"] = str(count)

		# Update the y coordinate for the next task
		y += count

		# Send the new data to be drawn
		source.stream(new_data)

#		if focus == placeholder or task == focus:
#			continue
		
		# Update the hilight
#		new_data["colors"] = ["#FFFFFF" for c in new_data["colors"]]
#		new_data.pop("tasktype")
#		new_data.pop("running_tasks")
#		hsource.stream(new_data)

'''
Retrieves the invocation lifecycle events (started, ok) from the database, in chronological order.
Returns data in a format that is understood by the multiline and patches renderer and result in
a visualization that displays the number of running tasks per task type as shaded (area-like) stacked step series.
'''
def query_running_tasks_history_stacked(session_id):

	global datasource
	global task_types
	global current_order

	global general_info

	# TODO: Query for late events and redraw if neccessary

	# Query for all tasks that have a timestamp greater than last_event_time and order them by the timestamp
	task_types_pipeline = [
		{"$match": {"session.id": session_id, "timestamp": { "$gt" : general_info["last_event_time"] }}},
		{"$sort": {"timestamp": 1}},
		{"$project": {"task_type": 1, "timestamp": 1, "event": 1}}
	]

	l = list(datasource.aggregate(task_types_pipeline))
	if len(l) == 0:
		return

	# Update the task type stack for the next task
	for doc in l:
		# Go through all events that were found

		# Update the general information of the current session
		general_info["active_tasks"] += 1
		if general_info["last_event_time"] != 0:
			general_info["elapsed_time"] += doc["timestamp"] - general_info["last_event_time"]

		# Draw the boxes for the time elapsed
		draw(datetime.fromtimestamp(doc["timestamp"])) 

		name = doc["task_type"]

		if doc["event"] == "invoc_start":
			# The Event is a start event

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

		elif doc["event"] == "invoc_stop":
			# The Event is a stop event	

			if current_order[name] == 1:
				# The last task of this type is finished (need to remove it from the stack)

				del(current_order[name])

			else:
				# There are still some tasks of this type running

				current_order[name] -= 1

		else:
			# The event type is unknown
			continue

		# Update the timestamps
		general_info["last_event_time"] = doc["timestamp"]

# =====================================================================================================================
# User Interface Methods
# =====================================================================================================================

"""
For storage in the select session dropdown menu, the values are prefixed (using strings that parse as int leads to an error in the widget)
"""
def add_prefix(session_str):
	return "s_" + str(session_str)
def rem_prefix(session_str):
	return session_str[2:] if session_str.startswith("s_") else session_str

"""
Switch to another scientific workflow session. Also used to update the current view.
"""
def select_session(session_id):

	global source
	global current_limit
	global current_session
	
	global task_types
	global current_order

	global session_map
	global dropdown

	global general_info

#	global select
#	global placeholder

	# Update session dropdown menu
	session_map = query_sessions()
	dropdown.menu = [(session_format(k, v['numLogEntries'], v['tstart']), add_prefix(k)) for k, v in session_map.iteritems() if v['tstart'] is not None]

	if session_id != current_session:
		task_types = {}
		current_limit = None
		current_session = session_id

		# Reset the timer
		general_info["last_event_time"] = 0
		# Remove all the rectangles from the previous session
		source.data["xss"]           = []
		source.data["yss"]           = []
		source.data["colors"]        = []
		source.data["tasktype"]      = []
		source.data["running_tasks"] = []
		# Reset the current order
		current_order.clear()

		# Reset variables for new task
		general_info["active_tasks"] = 0
		general_info["elapsed_time"] = 0

	# query active tasks history data (poll for new data or switch session)
	query_running_tasks_history_stacked(session_id)

#	select.options = [placeholder] + task_types.keys()

	# add session information to active tasks chart title
	p.title.text = session_format_short(session_id, session_map[session_id]['tstart']) #"Session " + session_id

	# update legend box
	manualLegendBox.text = legendFormat(task_types)

	# update general information info boxes
	numMessages.text = infoBoxFormat("Log Messages", str(general_info["active_tasks"]))
	wallClockTime.text = infoBoxFormat("Elapsed Time (wall)", str(datetime.fromtimestamp(general_info["elapsed_time"]).time()))
	sessionID.text = infoBoxFormat("Session", str(current_session))


""" Used to generate the labels in the session dropdown box """
def session_format(session_id, numMessages, timestamp):
	isoFormat = datetime.fromtimestamp(timestamp/1000.0).isoformat(" ")
	return "%s id:%s (%s message%s)" % (isoFormat, session_id, numMessages, "s" if int(numMessages) > 1 else "")
""" Used to generate the title in the active tasks plot """
def session_format_short(session_id, timestamp):
	isoFormat = datetime.fromtimestamp(timestamp/1000.0).isoformat(" ")
	return "Session %s, started %s" % (session_id, isoFormat)

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

# =====================================================================================================================
# Globals and Configuration
# =====================================================================================================================

# the maximum number of log messages to retrieve from the database
current_limit = None

# the database connection
db = MongoClient().scientificworkflowlogs
datasource = db.test

# get a list of all scientific workflow sessions with id, number of log messages and start timestamp.
session_map = query_sessions()

# select the latest session by default
current_session = session_map.keys()[0]

general_info = {"active_tasks": 0, "elapsed_time": 0, "last_event_time": 0}

# a map from task type name to associated attributed, currently only rendering color (which is needed across several visualizations to be consistent)
# e.g. task_type['diffit']['color'] = '#12ab3f'
task_types = OrderedDict()

# The current order of tasks together with the number of currently running tasks of this type
current_order = OrderedDict()

# active tasks history plot dimensions
PLOT_WIDTH = 1400
PLOT_HEIGHT = 600

# the main data source for all visualizations.
# xss, yss and colors belong the active tasks visualization
source = ColumnDataSource({"xss": [], "yss": [], "colors": [], "tasktype": [], "running_tasks": []})
query_running_tasks_history_stacked(current_session)

# =====================================================================================================================
# Controls
# =====================================================================================================================

# a dropdown menu to select a session for which data shall be visualized
session_menu = [(session_format(k, v['numLogEntries'], v['tstart']), add_prefix(k)) for k, v in session_map.iteritems() if v['tstart'] is not None]  # use None for separator
dropdown = Dropdown(label="Session", button_type="warning", menu=session_menu)
dropdown.on_click(lambda newValue: select_session(rem_prefix(newValue)))

# info boxes to display the number of log messages in this session and the elapsed wall clock time
numMessages = Div(text=infoBoxFormat("Log Messages", 0), width=200, height=100)
wallClockTime = Div(text=infoBoxFormat("Elapsed Time (wall)", str(time())), width=400, height=100)
sessionID = Div(text=infoBoxFormat("Session", str(current_session)), width=200, height=100)

# legend box that lists all task type names and their associated colors
manualLegendBox = Div(text=legendFormat(task_types), width=PLOT_WIDTH, height=80)

##########

#placeholder = "..."
#select_options = [placeholder] + task_types.keys()
#select = Select(title="Highlight task:", value=select_options[0], options=select_options)
#select.on_change("value", highlight_task)

#hsource = ColumnDataSource({"xss": [], "yss": [], "colors": []})

#focus = placeholder

######

# =====================================================================================================================
# Plots
# =====================================================================================================================

# the main plot, visualizes the number of running tasks per task type over time
p = figure(plot_height=PLOT_HEIGHT, plot_width=PLOT_WIDTH,
		   tools="xpan,xwheel_zoom,xbox_zoom,reset,hover", ##### ,hover # toolbar_location="right", #this is ignored for some reason.
		   x_axis_type="datetime", y_axis_location="right", y_axis_type=None, # y_axis_type="log",
		   webgl=True)

p.x_range.range_padding = 0
p.xaxis.axis_label = "Date and Time"
p.legend.location = "top_left"
yaxis = LinearAxis(ticker=AdaptiveTicker(min_interval=1.0))
p.add_layout(yaxis, 'right')
p.yaxis.axis_label = "Number of Running Tasks"

multiline = p.patches(xs="xss", ys="yss", color="colors", source=source, line_width=0, alpha=0.7)
#multiline = p.patches(xs="xss", ys="yss", color="colors", source=source, line_width=2, alpha=0.7) # legend="legends" doesn't work, it's just one logical element
# renderers['merge'] = p.line(x="time", y="merge", legend="merge", source=source)

########

#highlight = p.patches(xs="xss", ys="yss", color="colors", source=hsource, line_width=1, alpha=.9)

########

hover = p.select_one(HoverTool)
hover.point_policy = "follow_mouse"
hover.tooltips = [
	("task type", "@tasktype"),
	("#tasks", "@running_tasks"),
]

# =====================================================================================================================
# Main
# =====================================================================================================================

# prepare document
layout = column(
	row(WidgetBox(dropdown, width=405, height=100)),
	row(sessionID, wallClockTime, numMessages), #cumulativeTime
	p,
	manualLegendBox,
#	select,
	#progress,
	#limit,
)
curdoc().add_root(layout)
curdoc().title = "Session Dashboard"

# add periodical polling of the database for new data (pushes to the client via websockets)
#curdoc().add_periodic_callback(lambda: select_session(current_session), 150)
curdoc().add_periodic_callback(lambda: select_session(current_session), 300)

