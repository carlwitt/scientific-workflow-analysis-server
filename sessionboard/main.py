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
from bokeh.models import ColumnDataSource, Slider, Div, Dropdown, SingleIntervalTicker, AdaptiveTicker, HoverTool
from bokeh.models.axes import LinearAxis
from bokeh.models.layouts import WidgetBox
from bokeh.plotting import curdoc, figure
from pymongo import MongoClient

# brewer palette "paired"
Paired12 = ['#a6cee3', '#1f78b4', '#b2df8a', '#33a02c', '#fb9a99', '#e31a1c', '#fdbf6f', '#ff7f00', '#cab2d6',
			'#6a3d9a', '#ffff99', '#b15928']

# =====================================================================================================================
# Business Logic Methods
# =====================================================================================================================

'''
Retrieves a list of all sessions with start time, number of messages and session id.
Returns and ordered dictionary that allows to access the session information by session id but maintains the chronological order (newest first) of the sessions.
This is used to populate the session select dropdown menu.
'''
def query_sessions():
	global db
	pipeline = [
		{"$group": {"_id": "$session.id",
					"numLogEntries": {"$sum": 1},
					"tstart": {"$first": "$session.tstart"},
					}},
		{"$sort": {"tstart": -1}}
	]
	documents = list(db.raw.aggregate(pipeline))
	session_map = OrderedDict()
	for session in documents:
		session_map[session['_id']] = dict(numLogEntries=session['numLogEntries'], tstart=session['tstart'])
	return session_map

'''
Retrieves high level summaries of the given session, such as elapsed wall time between first and last message.
Used to fill the infoboxes.
'''
def get_general_information(session_id):
	global db
	pipeline = [
		{ "$match": { "session.id": session_id} },
		{"$sort": {"_id": 1}},  # order log entries by arrival time at database
		{ "$group": {"_id": "null",
			"firstMessage": { "$min": "$_id"},							# for computing the wall clock time
			"lastMessage": { "$max": "$_id"},
			"maxInvocationDuration": { "$max": "$data.info.tdur"},		# longest invocation
			"sumInvocationDuration": { "$sum": "$data.info.tdur"},		# accumulated compute time
			"avgInvocationDuration": { "$avg": "$data.info.tdur"},		# average of invocation durations
			"sdInvocationDuration": { "$stdDevSamp": "$data.info.tdur"},	# standard deviation (sample) of the invocation durations
			"numMessages": { "$sum": 1}									# number of messages
		}}
	]

	result = list(db.raw.aggregate(pipeline))[0]

	return {
		'wall_time': result['lastMessage'].generation_time - result['firstMessage'].generation_time,
		'parallel_time': datetime.fromtimestamp(result['sumInvocationDuration']/1000.0) - datetime.fromtimestamp(0),
		'num_messages': result['numMessages'],
	}

'''
Retrieves the invocation lifecycle events (started, ok) from the database, in chronological order.
Returns data in a format that is understood by the multiline and patches renderer and result in
a visualization that displays the number of running tasks per task type as shaded (area-like) stacked step series.
'''
def query_running_tasks_history_stacked(session_id, limit=None):
	global task_types
	print("query running tasks for session_id: {0}".format(session_id))

	#
	# 1. query distinct task types in the session, ordered alphabetically
	#
	task_types_pipeline = [
		{"$match": {"session.id": session_id}},
		{"$sort": {"_id": 1}},  # order log entries by arrival time at database
		{"$group": {"_id": "$data.lam_name"}},  # group by task type
		{"$sort": {"_id": 1}},		# give task types in alphabetical order
	]

	# create a task types array that defines the stacking order (bottom to top) of the patches in the chart
	task_type_names = [doc["_id"] for doc in list(db.raw.aggregate(task_types_pipeline))]
	# task_type_names = ["A", "B"]

	# add to task types map (stores the associated color) if the task type hasn't been seen before
	for name in task_type_names:
		if not task_types.has_key(name):
			color = Paired12[len(task_types.keys()) % 12]
			task_types[name] = {'color': color}

	#
	# 2. query for invocation lifecycle events (started, ok), in chronological order
	#
	limit_clause = {"$limit": limit}
	events_pipeline = [
		{"$match": {"session.id": session_id}},
		{"$sort": {"_id": 1}},  # order log entries by arrival time at database
		limit_clause,
		{"$project": {"task_type": "$data.lam_name",  # group by task type
					  "time": "$_id",
					  "lifecycle_event": "$data.status"}},  # for each task type
	]
	if limit is None: events_pipeline.remove(limit_clause)

	events_chronological = list(db.raw.aggregate(events_pipeline))
	# t0 = ObjectId.from_datetime(datetime(2010, 1, 1))
	# t1 = ObjectId.from_datetime(datetime(2011, 1, 1))
	# t2 = ObjectId.from_datetime(datetime(2012, 1, 1))
	# t3 = ObjectId.from_datetime(datetime(2013, 1, 1))
	# t4 = ObjectId.from_datetime(datetime(2014, 1, 1))
	# t5 = ObjectId.from_datetime(datetime(2015, 1, 1))
	# t6 = ObjectId.from_datetime(datetime(2016, 1, 1))
	#
	# events_chronological = [
	# 	dict(task_type='A', time=t0, lifecycle_event='started'),
	#
	# 	dict(task_type='A', time=t1, lifecycle_event='started'),
	# 	dict(task_type='B', time=t1, lifecycle_event='started'),
	#
	# 	dict(task_type='A', time=t2, lifecycle_event='ok'),
	#
	# 	dict(task_type='B', time=t3, lifecycle_event='started'),
	#
	# 	dict(task_type='A', time=t4, lifecycle_event='ok'),
	# 	dict(task_type='B', time=t4, lifecycle_event='started'),
	#
	# 	dict(task_type='B', time=t5, lifecycle_event='ok'),
	#
	# 	dict(task_type='B', time=t6, lifecycle_event='ok'),
	# 	dict(task_type='B', time=t6, lifecycle_event='ok'),
	# ]

	# the result data structure, each entry in xss is a list of x values, forming a polygon together with the according entry in the yss array.

	# initialize result data structure
	# make all series start at zero running tasks
	# this might not be accurate for degenerate logs, but it's convenient to always have a previous element available.
	first_timestamp = events_chronological[0]['time'].generation_time if len(events_chronological) > 0 else 0
	data = {"xss": [ [ first_timestamp ] for _ in task_type_names],
			"yss": [ [ 0 ] for _ in task_type_names],
			"tasktype": task_type_names,
			"colors": [task_types[name]['color'] for name in task_type_names]}

	# stores the number of currently running tasks for each task type.
	# same order as defined by task_type_names, so the i-th element corresponds to the series task_type_names[i]
	previous = [0] * len(task_type_names)

	# for each event add a new data point to all series (represented as patches to get the area below them shaded)
	# this will result in visually redundant data points (they do not effect the rendering) but it's convenient to have series of equal length for the stacking
	# (the top one series forms the bottom of the next series)
	for event in events_chronological:

		# silently skip unknown lifecycle events
		if event['lifecycle_event'] not in [u'started', u'ok']: continue

		# iterate over task types in bottom-up stacking order
		for i, tt in enumerate(task_type_names):

			running = previous[i]
			# check whether the number of running tasks has changed through this event
			if event['task_type'] == tt:
				if event['lifecycle_event'] == u"started": running += 1
				if event['lifecycle_event'] == u"ok": running -= 1

			# cumulative is the number of running task up to including this task type tt (in stacking order)
			# for task types below tt data['yss'][i-1][-1] is the current time, because we've added the value in the last loop iteration,
			# and thus len(data['yss'][i-1]) = len(data['yss'][i]) + 1
			sum_below = data['yss'][i-1][-1] if i > 0 else 0
			cumulative = running + sum_below

			# add (new x, previous y) to get step like segments instead of slopes
			data['xss'][i].append(event['time'].generation_time)
			data['yss'][i].append(data['yss'][i][-1])

			# add new (x, y) pair
			data['xss'][i].append(event['time'].generation_time)
			data['yss'][i].append(cumulative)

			previous[i] = running

	# after having added all the values, close the polygon by setting it to the upper border of the polygon below (in stacking order)
	# start at topmost patch because nobody accesses this patches surface after this (because we alter it now)
	for i in range(len(task_type_names)-1, -1, -1): #list(reversed(list(enumerate(task_type_names)))):
		data['xss'][i].extend(list(reversed(data['xss'][i])))
		upper_border_previous_polygon = list(reversed(data['yss'][i-1])) if i > 0 else list(np.zeros(len(data['xss'][i])))
		data['yss'][i].extend(upper_border_previous_polygon)

	# print("task_type_names: {0}".format(task_type_names))
	# for i, tt in enumerate(task_type_names):
	# 	print(tt+"\n")
	# 	print(zip(map(lambda dt: dt.year, data['xss'][i]), data['yss'][i]))

	return data


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

	if session_id != current_session:
		task_types = {}
		current_limit = None
		current_session = session_id

	# query active tasks history data (poll for new data or switch session)
	source.data = query_running_tasks_history_stacked(session_id)
	# add session information to active tasks chart title
	p.title.text = session_format_short(session_id, session_map[session_id]['tstart']) #"Session " + session_id
	# update legend box
	manualLegendBox.text = legendFormat(task_types)

	# update general information info boxes
	general_info = get_general_information(current_session)
	numMessages.text = infoBoxFormat("Log Messages", general_info['num_messages'])
	wallClockTime.text = infoBoxFormat("Elapsed Time (wall)", str(general_info['wall_time']))
	# cumulativeTime.text = infoBoxFormat("Elapsed Time (parallel)", datetime.fromtimestamp(general_info['parallel_time']).isoformat())


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

# get a list of all scientific workflow sessions with id, number of log messages and start timestamp.
session_map = query_sessions()

# select the latest session by default
current_session = session_map.keys()[0]

# a map from task type name to associated attributed, currently only rendering color (which is needed across several visualizations to be consistent)
# e.g. task_type['diffit']['color'] = '#12ab3f'
task_types = OrderedDict()

# active tasks history plot dimensions
PLOT_WIDTH = 1400
PLOT_HEIGHT = 600

# the main data source for all visualizations.
# xss, yss and colors belong the active tasks visualization
source = ColumnDataSource({"xss": [], "yss": [], "colors": []})
source.data = query_running_tasks_history_stacked(current_session)

# =====================================================================================================================
# Controls
# =====================================================================================================================

# a dropdown menu to select a session for which data shall be visualized
session_menu = [(session_format(k, v['numLogEntries'], v['tstart']), add_prefix(k)) for k, v in session_map.iteritems() if v['tstart'] is not None]  # use None for separator
dropdown = Dropdown(label="Session", button_type="warning", menu=session_menu)
dropdown.on_click(lambda newValue: select_session(rem_prefix(newValue)))

# a slider to control the maximum number of log messages to query
limit = Slider(title="Limit the number of log messages", value=50, start=0, end=1500, step=10)
def update(_, old, new):
	global current_limit
	current_limit = new if new > 0 else None
	select_session(current_session)
limit.on_change('value', update)

# info boxes to display the number of log messages in this session and the elapsed wall clock time
numMessages = Div(text=infoBoxFormat("Log Messages", 0), width=200, height=100)
wallClockTime = Div(text=infoBoxFormat("Elapsed Time (wall)", str(time())), width=400, height=100)
# cumulativeTime = Div(text=infoBoxFormat("Elapsed Time (parallel)", last.isoformat()), width=400, height=100)

# legend box that lists all task type names and their associated colors
manualLegendBox = Div(text=legendFormat(task_types), width=PLOT_WIDTH, height=250)

# =====================================================================================================================
# Plots
# =====================================================================================================================

# the main plot, visualizes the number of running tasks per task type over time
p = figure(plot_height=PLOT_HEIGHT, plot_width=PLOT_WIDTH,
		   tools="xpan,xwheel_zoom,xbox_zoom,reset,hover", # toolbar_location="right", #this is ignored for some reason.
		   x_axis_type="datetime", y_axis_location="right", y_axis_type=None, # y_axis_type="log",
		   webgl=True)
p.x_range.range_padding = 0
p.xaxis.axis_label = "Date and Time"
p.legend.location = "top_left"
# y axis displays only integer values
yaxis = LinearAxis(ticker=AdaptiveTicker(min_interval=1.0))
p.add_layout(yaxis, 'right')
p.yaxis.axis_label = "Number of Running Tasks"

multiline = p.patches(xs="xss", ys="yss", color="colors", source=source, line_width=2, alpha=0.7) # legend="legends" doesn't work, it's just one logical element
# renderers['merge'] = p.line(x="time", y="merge", legend="merge", source=source)

hover = p.select_one(HoverTool)
hover.point_policy = "follow_mouse"
hover.tooltips = [
    ("task type", "@tasktype"),
]


# =====================================================================================================================
# Main
# =====================================================================================================================

# prepare document
layout = column(
	row(WidgetBox(dropdown, width=405, height=100)),
	row(wallClockTime, numMessages), #cumulativeTime
	p,
	manualLegendBox,
	#limit,
)
curdoc().add_root(layout)
curdoc().title = "Session Dashboard"

# add periodical polling of the database for new data (pushes to the client via websockets)
curdoc().add_periodic_callback(lambda: select_session(current_session), 150)

















# session = push_session(curdoc())
# print("session: {0}".format(session))

# session.loop_until_closed()
# adding it to the document makes the periodic callback server-side
# => I think the idea is that the client leave and come back (with the previous session id) and see the result of the callbacks happened in the meantime
# curdoc().add_periodic_callback(changeSource, 2000)
# It is possible to achieve the desired effect by binding the callback to a use interface element.
# So either triggering the element automatically or calling a callback automatically can push this to the client?



# sets up the main loop
# session = push_session(curdoc())
# session.loop_until_closed()


# def update(attr, old, new):
#     data['python'] = data['python'] + [randint(0,4)]
#     data['pypy'] = data['pypy'] + [randint(4, 8)]
#     data['jython'] = data['jython'] + [randint(3, 9)]
#
#     global area2
#     area2  = Area(data=data, title="Stacked Area Chart", legend="top_left", stack=True, xlabel='time', ylabel='memory')
#     # layout.children[1] = create_figure()
#     # area2.title.text = area2.title.text + "+"

# def add_series(series_name):
# 	global data_series
# 	global columns
# 	global source
# 	global p
#
# 	# assign colors in a round-robin fashion
# 	color = Paired12[len(data_series) % len(Paired12)]
# 	# add
# 	data_series = data_series + [series_name]
# 	columns = base_columns + data_series
#
# 	# add to column data set with all rows set to zero
# 	source.add([0] * len(source.data["time"]), series_name)
# 	# p.line(x='time', y=series_name, legend=series_name, alpha=0.8, line_width=2, color=color, source=source)
# 	y0 = 0 if len(data_series) == 1 else data_series[-2]
# 	print("y0: {0}".format(y0))
# 	p.segment(x0='time', x1='time', y0=y0, y1=series_name, legend=series_name, alpha=0.8, line_width=2, color=color,
# 			  source=source)
#
#
# def changeSource():
# 	global source
# 	# global p
# 	# global renderers
# 	new_data = {}
# 	new_data['time'] = [randint(0, 10), randint(11, 20), randint(21, 30)]
# 	for series in ['merge', 'diffit']:
# 		new_data[series] = [randint(0, 10), randint(0, 10), randint(0, 10)]
# 	source.data = new_data
#
# 	# for series in ['merge', 'diffit']:
# 	#     if not renderers.has_key(series):
# 	#         print("doesn't have renderer for: {0}".format(series))
# 	#         renderers[series] = p.line(x="time", y=series, legend=series, source=source)
# 	#         print("renderers: {0}".format(renderers))
#
# 	print("new_data: {0}".format(new_data))
