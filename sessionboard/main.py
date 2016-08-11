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
from bokeh.client import pull_session
from bokeh.models.axes import DatetimeAxis

from bokeh.layouts import row, column, gridplot
from bokeh.models import ColumnDataSource, Slider, Select, Div, Button, Dropdown, CustomJS
from bokeh.models.layouts import WidgetBox
import numpy as np
from bokeh.plotting import curdoc, figure
from bokeh.charts import Area
from bokeh.driving import count, linear
from numpy.random.mtrand import randint
from bokeh.client import push_session
from datetime import time, datetime
from pymongo import MongoClient
from bson.objectid import ObjectId

# brewer palette "paired"
Paired12 = ['#a6cee3', '#1f78b4', '#b2df8a', '#33a02c', '#fb9a99', '#e31a1c', '#fdbf6f', '#ff7f00', '#cab2d6',
			'#6a3d9a', '#ffff99', '#b15928']

# =====================================================================================================================
# Business Logic Methods
# =====================================================================================================================

'''
Retrieves a list of all sessions with start time, number of messages and session id.
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
	return list(db.raw.aggregate(pipeline))

def get_elapsed_time():
	print(datetime.now())

'''
Like query_running_tasks_history, but outputs polygons that are stacked.
'''
def query_running_tasks_history_stacked(session_id):
	print("query running tasks for session_id: {0}".format(session_id))
	print("query running tasks current_limit: {0}".format(current_limit))

	#
	# 1. query distinct task types in the session, ordered alphabetically
	#

	task_types_pipeline = [
		{"$match": {"session.id": session_id}},
		{"$sort": {"_id": 1}},  # order log entries by arrival time at database
		{"$limit": current_limit},
		{"$group": {"_id": "$data.lam_name"}},  # group by task type
		{"$sort": {"_id": 1}},		# give task types in alphabetical order
	]

	# create a task types array that defines the stacking order (bottom to top) of the patches in the chart
	task_type_names = [doc["_id"] for doc in list(db.raw.aggregate(task_types_pipeline))]



	# task_type_names = ["A", "B"]




	# print("task_type_names: {0}".format(task_type_names))

	# add to task types map (stores the associated color) if the task type hasn't been seen before
	for name in task_type_names:
		if not task_types.has_key(name):
			color = Paired12[len(task_types.keys()) % 12]
			task_types[name] = {'color': color}

	#
	# 2. query for invocation lifecycle events (started, ok), in chronological order
	#
	events_pipeline = [
		{"$match": {"session.id": session_id}},
		{"$sort": {"_id": 1}},  # order log entries by arrival time at database
		{"$limit": current_limit},
		{"$project": {"task_type": "$data.lam_name",  # group by task type
					  "time": "$_id",
					  "lifecycle_event": "$data.status"}},  # for each task type
	]

	events_chronological = list(db.raw.aggregate(events_pipeline))

	# t0 = ObjectId.from_datetime(datetime(2010, 1, 1))
	# t1 = ObjectId.from_datetime(datetime(2011, 1, 1))
	# t1 = ObjectId.from_datetime(datetime(2011, 1, 1))
	# t2 = ObjectId.from_datetime(datetime(2012, 1, 1))
	# t3 = ObjectId.from_datetime(datetime(2013, 1, 1))
	# t4 = ObjectId.from_datetime(datetime(2014, 1, 1))
	# t4 = ObjectId.from_datetime(datetime(2014, 1, 1))
	# t5 = ObjectId.from_datetime(datetime(2015, 1, 1))
	# t6 = ObjectId.from_datetime(datetime(2016, 1, 1))
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
	first_timestamp = events_chronological[0]['time'].generation_time if len(events_chronological) > 0 else 0
	data = {"xss": [ [ first_timestamp ] for _ in task_type_names],
			"yss": [ [ 0 ] for _ in task_type_names],
			"colors": [task_types[name]['color'] for name in task_type_names]}

	# stores the number of currently running tasks for each task type.
	# same order as defined by task_type_names, so the i-th element corresponds to the series task_type_names[i]
	previous = [0] * len(task_type_names)

	# for each event add a new data point to all series (represented as patches to get the area below them shaded)
	for event in events_chronological:

		# silently skip unknown lifecycle events
		if event['lifecycle_event'] not in [u'started', u'ok']: continue

		for i, tt in enumerate(task_type_names):

			# this done through the next step anyway
			# add an additional first data point with 0 task running. not strictly necessary but lines that start "in the air" look strange.
			# if len(data['xss'][i]) == 0:
			# 	data['xss'][i].append(event['time'].generation_time)
			# 	data['xss'][i].append(0)

			# add an (new x, previous y) pair to get step like segments instead of slopes.
			# data['xss'][i].append(event['time'].generation_time)
			# data['yss'][i].append(previous[i])

			# add new (x, y) pair
			data['xss'][i].append(event['time'].generation_time)

			# the number of previously running tasks of task type tt is the last y value (cumulative) minus the last y value of the series below
			#
			# previously_running = data['yss'][i][-1] - (data['yss'][i-1][-2] if i > 0 else 0)
			running = previous[i]
			if event['task_type'] == tt:
				if event['lifecycle_event'] == u"started": running += 1
				if event['lifecycle_event'] == u"ok": running -= 1

			# convert to a cumulative value by adding the cumulative number of running tasks of types below tt.
			# data['yss'][i-1][-1] is the current time, because we've added the value in the last loop iteration, and thus len(data['yss'][i-1]) = len(data['yss'][i]) + 1
			sum_below = data['yss'][i-1][-1] if i > 0 else 0
			cumulative = running + sum_below
			data['yss'][i].append(cumulative)
			previous[i] = running

		# if we set all previous values at once, after having used them to compute the next step, we don't need an extra buffer to differentiate between actual and cumulated values?
		# previous = [data['yss'][i] for i in range(len(task_type_names))]

	# after having added all the values, close the polygon by setting it to the upper border of the polygon below (in stacking order)
	# start at topmost patch because nobody accesses this patches surface after this (because we alter it now)
	for i in range(len(task_type_names)-1, -1, -1): #list(reversed(list(enumerate(task_type_names)))):
		data['xss'][i].extend(list(reversed(data['xss'][i])))
		upper_border_previous_polygon = list(reversed(data['yss'][i-1])) if i > 0 else list(np.zeros(len(data['xss'][i])))
		data['yss'][i].extend(upper_border_previous_polygon)

	# print('legends:' + str(data['legends']))
	# print("task_type_names: {0}".format(task_type_names))
	# print('ys:' + str(data['yss']))
	# print('xs:' + str(data['xss']))

	return data

'''
Retrieves the invocation lifecycle events (started, ok) from the database, in chronological order and grouped by task.
Returns data in a format that is understood by the multiline and patches renderer.
This results in overlapping patches, which is visually not very pleasing.
'''
def query_running_tasks_history(session_id):
	print("query running tasks for session_id: {0}".format(session_id))
	print("query running tasks current_limit: {0}".format(current_limit))
	# query for invocation lifecycle events (started, ok), in chronological order and grouped by task.
	pipeline = [
		{"$match": {"session.id": session_id}},
		{"$sort": {"_id": 1}},  # order log entries by arrival time at database
		{"$limit": current_limit},
		{"$group": {"_id": "$data.lam_name",  # group by task type
					"events": {"$push": {"time": "$_id", "type": "$data.status"}},  # for each task type
					}},
	]

	# the result data structure
	data = {"xs": [], "ys": [], "legends": [], "colors": []}

	events_by_tasktype = list(db.raw.aggregate(pipeline))

	# for each task type create one array with x values, one array with y values, a legend label and a color.
	# the arrays and scalar values form a single row in the column data set (although the values are lists)
	for tasktype in events_by_tasktype:

		# get task type's name
		name = tasktype['_id']

		# add to task types map if the task type hasn't been seen before
		if not task_types.has_key(name):
			color = Paired12[len(task_types.keys())%12]
			task_types[name] = {'color': color}

		xss = []
		yss = []
		color = task_types[name]['color']

		previous = 0
		for event in tasktype['events']:

			# silently skip unknown lifecycle events
			if event['type'] not in [u'started', u'ok']: continue

			# add an additional first data point with 0 task running. not strictly necessary but lines that start "in the air" look strange.
			if len(xss) == 0:
				xss.append(event['time'].generation_time)
				yss.append(0)

			# add an (new x, old y) pair to get a step segments instead of slopes.
			xss.append(event['time'].generation_time)
			yss.append(previous)

			# add new (x, y) pair
			xss.append(event['time'].generation_time)

			newValue = previous
			if event['type'] == u"started": newValue += 1
			if event['type'] == u"ok": newValue -= 1
			yss.append(newValue)
			previous = newValue

		# push a closer value at (last x, 0) to avoid self-intersecting polygons
		if len(xss) > 0:
			xss.append(xss[-1])
			yss.append(0)


		data['xs'].append(xss)
		data['ys'].append(yss)
		data['legends'].append(name)
		data['colors'].append(color)

	# print('legends:' + str(data['legends']))
	# print('xs:' + str(data['xs']))
	# print('ys:' + str(data['ys']))

	return data

# =====================================================================================================================
# User Interface Methods
# =====================================================================================================================

def add_prefix(session_str):
	return "s_" + str(session_str)
def rem_prefix(session_str):
	return session_str[2:] if session_str.startswith("s_") else session_str

def select_session(session_id):
	p.title.text = "Session " + session_id
	global source
	global current_limit
	global current_session
	global task_types
	current_limit += 1
	if session_id != current_session:
		task_types = {}
		current_limit = 1
		current_session = session_id
	source.data = query_running_tasks_history_stacked(session_id)

# Used to generate the labels in the session dropdown box
def session_format(session_id, numMessages, timestamp):
	return "%s id:%s (%s message%s)" % (timestamp, session_id, numMessages, "s" if int(numMessages) > 1 else "")


# Used to generate the contents in the infobox divs that display the number of messages, etc.
def infoBoxFormat(label, value):
	# margin-left: 13px;
	return """
			<div style="margin-right: 13px; padding: 20px; border: 1px solid rgb(204, 204, 204); border-radius: 9px; text-align: center; height: 50px; font-size: 19px; background-color: rgb(244, 244, 244);">
				%s<br/>
				%s
			</div>""" % (label, value)


# Tables have highlighted headers, not consistent with the other boxes
# def timeStatsFormat(wallclock, cumulative):
#     return """
#         <div style="padding: 20px; border: 1px solid rgb(204, 204, 204); border-radius: 9px; text-align: center; height: 50px; font-size: 19px; background-color: rgb(244, 244, 244);">
#             <table><tr><th>Wall<br></th><th>Cumulative<br></th></tr><tr><td>%s<br></td><td>%s<br></td></tr></table>
#         </div>"""%(wallclock, cumulative)
# =====================================================================================================================
# Globals and Configuration
# =====================================================================================================================

current_limit = 1
current_session = "525225844959"

# the database connection
db = MongoClient().scientificworkflowlogs

PLOT_WIDTH = 1500

source = ColumnDataSource({"xss": [], "yss": [], "colors": []})

# the glyph renderers associated to each data series. could be turned into a property of the elements of the data_series list
renderers = {}

task_types = {}
# =====================================================================================================================
# Controls
# =====================================================================================================================


''' Session ids need to be prefixed (because the stupid dropdown widget (or some other one) can't handle integers where it expects strings and some conversion makes the string an integer). '''
session_menu = [(session_format(s['_id'], s['numLogEntries'], s['tstart']), add_prefix(s['_id'])) for s in query_sessions()]  # (session_format(id, nm, ts), id) for  , nm, ts in [("1203fafai33fe", 1202, "3.9.2016 08:21"), ("2103fafae433fe", 302, "4.9.2016 18:21"), ("120ae32dai33fe", 2, "4.9.2016 19:21")]*50 ] # use None for separator
dropdown = Dropdown(label="Session", button_type="warning", menu=session_menu)
def dropdown_change(newValue):
	print("newValue: {0}".format(newValue))
	select_session(rem_prefix(newValue))
dropdown.on_click(dropdown_change)

# control elements
limit = Slider(title="Limit", value=50, start=1, end=500, step=1)
def update(attr, old, new):
	global current_limit
	current_limit = new
	select_session(current_session)
limit.on_change('value', update)

last = time()
numMessages = Div(text=infoBoxFormat("Log Messages", 0), width=200, height=100)
wallClockTime = Div(text=infoBoxFormat("Elapsed Time (wall)", str(last)), width=400, height=100)
cumulativeTime = Div(text=infoBoxFormat("Elapsed Time (parallel)", last.isoformat()), width=400, height=100)

# =====================================================================================================================
# Plots
# =====================================================================================================================

p = figure(plot_height=300, plot_width=1500,
		   tools="xpan,xwheel_zoom,xbox_zoom,reset", # toolbar_location="right", #this is ignored for some reason.
		   x_axis_type="datetime", y_axis_location="right", # y_axis_type="log",
		   webgl=True)
p.x_range.range_padding = 0
p.legend.location = "top_left"

multiline = p.patches(xs="xss", ys="yss", color="colors", source=source, line_width=2, alpha=0.7) # legend="legends" doesn't work, it's just one logical element
# renderers['merge'] = p.line(x="time", y="merge", legend="merge", source=source)


# =====================================================================================================================
# Main
# =====================================================================================================================

source.data = query_running_tasks_history_stacked(current_session)

layout = column(
	row(WidgetBox(dropdown, width=405, height=100)),
	row(wallClockTime, cumulativeTime, numMessages),
	limit,
	p)  # gridplot([[create_figure()],], legend="top_left", toolbar_location="right", plot_width=1500))

# prepare document
curdoc().add_root(layout)  # [p2], [area2]
curdoc().title = "Session Dashboard"


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
