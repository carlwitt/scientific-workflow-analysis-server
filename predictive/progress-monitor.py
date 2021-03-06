# A dashboard to monitor the execution of a cuneiform scientific workflow.
# Implemented as a bokeh application that displays aggregations of the log data collected in MongoDB.
#
# The idea is that this server runs on the same machine as the HTTP server that accepts incoming log data from cuneiform and writes it to a local MongoDB instance.
# By interacting with the dashboard server, the user then gets a processed view of the log data generated by her/his workflow.
#
# Start the server:
# bokeh serve progress-monitor.py --port 5100 --host 192.168.24.74:5100
# The --host attribute whitelists http requests send to this IP address and port.
#

import numpy as np

import pymongo as mng
import bson
from bson.objectid import ObjectId
from bson.json_util import loads

from bokeh.layouts import column
from bokeh.models import Button
from bokeh.palettes import RdYlBu3
from bokeh.plotting import figure, curdoc

from bokeh.models.widgets import Select
from bokeh.io import output_file, show, vform

# ============================================================================
# configuration and global variables
# ============================================================================

# MongoDB collection from which to draw log entries
mongoDbCollection = "raw"

client = mng.MongoClient('mongodb://localhost:27017/')
db = client.scientificworkflowlogs

# ============================================================================
# queries
# ============================================================================

# Retrieves sessions and overview information (a session ideally corresponds to one workflow execution).
#   - Session id (string)
#   - Number of log entries  (int)
#   - Session start timestamp (long)
# Used for providing a selection box to scope the dashboard.
def get_sessions():
    pipeline = [
        { "$group": {"_id": "$session.id",
            "tstart": {"$first": "$session.tstart"},
            "numLogEntries": {"$sum": 1}
        }},
        { "$sort": {"tstart": -1}}
    ]
    sessions = []
    for session in db[mongoDbCollection].aggregate(pipeline):
        sessions.append(session)

    return sessions

# Retrieves basic information about a session.
#   - The earliest and latest timestamp of a message to compute wall clock time.
#   - Invocation duration statistics: count, sum , maximum, average, standard deviation
# Used to provide fill the general information panel.
def get_session_statistics(session_id):
    # I didn't manage to use queries as in MongoDB, i.e. using non-escaped keys, null, etc.
    #   codes.Code didn't do it
    #   json_utils.load is just as strict as normal JSON.parse (requires quoted key identifiers)
    pipeline = [
    { "$match": { "session.id": session_id} },
    { "$group": {"_id": None,
        "firstMessageId": {"$min": "$_id"},
        "lastMessageId": {"$max": "$_id"},
        "maxInvocationDuration": {"$max": "$data.info.tdur"},
        "sumInvocationDuration": {"$sum": "$data.info.tdur"},
        "avgInvocationDuration": {"$avg": "$data.info.tdur"},
        "sdInvocationDuration": {"$stdDevSamp": "$data.info.tdur"},
        "invocations": {"$sum": 1}
    }}]

    return db[mongoDbCollection].aggregate(pipeline).next()

# Aggregates information about all invocations of a task type for a given session.
#   - Invocation durations: count, minimum, maximum, sum, average, standard deviation
# The sum is used to compute the overall share of total compute time per task, as displayed in the bottleneck/time share visualization.
def get_task_type_statistics(session_id):
    pipeline = [
        { "$match": {"session.id": session_id}},
        { "$group": {"_id": "$data.lam_name",
            "minDur": {"$min": "$data.info.tdur"},
            "maxDur": {"$max": "$data.info.tdur"},
            "sumDur": {"$sum": "$data.info.tdur"},
            "avgDur": {"$avg": "$data.info.tdur"},
            "sdsDur": {"$stdDevSamp": "$data.info.tdur"},
            "invocations": {"$sum": 1}
            }},
        { "$sort": {"tstart": -1}}
    ]
    stats = []
    for stat in db[mongoDbCollection].aggregate(pipeline):
        stats.append(stat)

    return stats


# Returns the start and stop messages sorted by arrival time (at database) per task type
# Used to visualize the number of running invocations per task type over time.
def get_invocation_lifecycle_events_per_task_type(session_id):
    pipeline = [
        { "$match": {"session.id": session_id}},
        { "$sort":  {"_id": 1}},       			    # order log entries by arrival time at database
        { "$group": {"_id": "$data.lam_name", 		# group by task type
            "data": {"$push": {"time": "$_id", "type": "$data.status"}},  # for each task type
        }},
    ]

    events = []
    for task_type in db[mongoDbCollection].aggregate(pipeline):
        events.append(task_type)

    return events

# Converts the output of get_invocation_lifecycle_events_per_task_type to time series comprehensible to bokeh plots
def events_to_counts(lifecycle_events):
    for task_type in lifecycle_events:
        time = [0]
        count = [0]
        for event in task_type['data']:
            time.append(str(event['time']))
            if(event['type'] == "ok"):
                count.append(count[-1] - 1)
            else:
                count.append(count[-1] + 1)
        print time
        print count
# print(get_sessions())
# print(get_session_statistics("9985004919"))
# print(get_task_type_statistics("9985004919"))
# print(get_invocation_lifecycle_events_per_task_type("9985004919"))
print(events_to_counts(get_invocation_lifecycle_events_per_task_type("9985004919")))
# ============================================================================
# plot set up
# ============================================================================

# create a plot and style its properties
p = figure(x_range=(0, 100), y_range=(0, 100), toolbar_location=None)
p.border_fill_color = 'black'
p.background_fill_color = 'black'

# add a text renderer to out plot (no data yet)
r = p.text(x=[], y=[], text=[], text_color=[], text_font_size="20pt",
           text_baseline="middle", text_align="center")


# select = Select(title="Session:", value="foo", options=["get_sessions()", "abc"])

# ============================================================================
# plot interaction
# ============================================================================

i = 0
ds = r.data_source
# create a callback that will add a number in a random location
def callback():
    global i
    ds.data['x'].append(np.random.random()*70 + 15)
    ds.data['y'].append(np.random.random()*70 + 15)
    ds.data['text_color'].append(RdYlBu3[i%3])
    ds.data['text'].append(str(i))
    ds.trigger('data', ds.data, ds.data)
    i = i + 1

# ============================================================================
# plot composition
# ============================================================================

# add a button widget and configure with the call back
button = Button(label="Press Me")
button.on_click(callback)

# put the button and plot in a layout and add to the document
curdoc().add_root(column(button, p)) #vform(select),
