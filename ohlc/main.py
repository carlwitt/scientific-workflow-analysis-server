import datetime
from bokeh.models.axes import DatetimeAxis
from numpy import asarray, cumprod, convolve, exp, ones
from numpy.random import lognormal, gamma, uniform

from bokeh.layouts import row, column, gridplot
from bokeh.models import ColumnDataSource, Slider, Select
from bokeh.plotting import curdoc, figure
from bokeh.charts import Area
from bokeh.driving import count
from numpy.random.mtrand import randint
from bokeh.client import push_session
import time

# brewer palette "paired"
Paired12 = ['#a6cee3', '#1f78b4', '#b2df8a', '#33a02c', '#fb9a99', '#e31a1c', '#fdbf6f', '#ff7f00', '#cab2d6', '#6a3d9a', '#ffff99', '#b15928']

# =====================================================================================================================
# Methods
# =====================================================================================================================

# builds an update dict that can be used in ColumnDataSource.stream
# takes the new values for each column in the order specified by columns
def pack_new_data(values = None):
    result = {}
    if values is None:
        # add an empty list for each column
        for column in columns: result[column] = []
        return result
    else:
        for i, column in enumerate(columns):
            if type(values[i]) is list:
                result[column] = values[i]
            else:
                result[column] = [values[i]]
        return result

# Returns a list of new values for each column
# t is just taken as is
# new values for data series are generated randomly
# sum is computed
def generate_new_data(t):
    returns = []
    for c, task_type in enumerate(data_series):
        last_val = 0 if t==0 else source.data[task_type][-1] - ( 0 if c == 0 else source.data[data_series[c-1]][-1])
        new_value = max(0, last_val + randint(-2,2))
        returns.append(new_value + (0 if len(returns) == 0 else returns[-1]))
    #datetime.datetime.fromtimestamp(time.time() + t*1000) doesn't work (axis seems to require something else, the timestamps are o.k.)
    return [t, returns[-1]] + returns

def add_series(series_name):
    global data_series
    global columns
    global source
    global p

    # assign colors in a round-robin fashion
    color = Paired12[len(data_series) % len(Paired12)]
    # add
    data_series = data_series + [series_name]
    columns = base_columns + data_series

    # add to column data set with all rows set to zero
    source.add([0] * len(source.data["time"]), series_name)
    # p.line(x='time', y=series_name, legend=series_name, alpha=0.8, line_width=2, color=color, source=source)
    y0 = 0 if len(data_series)==1 else data_series[-2]
    print("y0: {0}".format(y0))
    p.segment(x0='time', x1='time', y0=y0, y1=series_name, legend=series_name, alpha=0.8, line_width=2, color=color, source=source)

# =====================================================================================================================
# Globals and Configuration
# =====================================================================================================================

PLOT_WIDTH = 1500
base_columns = ["time", "sum"]
data_series = []
columns = base_columns + data_series
source = ColumnDataSource(pack_new_data([0, 0, 8]))

# =====================================================================================================================
# Controls
# =====================================================================================================================

# control elements
interval = Slider(title="interval", value=0, start=0, end=1000, step=1)
stddev = Slider(title="stddev", value=0.04, start=0.01, end=0.1, step=0.01)
mavg = Select(value='abc', options=['123', 'abc', "xyz", "ema"])


# =====================================================================================================================
# Plots
# =====================================================================================================================

# the upper plot
p = figure(plot_height=500,
           tools="xpan,xwheel_zoom,xbox_zoom,reset",
           x_axis_type="datetime", y_axis_location="left", webgl=True) #x_axis_type=None
p.x_range.range_padding = 0
p.x_range.follow = "end"
p.x_range.follow_interval = None if interval.value == 0 else interval.value

# p.line(x='time', y='diffit', legend='diffit', alpha=0.2, line_width=3, color='navy', source=source)
# p.line(x='time', y='project', legend='project', alpha=0.8, line_width=2, color='orange', source=source)
# p.line(x='time', y='merge', legend='merge', alpha=0.8, line_width=2, color='red', source=source)
# p.line(x='time', y='sum', legend='sum', alpha=0.8, line_width=2, color='gray', source=source)

# candle stick stuff
# p.segment(x0='time', y0='low', x1='time', y1='high', line_width=2, color='black', source=source)
# p.segment(x0='time', y0='open', x1='time', y1='close', line_width=8, color='color', source=source)

# the lower plot
# time series
p2 = figure(plot_height=100, x_range=p.x_range, tools="xpan,xwheel_zoom,xbox_zoom,reset", y_axis_location="right")
# p2.line(x='time', y='sum', color='gray', source=source)

data = dict(
    python=[2, 3, 7, 5, 26, 221, 44, 233, 254, 265, 266, 267, 120, 111],
    pypy=[12, 33, 47, 15, 126, 121, 144, 233, 254, 225, 226, 267, 110, 130],
    jython=[22, 43, 10, 25, 26, 101, 114, 203, 194, 215, 201, 227, 139, 160],
)
area2 = Area(source.data, title="Stacked Area Chart", legend="top_left", stack=True, xlabel='time', ylabel='memory')
# dict(diffit=source.data['diffit'],project=source.data['project']

# the bar in the lower plot
# p2.segment(x0='time', y0=0, x1='time', y1='macdh', line_width=6, color='black', alpha=0.5, source=source)


# =====================================================================================================================
# Main
# =====================================================================================================================

# prepare document
curdoc().add_root(column(row(interval, stddev, mavg), gridplot([[p], ], toolbar_location="right", plot_width=1500))) # [p2], [area2]

add_series("project")
add_series("diffit1")
add_series("merge")
add_series("background")
add_series("normalize")
add_series("stitch")
add_series("write")
add_series("read")
add_series("gather")
add_series("gather")


# The body of the update loop (see add_periodic_callback below)
@count()
def update(t):

    # new_series = []
    # if t == 3: new_series = ["diffit1"]
    # if t == 6: new_series = ["merge"]
    # if t == 9: new_series = ["project"]
	#
    # for series in new_series:
    #     add_series(series)

    # generates new data
    new_data = generate_new_data(t)

    print("new_data: {0}".format(new_data))
    rollover = None if interval.value == 0 else interval.value

    # Update the data by sending only the new data points
    if(randint(0,10)>-1):
        source.stream(pack_new_data(new_data), rollover)
    else:
        source.stream(pack_new_data(None), rollover)

    # global area2
    # print("source.data: {0}".format(source.data))
    # area2 = Area(source.data, title="Stacked Area Chart", legend="top_left", stack=True, xlabel='time', ylabel='memory')

# sets up the main loop
session = push_session(curdoc())

curdoc().add_periodic_callback(update, 50)
curdoc().title = "Session Dashboard"

session.loop_until_closed()