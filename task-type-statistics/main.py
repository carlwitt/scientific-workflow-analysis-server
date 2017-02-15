'''

'''
from collections import OrderedDict
from datetime import time, datetime
from itertools import cycle

import numpy as np
from bokeh.layouts import row, column
from bokeh.models import ColumnDataSource, Slider, Div, Dropdown, SingleIntervalTicker, AdaptiveTicker, HoverTool
from bokeh.models.axes import LinearAxis
from bokeh.models.layouts import WidgetBox
from bokeh.plotting import curdoc, figure
from pymongo import MongoClient
from scipy.stats import norm
from scipy.stats import expon
from scipy.stats import lognorm

# brewer palette "paired"
Paired12 = ['#a6cee3', '#1f78b4', '#b2df8a', '#33a02c', '#fb9a99', '#e31a1c', '#fdbf6f', '#ff7f00', '#cab2d6',
			'#6a3d9a', '#ffff99', '#b15928']

TimeScaleColors = {'hours': '#e41a1c','minutes': '#377eb8', 'seconds': '#4daf4a'}

# =====================================================================================================================
# Business Logic Methods
# =====================================================================================================================


# =====================================================================================================================
# User Interface Methods
# =====================================================================================================================


# =====================================================================================================================
# Globals and Configuration
# =====================================================================================================================

# the database connection
db = MongoClient().scientificworkflowlogs


# =====================================================================================================================
# Controls
# =====================================================================================================================


# =====================================================================================================================
# Plots
# =====================================================================================================================

# the main plot, visualizes the number of running tasks per task type over time

# =====================================================================================================================
# Main
# =====================================================================================================================


from datetime import datetime as dt
import time

from bokeh.sampledata.daylight import daylight_warsaw_2013
from bokeh.plotting import figure, show, output_file
from bokeh.models import Span

db = MongoClient().scientificworkflowlogs

pipeline = [
    {"$match": {
        # "session.id":{"$in":["20160831T051239+0000", "20160831T050311+0000"]},
        "data.info.tdur":{"$exists":True}
        }},
    {"$group": {
        "_id": "$data.lam_name",
        "count": {"$sum": 1},
        "mean_duration": {"$avg": "$data.info.tdur"},
        "sd_duration": {"$stdDevSamp":"$data.info.tdur"},
        "data": { "$push": {
            "session_id":"$session.id",
            "duration":{"$divide": ["$data.info.tdur", 1000]},
         }},
     }},
    {"$sort": {"_id":1}},
    {"$match": {"count": {"$gt":1}}},
	# {"$limit": 15}
]

plots = []
for task_stats in db.raw.aggregate(pipeline):

	# sort by duration
	sorted_data = sorted(task_stats['data'], key=lambda d: d['duration'])

	# compute the cdf
	n = len(sorted_data)
	# the empirical probability of falling below a given value is the fraction of observations below that value
	for i, datum in enumerate(sorted_data): datum['cdf'] = float(i) / n

	# adapt time scale
	mean_duration = task_stats['mean_duration'] / 1000.0	# mean duration is now in seconds
	sd_duration = task_stats['sd_duration'] / 1000.0  		# standard deviation is now in seconds

	if mean_duration > 3600:
		for d in sorted_data:
			d['duration'] = d['duration'] / 3600.0
			d['color'] = TimeScaleColors['hours']
		mean_duration = mean_duration / 3600.0
		sd_duration = sd_duration / 3600.0
		scale_text = "hours"
	elif mean_duration > 60:
		for d in sorted_data:
			d['duration'] = d['duration'] / 60.0
			d['color'] = TimeScaleColors['minutes']
		mean_duration = mean_duration / 60.0
		sd_duration = sd_duration / 60.0
		scale_text = "minutes"
	else:
		for d in sorted_data:
			d['color'] = TimeScaleColors['seconds']
		scale_text = "seconds"

	# compute reference log normal distribution
	log_transformed_durations = list(map(lambda d: np.log(d['duration']), sorted_data))
	log_transformed_mean = np.mean(log_transformed_durations)
	log_transformed_sd = np.std(log_transformed_durations)
	reference_x = np.linspace(sorted_data[0]['duration'], sorted_data[-1]['duration'], 50)		# evenly spaced support between minimum and maximum duration
	reference_cdf = [norm.cdf((np.log(value)-log_transformed_mean)/log_transformed_sd) for value in reference_x]		# lookup cdf of the z-scores

	# compute reference exponential distribution
	# loc, scale = expon.fit(list(map(lambda d: d['duration'], sorted_data)))
	# ref_expon = expon(loc=loc, scale=scale)
	# reference_cdf2 = [ref_expon.cdf(value) for value in reference_x]

	# create the plot
	p = figure(y_axis_location="right", tools="box_zoom,save,hover,reset,pan,wheel_zoom") # x_axis_type="time"

	# compute a color for each session id
	session_ids = list(map(lambda d: d['session_id'], sorted_data))
	# session_colors = {}
	# unique_session_ids = set(session_ids)
	# for i, sid in enumerate(unique_session_ids):
	# 	session_colors[sid] = Paired12[i%(len(Paired12))]

	# create a data source for the observations (to allow tool tips on hover)
	source = ColumnDataSource({
		'duration': list(map(lambda d: d['duration'], sorted_data)),
		'cdf': list(map(lambda d: d['cdf'], sorted_data)),
		'session_id': session_ids,
		'color': list(map(lambda d: d['color'], sorted_data)),
		# 'color': ['navy'] * n if len(unique_session_ids) > 15 else list(map(lambda d: session_colors[d['session_id']], sorted_data))
	})

	# mark the first, second, and third quartile
	quartile_indices = [int(len(sorted_data) * i) for i in [0.25, 0.5, 0.75]]
	quartiles_x = [sorted_data[idx]['duration'] for idx in quartile_indices]
	quartiles_y = [sorted_data[idx]['cdf'] for idx in quartile_indices]
	p.circle(quartiles_x, quartiles_y, size=10, color=["navy", "red", "navy"], alpha=0.5)

	# draw the reference CDF
	p.line(reference_x, reference_cdf, line_dash='dotted', line_width=1)
	# p.line(reference_x, reference_cdf2, line_dash='dashed', line_width=1)

	# draw the observations
	p.cross(source=source, x='duration', y='cdf', color='color', line_width=2)

	# add plot labels
	p.title.text = "%s (%s observations)" % (task_stats['_id'], n)
	p.xaxis.axis_label = 'duration [%s]' % scale_text
	p.yaxis.axis_label = 'P(x ≤ X)'

	hover = p.select_one(HoverTool)
	hover.point_policy = "follow_mouse"
	hover.tooltips = [
		("Session", "@session_id"),
		("Duration [%s]"%scale_text, "@duration"),
		("P(dur ≤ X)", "@cdf")]

	plots.append(p)




figure_explanation = Div(text="Each plot shows the empirical cumulative distribution function (CDF) of runtime for a task type. <br/>"
							  "The median is marked in red and the first and third quantile are marked in navy.<br/>"
							  "The dashed line shows the CDF of a log-normal fitted to the sample. <br/>"
							  "Color indicates the time scale: "
							  "<span style='margin-right:3px;background-color:#4daf4a; display: inline-block; width:12px; height:12px;'></span>seconds "
							  "<span style='margin-right:3px;background-color:#377eb8; display: inline-block; width:12px; height:12px;'></span>minutes "
							  "<span style='margin-right:3px;background-color:#e41a1c; display: inline-block; width:12px; height:12px;'></span>hours", width=800)

from bokeh.layouts import gridplot
layout = column(gridplot(plots, ncols=3, plot_width=500, plot_height=250), figure_explanation)

curdoc().add_root(layout)
curdoc().title = "Task Data Overview"

# prepare document
# layout = column(
# 	row(WidgetBox(dropdown, width=405, height=100)),
# 	row(wallClockTime, numMessages), #cumulativeTime
# 	p,
# 	manualLegendBox,
# 	#limit,
# )
# curdoc().add_root(layout)
# curdoc().title = "Session Dashboard"
#
# # add periodical polling of the database for new data (pushes to the client via websockets)
# curdoc().add_periodic_callback(lambda: select_session(current_session), 150)









