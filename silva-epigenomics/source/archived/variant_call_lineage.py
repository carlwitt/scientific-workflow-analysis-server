"""
See if there are lineage effects in the variant calling workflows in the MongoDB.
To check, plot the duration of a invocation as a function of the duration of the ancestor task for all 28 ancestor pairs:
	(twoPassAlignment, addOrReplaceGroups), (twoPassAlignment, markDuplicates), ... (addOrReplaceGroups, markDuplicates), ... ,(printReads, variantCall)
"""
from sklearn.linear_model import RidgeCV

__author__ = 'Carl Witt'
__email__ = 'wittcarl@deneb.uberspace.de'

from numpy.random.mtrand import randint
from pymongo import MongoClient
import numpy as np
import scipy.stats

task_names = [
	"twoPassAlignment",
	"addOrReplaceReadGroups",
	"markDuplicates",
	"splitNCigarReads",
	"indelRealignment",
	"recalibrate",
	"printReads",
	"variantcall",
]


def query_invocation_data(task_name):
	"""
	Gets the runtime for a tasks invocation by workflow id (session id)
	:param task_name: The name of the task type, e.g. "variantcall" or "splitNCigarReads"
	:return: A dictionary that associates the runtime of the tasks invocation to its session id. (Each task occurs only once in the session)
	"""
	global db
	pipeline = [
		{"$match":{"data.lam_name": task_name, "data.status": "ok"}},
		{"$project":{"sid": "$session.id", "dur": "$data.info.tdur"}}
	]

	data = dict()
	for document in list(db.raw.aggregate(pipeline)):
		data[document['sid']] = document['dur']

	return data


def get_task_type_data():
	# get the invocation data for each task type
	invocation_data = dict()
	for task_name in task_names:
		invocation_data[task_name] = query_invocation_data(task_name)
		print("number of sessions containing {1}: {0}".format(len(invocation_data[task_name].keys()), task_name))

	# for each task type: assemble a result vector (duration of invocation) and a feature vector (durations of predecessor invocations)
	# skip first task, has no predecessors

	task_type_data = dict()

	for i in range(1, len(task_names)):
		task_name = task_names[i]
		invocations = invocation_data[task_name]

		session_ids = []
		predecessor_durations = []
		predecessor_names = task_names[:i]
		invocation_duration = []

		# each observation will be one row
		for session_id in invocations.keys():
			durations = []
			# for each task preceding the task for which we build a model, append the duration as column
			for predecessor in task_names[:i]:
				if session_id in invocation_data[predecessor]: durations.append(
					invocation_data[predecessor][session_id])
			# if there are missing values, drop the row
			if len(durations) == i:
				session_ids.append(session_id)
				predecessor_durations.append(durations)
				invocation_duration.append(invocations[session_id])

		task_type_data[task_name] = {
			"num_predecessors": i,
			"predecessor_names": predecessor_names,
			"session_ids": session_ids,
			"predecessor_durations": np.array(predecessor_durations),
			"invocation_duration": invocation_duration,
		}

	return task_type_data

def plot(task_type_data):
	import matplotlib.pyplot as plt
	import numpy as np

	for task_name in task_type_data.keys():

		task_data = task_type_data[task_name]
		ys = task_data['invocation_duration']

		# analyze each predecessor, successor relationship
		for predecessor_id in range(task_data['num_predecessors']):
			xs = task_data['predecessor_durations'][:,predecessor_id]
			predecessor_name = task_data['predecessor_names'][predecessor_id]

			# one plot of the raw data and one plot with log-transformed data
			for transform, independent, dependent in [("", xs, ys), ("log_", np.log(xs), np.log(ys))]:
				plt.clf()
				plt.scatter(independent, dependent)
				plt.xlabel('{0}dur {1}'.format(transform, predecessor_name))
				plt.ylabel('{0}dur {1}'.format(transform, task_name))
				plt.title('{2}{0} ~ {2}{1}'.format(task_name, predecessor_name, transform))
				plt.savefig("out/{2}{0}_{1}.png".format(task_name,predecessor_name,transform))

def regress(task_type_data):
	"""
	The interesting question is whether we can combine the durations of the tasks seen so far to improve the runtime predictions for a task.
	We'll compare predictions based on a single predecessor task with predictions based on all predecessor tasks
	For each task type, do the following:
		Report max {correlation(task type, predecessor task)}
		Report lin regression duration ~ predecessor durations
		Report ridge regression (regularization by cross validation) duration ~ predecessor durations
	:param task_type_data:
	:return:
	"""
	for task_name in task_type_data.keys():

		print("task_name: {0}".format(task_name))

		task_data = task_type_data[task_name]

		#### LOG Transform
		ys = task_data['invocation_duration']
		#### LOG Transform

		max_corr_id = 0

		# analyze each predecessor, successor relationship via pearson correlation
		cor_values = []
		for predecessor_id in range(task_data['num_predecessors']):

			#### LOG Transform
			xs = task_data['predecessor_durations'][:,predecessor_id]
			#### LOG Transform

			corr, p_value = scipy.stats.pearsonr(x=xs, y=ys)
			cor_values.append(corr)
			if abs(corr) > cor_values[max_corr_id]:
				max_corr_id = predecessor_id

		# print results
		for predecessor_id in range(task_data['num_predecessors']):
			marker = "  > " if predecessor_id == max_corr_id else "\t"
			print("{0}{1} {2:.2g}".format(marker, task_data['predecessor_names'][predecessor_id], cor_values[predecessor_id]))


		from sklearn import linear_model
		#### LOG Transform
		xs  = task_data['predecessor_durations']
		#### LOG Transform

		# reg = linear_model.RidgeCV(alphas=[10**i for i in np.linspace(-2,2,10)])
		reg = linear_model.LinearRegression()
		reg.fit(xs, ys)

		# print("\tMean squared error: %.2f" % np.mean((reg.predict(xs) - ys) ** 2))
		# Explained variance score: 1 is perfect prediction
		print('\tR^2 - ρ^2 for all best pred: %.2f' % (reg.score(xs, ys)-cor_values[max_corr_id]**2))
		# print("\t\treg.alpha: {0}".format(reg.alpha_))

def main():

	task_type_data = get_task_type_data()
	#plot(task_type_data)
	regress(task_type_data)

	# for i, ancestor_task_name in enumerate(task_names):
	# 	ancestor_data = invocation_data[ancestor_task_name]
	#
	# 	for descendant_task_name in task_names[i+1:]:
	# 		descendant_data = invocation_data[descendant_task_name]
	# 		xs = []
	# 		ys = []
	# 		for sid in ancestor_data.keys():
	# 			if sid in descendant_data.keys():
	# 				xs.append(ancestor_data[sid])
	# 				ys.append(descendant_data[sid])
	#
	#

db = MongoClient().scientificworkflowlogs
main()