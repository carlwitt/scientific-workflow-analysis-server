"""
	An online learner, even if it only returns the mean of the values seen so far, is difficult to evaluate because its performance depends on the input order.
	These simple experiments show how the performance of a mean predictor varies based on different permutations of an input sequence.
"""

__author__ = 'Carl Witt'
__email__ = 'wittcarl@deneb.uberspace.de'

import numpy as np
import matplotlib.pyplot as plot

def predict(history):
	return 0 if len(history) == 0 else np.mean(history)

# a favorable example. works well in the beginning, a few heavy errors in the end (although the large number of observations seen so far prevents sufficiently quick changes)
sequence1 = [0] * 100 + [100]*5
# an unfavorable example, because the predictor will never adapt completely and adaption slows down quickly
sequence2 = [100,0,100,0,100,0,100,0,100] + [0] * 95
# a mixed example, usually performing somewhere in between the favorable and unfavorable one
sequence3 = [0]*105
for i in range(5): sequence3[np.random.randint(0,105)] = 100

# calculate predictions and errors on input sequences
for (sequence, label) in [(sequence1, 'optimistic %.1f'), (sequence2, 'pessimistic %.1f'), (sequence3, 'random %.1f')]:
	history = []
	errors = []
	print("sequence: {0}".format(sequence))
	for value in sequence:
		errors.append( abs( predict(history) - value) )
		history.append(value)
	print("error: {0}".format(errors))
	total_error = sum(errors)
	print("sum(errors): {0}".format(total_error))
	plot.plot(errors, label=label%total_error)

# show results
plot.xlabel("prediction index")
plot.ylabel("absolute error abs(predicted-actual)")
plot.legend(loc='best')
plot.title("Input order affects performance (area under the curve)")
plot.show()
