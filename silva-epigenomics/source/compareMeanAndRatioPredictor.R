# rough (and optimistic) estimation of the performance of two predictors
# the first uses the ratio target_metric/independent_variable to explain the observations
# the second uses simply the mean of the target metric to explain the observations

dataset <- ilmn
predictor <- 'insize'
metric <- 'total_time_s'
# (an optimistic estimate of) the mean error of the input size predictor
ratio <- mean(dataset[,metric] / dataset[,predictor])
ratioprederr <- abs(dataset[,metric] - ratio * dataset[,predictor]) / dataset[,metric]
ratiopredperf <- mean(ratioprederr)
print(ratiopredperf)
# (on optimistic estimate of) the mean error of the mean predictor
themean <- mean(dataset[,metric])
meanprederr <- abs(dataset[,metric] - themean) / dataset[,metric]
meanpredperf <- mean(meanprederr)
print(meanpredperf)
