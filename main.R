library(SparkR)
library(uuid)
library(BPRSparkCalCommon)
library(BPRUCBCal)

source("TMUCBCalProcess.R")

TMCalCurveSkeleton3 <- BPRSparkCalCommon::TMCalCurveSkeleton3
curve_func <- BPRSparkCalCommon::curve_func

cmd_args = commandArgs(T)
if (cmd_args[1] == "UCB") {
    ss <- sparkR.session(appName = "UCB-Submit")
    TMUCBCalProcess(
        cal_data_path = cmd_args[2],
        weight_path = cmd_args[3],
        curves_path = cmd_args[4],
        competitor_path = cmd_args[5]
    )   
}
