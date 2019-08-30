library(SparkR)
library(uuid)
library(BPRSparkCalCommon)

TMCalCurveSkeleton3 <- BPRSparkCalCommon::TMCalCurveSkeleton3
curve_func <- BPRSparkCalCommon::curve_func

cmd_args = commandArgs(T)
if (cmd_args[1] == "UCB") {
    source("TMUCBCalProcess.R")
    ss <- sparkR.session(appName = "UCB-Submit")
    TMUCBCalProcess(
        cal_data_path = cmd_args[2],
        weight_path = cmd_args[3],
        curves_path = cmd_args[4],
        competitor_path = cmd_args[5]
    )
} else if (cmd_args[1] == "NTM") {
    source("TMCalProcess.R")
    ss <- sparkR.session(appName = "NTM-Submit")
    TMCalProcess(
        cal_data_path = cmd_args[2],
        weight_path = cmd_args[3],
        manage_path = cmd_args[4],
        curves_path = cmd_args[5],
        competitor_path = cmd_args[6],
        standard_time_path = cmd_args[7],
        level_data_path = cmd_args[8],
    )
}
