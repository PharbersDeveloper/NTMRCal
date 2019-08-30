
library(SparkR)
library(uuid)
library(BPRSparkCalCommon)


#' UCB Calculation
#' @export
UCBRun <- function(...) {
    cmd_args = list(...)
    ss <- sparkR.session(appName = "UCB-Submit")
    TMUCBCalProcess(
        cal_data_path = cmd_args[1],
        weight_path = cmd_args[2],
        curves_path = cmd_args[3],
        competitor_path = cmd_args[4]
    )
}
