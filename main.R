library(httr)
library(jsonlite)
library(SparkR)
library(uuid)
library(BPRSparkCalCommon)
library(RKafkaProxy)

TMCalCurveSkeleton3 <- BPRSparkCalCommon::TMCalCurveSkeleton3
curve_func <- BPRSparkCalCommon::curve_func

cmd_args = commandArgs(T)

JobID <- cmd_args[2]
proposalId <- cmd_args[3]
projectId <- cmd_args[4]
periodId <- cmd_args[5]
phase <- cmd_args[6]

ss <- sparkR.session(appName = "UCB-Submit")
job_scala_proxy <- sparkR.newJObject("com.pharbers.CallJMethod.BPTMProxy.TMProxy")
tmpId <- sparkR.callJMethod(job_scala_proxy, "BPTMUCBPreCal", 
                            proposalId,
                            projectId,
                            periodId,
                            as.numeric(phase))

if (cmd_args[1] == "UCB") {
    output_dir <- paste0("hdfs://192.168.100.137:9000/tmtest0831/jobs/", tmpId, "/input/")
	#PushMessage(list("JobId" = JobId, "Status" = "Running", "Message" = "", "Progress" = "0"))
    source("TMUCBCalProcess.R")
    TMUCBCalProcess(
        cal_data_path =  paste0(output_dir, "cal_data"), #cmd_args[2],
        weight_path = cmd_args[7],
        curves_path = cmd_args[8],
        competitor_path = paste0(output_dir, "cal_comp"), #cmd_args[5]
        jobid = tmpId,
        proposalid = proposalId,
        projectid = projectId,
        periodid = periodId
    )
} else if (cmd_args[1] == "NTM") {
	JobId <<- cmd_args[9]
	PushMessage(list("JobId" = JobId, "Status" = "Running", "Message" = "", "Progress" = "0"))
    source("TMCalProcess.R")
    TMCalProcess(
        cal_data_path = cmd_args[2],
        weight_path = cmd_args[3],
        manage_path = cmd_args[4],
        curves_path = cmd_args[5],
        competitor_path = cmd_args[6],
        standard_time_path = cmd_args[7],
        level_data_path = cmd_args[8],
        jobid = JobId
    )
}

tmpId <- sparkR.callJMethod(job_scala_proxy, "BPTMUCBPostCal",
                            tmpId,
                            proposalId,
                            projectId,
                            periodId,
                            as.numeric(phase))

#PushMessage(list("JobId" = JobId, "Status" = "Finish", "Message" = "", "Progress" = "100"))