library(httr)
library(jsonlite)
library(SparkR)
library(uuid)
library(BPRSparkCalCommon)
library(RKafkaProxy)

TMCalCurveSkeleton3 <- BPRSparkCalCommon::TMCalCurveSkeleton3
curve_func <- BPRSparkCalCommon::curve_func
TMAggSchema <- BPRSparkCalCommon::TMAggSchema

cmd_args = commandArgs(T)

JobID <- cmd_args[2]
proposalId <- cmd_args[3]
projectId <- cmd_args[4]
periodId <- cmd_args[5]
phase <- cmd_args[6]

ss <- sparkR.session(
    appName = "UCB-Submit",
    sparkConfig = list(
        spark.driver.memory = "1g",
        spark.executor.memory = "1g",
        spark.executor.cores = "1",
        spark.executor.instances = "1",
        spark.default.parallelism = 1L,
        es.nodes.wan.only = TRUE,
        es.pushdown = TRUE,
        es.index.auto.create = TRUE,
        es.nodes = "pharbers.com",
        es.port = 9200L) 
    )
job_scala_proxy <- sparkR.newJObject("com.pharbers.CallJMethod.BPTMProxy.TMProxy")
tmpId <- sparkR.callJMethod(job_scala_proxy, "BPTMUCBPreCal", 
                            proposalId,
                            projectId,
                            periodId,
                            as.numeric(phase))

output_dir <- paste0("hdfs://192.168.100.137:8020/tmtest0831/jobs/", tmpId, "/input/")

if (cmd_args[1] == "UCB") {
	PushMessage(list("JobId" = JobID, "Status" = "Running", "Message" = "", "Progress" = "0"))
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
	PushMessage(list("JobId" = JobID, "Status" = "Running", "Message" = "", "Progress" = "0"))
    source("TMCalProcess.R")
    TMCalProcess(
        cal_data_path =  paste0(output_dir, "cal_data"), #cmd_args[2],
        weight_path = cmd_args[7],
        curves_path = cmd_args[8],
        manage_path = cmd_args[9],
        competitor_path = paste0(output_dir, "cal_comp"), #cmd_args[5]
        standard_time_path = cmd_args[10],
        level_data_path = cmd_args[11],
        jobid = tmpId,
        proposalid = proposalId,
        projectid = projectId,
        periodid = periodId
    )
}

sparkR.callJMethod(job_scala_proxy, "BPTMUCBPostCal",
                   tmpId,
                   proposalId,
                   projectId,
                   periodId,
                   as.numeric(phase))

sparkR.callJMethod(job_scala_proxy, "BPTMUBShowResult",
                   tmpId,
                   proposalId,
                   projectId,
                   periodId,
                   as.numeric(phase))

PushMessage(list("JobId" = JobID, "Status" = "Finish", "Message" = "", "Progress" = "100"))