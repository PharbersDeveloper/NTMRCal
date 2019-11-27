# TM UCB Process
#

# just for client mode
# don't need it when cluster mode
#Sys.setenv(SPARK_HOME="/Users/alfredyang/Desktop/spark/spark-2.3.0-bin-hadoop2.7")
#Sys.setenv(YARN_CONF_DIR="/Users/alfredyang/Desktop/hadoop-3.0.3/etc/hadoop/")

#library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
#library(BPCalSession)
#library(uuid)
#library(BPRSparkCalCommon)

#source("./R/TMUCBCalProcess.R")

#TMCalCurveSkeleton3 <- BPRSparkCalCommon::TMCalCurveSkeleton3
#curve_func <- BPRSparkCalCommon::curve_func

#ss <- BPCalSession::GetOrCreateSparkSession("UCBCal", "cluster")

#TMUCBCalProcess(
#    cal_data_path = "hdfs://192.168.100.137:8020//test/UCBTest/inputParquet/TMInputParquet0820/cal_data",
#    weight_path = "hdfs://192.168.100.137:8020//test/UCBTest/inputParquet/TMInputParquet0820/weightages",
#    curves_path = "hdfs://192.168.100.137:8020//test/UCBTest/inputParquet/TMInputParquet0820/curves-n",
#    competitor_path = "hdfs://192.168.100.137:8020//test/UCBTest/inputParquet/TMInputParquet0820/competitor"
#)
