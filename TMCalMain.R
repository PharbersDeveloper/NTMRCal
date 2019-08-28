
Sys.setenv(SPARK_HOME="/Users/alfredyang/Desktop/spark/spark-2.3.0-bin-hadoop2.7")
Sys.setenv(YARN_CONF_DIR="/Users/alfredyang/Desktop/hadoop-3.0.3/etc/hadoop/")

library(magrittr)
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
library(BPCalSession)
library(BPRDataLoading)

source("AddCols.R")
source("CastCol2Double.R")
source("ColMin.R")
source("ColMax.R")
source("ColRename.R")
source("ColSum.R")
source("CurveFunc.R")
source("TMDataCbind.R")
source("TMCalCurveSkeleton2.R")
source("TMCalColFuncs.R")
source("TMCalResAchv.R")

# 1. 将curves 数据从数据中提出，并全部转化为double类型
# 2. 将cal 以及参数进行列添加, 并ID 化
# 3. 通过计算添加level_factor以及level
# 4. 通过计算人
# 5. 计算Call time
# 6. cal quota
# 7. 计算field work 
# 8. 计算 strategy planing, admin work, kpi check, teem meeting, kol management
# 9. 计算 budget, meeting attendance, customer relationsip
# 10. 计算 oa, share delta, share & salesu
# 11. 计算代表的销售达成

TMCalProcess <- function(
    cal_data_path, 
    weight_path, 
    manage_path,
    curves_path, 
    competitor_path,
    level_data_path,
    standard_time_path) {
  
    ss <- BPCalSession::GetOrCreateSparkSession("TMCal", "client")
    cal_data <- BPRDataLoading::LoadDataFromParquent(cal_data_path)
    weightages <- BPRDataLoading::LoadDataFromParquent(weight_path)
    manager <- BPRDataLoading::LoadDataFromParquent(manage_path)

    curves <- CastCol2Double(BPRDataLoading::LoadDataFromParquent(curves_path), c("x", "y"))
   
    cal_data <- TMDataCbind(cal_data, weightages, manager)
    
    cal_data <- mutate(cal_data, 
                       level_factor = cal_level_factor(cal_data),
                       work_motivation = cal_work_motivation(cal_data),
                       territory_management_ability = cal_tma(cal_data),
                       sales_skills = cal_sales_skill(cal_data),
                       product_knowledge = cal_product_knowledge(cal_data),
                       quota_growth = cal_quota_growth(cal_data),
                       budget_prop = cal_budget_prop(cal_data)
    ) 
    cal_data <- mutate(cal_data, 
                       level = cal_level(cal_data),
                       quota_restriction_factor = cal_quota_restriction_factor(cal_data)
                       )
    
    cal_data <- TMCalCurveSkeleton2(cal_data, curves, 
                                   c(
                                     "curve09", "behavior_efficiency_factor", "one_on_one_coaching", "None",
                                     "curve10:curve11:curve12", "call_time_index", "call_time", "level$1:2:3",
                                     "curve14", "quota_restriction_index", "quota_restriction_factor", "None",
                                     "curve16", "field_work_index", "field_work", "None",
                                     "curve18", "business_strategy_planning_index", "business_strategy_planning", "None",
                                     "curve18", "admin_work_index", "admin_work", "None",
                                     "curve18", "employee_kpi_and_compliance_check_index", "employee_kpi_and_compliance_check", "None",
                                     "curve18", "team_meeting_index", "team_meeting", "None",
                                     "curve18", "kol_management_index", "kol_management", "None",
                                     "curve02:curve03:curve04", "budget_factor", "budget_prop", "level$1:2:3",
                                     "curve05:curve06:curve07", "meeting_attendance_factor", "meeting_attendance", "level$1:2:3"
                                     # "curve28", "share_delta_factor", "offer_attractiveness", "None"
                                     )
                                  )
    
    cal_data <- mutate(cal_data, 
                       behavior_efficiency = cal_b_e(cal_data),
                       deployment_quality = cal_deployment_quality(cal_data),
                       customer_relationship = cal_customer_relationship(cal_data))
    cal_data <- mutate(cal_data, general_ability = cal_general_ability(cal_data))
    cal_data <- mutate(cal_data, 
                       rep_ability_efficiency = cal_rep_ability_eff(cal_data))
    cal_data <- mutate(cal_data, 
                       sales_performance = cal_sales_performance(cal_data))
    cal_data <- mutate(cal_data, 
                       offer_attractiveness = cal_offer_attractiveness(cal_data))
    
    cal_data <- TMCalCurveSkeleton2(cal_data, curves, 
                                   c(
                                     "curve28", "share_delta_factor", "offer_attractiveness", "None"
                                     )
                                   )
 
    cal_data <- mutate(cal_data, 
                       share = cal_data$p_share * (cal_data$share_delta_factor + 1))
    cal_data <- mutate(cal_data, 
                       sales = cal_data$potential / cal_data$share * 4)
    
    cal_data <- TMCalResAchv(cal_data)
    
    cal_data <- mutate(cal_data, 
                       rep_quota_achv = cal_data$rep_sales / cal_data$rep_quota,
                       target = cal_data$p_target,
                       target_coverage = cal_data$p_target_coverage)
    cal_data <- mutate(cal_data, 
                       work_motivation = cal_update_work_motivation(cal_data),
                       class1 = cal_classABC(cal_data),
                       class2 = cal_classABC(cal_data))
    cal_data <- mutate(cal_data,
                       target_coverage = cal_target_coverage(cal_data),
                       high_target_m = cal_high_target_m(cal_data),
                       middle_target_m = cal_middle_target_m(cal_data),
                       low_target_m = cal_low_target_m(cal_data))

    cal_data <- mutate(cal_data,
                       high_target = cal_high_target(cal_data),
                       middle_target = cal_middle_target(cal_data),
                       low_target = cal_low_target(cal_data))
    
    cal_data <- select(cal_data, 
                       "hospital", "hospital_level", "budget", "meeting_attendance", "product", "quota", "call_time",
                       "one_on_one_coaching", "field_work", "performance_review", "product_knowledge_training",
                       "territory_management_training", "representative", "sales_skills_training", "career_development_guide",
                       "employee_kpi_and_compliance_check", "admin_work", "kol_management", "business_strategy_planning",
                       "team_meeting", "potential", "p_sales", "p_quota", "p_share", "life_cycle", "representative_time",
                       "p_territory_management_ability", "p_sales_skills", "p_product_knowledge", "p_behavior_efficiency",
                       "p_work_motivation", "total_potential", "total_p_sales", "total_quota", "total_place", "manager_time", 
                       "work_motivation", "territory_management_ability", "sales_skills", 
                       "product_knowledge", "behavior_efficiency", "general_ability", "target", "target_coverage", 
                       "high_target", "middle_target", "low_target", "share", "sales")
   
    # write.df(cal_data, "hdfs://192.168.100.137:9000//test/TMTest/inputParquet/TMInputParquet0815/output/abcde")
    # write.parquet(cal_data, "hdfs://192.168.100.137:9000//test/TMTest/inputParquet/TMInputParquet0815/output/abcde-parquet")
    
    print(head(cal_data, 10))
    
    # BPCalSession::CloseSparkSession()
}

TMCalProcess(
    cal_data_path = "hdfs://192.168.100.137:9000//test/TMTest/inputParquet/TMInputParquet0815/cal_data",
    weight_path = "hdfs://192.168.100.137:9000//test/TMTest/inputParquet/TMInputParquet0815/weightages",
    manage_path = "hdfs://192.168.100.137:9000//test/TMTest/inputParquet/TMInputParquet0815/manager",
    curves_path = "hdfs://192.168.100.137:9000//test/TMTest/inputParquet/TMInputParquet0815/curves-n"
)
