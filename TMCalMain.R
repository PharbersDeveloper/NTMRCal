
#Sys.setenv(SPARK_HOME="/Users/cui/workFile/calc/spark-2.3.0-bin-hadoop2.7")
#Sys.setenv(YARN_CONF_DIR="/Users/cui/workFile/calc/hadoop-3.0.3/etc/hadoop/")

#library(magrittr)
#library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
library(SparkR)
#library(BPCalSession)
#library(BPRDataLoading)
library(uuid)

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
  
    jobid <- uuid::UUIDgenerate()
    #ss <- BPCalSession::GetOrCreateSparkSession("TMCal", "client")
    
    #cal_data <- BPRDataLoading::LoadDataFromParquent(cal_data_path)
    #weightages <- BPRDataLoading::LoadDataFromParquent(weight_path)
    #manager <- BPRDataLoading::LoadDataFromParquent(manage_path)
    #competitor <- BPRDataLoading::LoadDataFromParquent(competitor_path)
    #standard_time <- BPRDataLoading::LoadDataFromParquent(standard_time_path)
    #level_data <- BPRDataLoading::LoadDataFromParquent(level_data_path)
    ss <- sparkR.session(appName = "TM-Submit")
    cal_data <- read.parquet(cal_data_path)
    weightages <- read.parquet(weight_path)
    manager <- read.parquet(manage_path)
    competitor <- read.parquet(competitor_path)
    standard_time <- read.parquet(standard_time_path)
    level_data <- read.parquet(level_data_path)
    curves <- read.parquet(curves_path)
    
    
    curves <- CastCol2Double(curves, c("x", "y"))
    curves <- collect(curves)
   
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
    #result for assessment
    cal_data_for_assessment <- mutate(cal_data, 
                       sales = cal_data$potential / cal_data$share * 4)
    
    persist(cal_data_for_assessment, "MEMORY_ONLY")
    
    cal_data <- TMCalResAchv(cal_data_for_assessment)
    
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
    write.parquet(cal_data, paste("hdfs://192.168.100.137:9000//test/TMTest/output", jobid, "TMResult", sep = "/"))
    # BPCalSession::CloseSparkSession()
    
    ## competitor ----
    competitor_data <- CastCol2Double(competitor, c("p_share")) 
    
    competitor_data <- AddCols(competitor_data, head(distinct(select(cal_data, "total_potential")), 1), "total_potential")
    
    competitor_data <- mutate(competitor_data, p_sales = competitor_data$total_potential / 4 * competitor_data$p_share,
                              share = competitor_data$p_share * (rand() / 5 + 0.9))
    
    competitor_data <- mutate(competitor_data, sales = competitor_data$total_potential / 4 * competitor_data$share)
    
    competitor_data <- mutate(competitor_data, sales_growth = competitor_data$sales / competitor_data$p_sales - 1)
    
    competitor_data <- select(competitor_data, "product", "sales", "share", "sales_growth")
    
    write.parquet(competitor_data, paste("hdfs://192.168.100.137:9000//test/TMTest/output", jobid, "TMCompetitor", sep = "/"))
    
    ## assessment_region_division ----
    cal_data_agg_for_assessment <- ColRename(agg(groupBy(cal_data_for_assessment, "representative", "general_ability", "total_potential", "total_p_sales"),
                                     potential= "sum", 
                                       p_sales= "sum"),
                                 c("sum(potential)", "sum(p_sales)"),
                                 c("potential", "p_sales"))
    
    assessment_region_division <- withColumn(cal_data_agg_for_assessment, "sumga_scale", lit(head(selectExpr(cal_data_agg_for_assessment, "sum(general_ability -50) as sumga_scale"), 1)[["sumga_scale"]]))
        
    assessment_region_division <- mutate(assessment_region_division,
                              potential_prop = assessment_region_division$potential / assessment_region_division$total_potential,
                              p_sales_prop = assessment_region_division$p_sales / assessment_region_division$total_p_sales)
    
    assessment_region_division <- mutate(assessment_region_division, 
                              ga_prop = (assessment_region_division$general_ability - 50) / assessment_region_division$sumga_scale)
    
    assessment_region_division <- withColumn(assessment_region_division, "ptt_ps_prop" , assessment_region_division[["potential_prop"]] * 0.6 + assessment_region_division[["p_sales_prop"]] * 0.4)
    
    assessment_region_division <- mutate(assessment_region_division, score_s = abs(assessment_region_division$ptt_ps_prop - assessment_region_division$ga_prop))

    assessment_region_division <- mutate(selectExpr(assessment_region_division, "mean(score_s) as score"), index_m = lit("region_division"))
    
    
    
    ## assessment_target_assigns ----
    assessment_target_assigns <- select(cal_data_for_assessment, "potential", "p_sales", "quota", "sales", "total_potential", "total_p_sales", "total_quota")
    
    assessment_target_assigns <- mutate(assessment_target_assigns, potential_prop = assessment_target_assigns$potential / assessment_target_assigns$total_potential,
                                        p_sales_prop = assessment_target_assigns$p_sales / assessment_target_assigns$total_p_sales)
    
    assessment_target_assigns <- mutate(assessment_target_assigns, ptt_ps_prop = assessment_target_assigns$potential_prop * 0.6 + assessment_target_assigns$p_sales_prop * 0.4,
                                        quota_prop = assessment_target_assigns$quota / assessment_target_assigns$total_quota)
    
    assessment_target_assigns <- mutate(assessment_target_assigns, ptt_ps_score = abs(assessment_target_assigns$ptt_ps_prop - assessment_target_assigns$quota_prop),
                                        quota_growth = assessment_target_assigns$quota - assessment_target_assigns$p_sales,
                                        sales_growth = assessment_target_assigns$sales - assessment_target_assigns$p_sales)
    assessment_target_assigns <- withColumn(assessment_target_assigns, "total_sales", lit(head(selectExpr(assessment_target_assigns, "sum(sales)"), 1)[["sum(sales)"]]))
    
    assessment_target_assigns <- withColumn(assessment_target_assigns, "qg_prop", assessment_target_assigns$quota_growth / (assessment_target_assigns$total_quota - assessment_target_assigns$total_p_sales))
    
    assessment_target_assigns <- withColumn(assessment_target_assigns, "sg_prop", assessment_target_assigns$sales_growth / (assessment_target_assigns$total_sales - assessment_target_assigns$total_p_sales))
    
    assessment_target_assigns <- mutate(assessment_target_assigns, q_s_score = abs(assessment_target_assigns$qg_prop - assessment_target_assigns$sg_prop))
    
    assessment_target_assigns <- mutate(selectExpr(assessment_target_assigns, "mean(ptt_ps_score) * 0.7 + mean(q_s_score) * 0.3 as score"), index_m = lit("target_assigns"))
    
    
    
    ## assessment_resource_assigns ----
    assessment_resource_assigns <- select(cal_data_for_assessment, "potential", "p_sales", "budget", "call_time", "representative_time", "meeting_attendance", 
                                          "total_potential", "total_p_sales", "total_budget", "total_place")

    assessment_resource_assigns <- mutate(assessment_resource_assigns, potential_prop = assessment_resource_assigns$potential / assessment_resource_assigns$total_potential,
                                          p_sales_prop = assessment_resource_assigns$p_sales / assessment_resource_assigns$total_p_sales)

    assessment_resource_assigns <- mutate(assessment_resource_assigns, ptt_ps_prop = assessment_resource_assigns$potential_prop * 0.6 + assessment_resource_assigns$p_sales_prop * 0.4,
                                          budget_prop = assessment_resource_assigns$budget / assessment_resource_assigns$total_budget,
                                          time_prop = assessment_resource_assigns$call_time / (assessment_resource_assigns$representative_time * 5),
                                          place_prop = assessment_resource_assigns$meeting_attendance / assessment_resource_assigns$total_place)

    assessment_resource_assigns <- mutate(assessment_resource_assigns, budget_score = abs(assessment_resource_assigns$ptt_ps_prop - assessment_resource_assigns$budget_prop),
                                          time_score = abs(assessment_resource_assigns$ptt_ps_prop - assessment_resource_assigns$time_prop),
                                          place_score = abs(assessment_resource_assigns$ptt_ps_prop - assessment_resource_assigns$place_prop))

    assessment_resource_assigns <- mutate(selectExpr(assessment_resource_assigns, "mean(budget_score) * 0.45 +mean(time_score) * 0.25 + mean(place_score) * 0.3 as score"), index_m = lit("resource_assigns"))
    
    

    ## manage_time ----
    manage_time <- distinct(select(cal_data_for_assessment, "representative", "field_work", "one_on_one_coaching", "employee_kpi_and_compliance_check", 
                                   "admin_work", "kol_management", "business_strategy_planning", "team_meeting", "manager_time"))
    
    manage_time <- ColRename(agg(groupBy(manage_time, "employee_kpi_and_compliance_check", "admin_work", "kol_management", "business_strategy_planning", "team_meeting", "manager_time"),
                                 field_work = "sum",
                                 one_on_one_coaching = "sum"),
                             c("sum(field_work)", "sum(one_on_one_coaching)"),
                             c("field_work", "one_on_one_coaching"))
    
    
    standard_time_data <- CastCol2Double(standard_time, c("employee_kpi_and_compliance_check_std", 
                                                         "admin_work_std", "kol_management_std", 
                                                         "business_strategy_planning_std", 
                                                         "team_meeting_std", "field_work_std", 
                                                         "one_on_one_coaching_std"))
    manage_time <- AddCols(manage_time, head(standard_time_data, 1), columns(standard_time_data))
    
    manage_time <- mutate(selectExpr(manage_time, "(abs(employee_kpi_and_compliance_check - employee_kpi_and_compliance_check_std) / manager_time 
                                     + abs(admin_work - admin_work_std) / manager_time 
                                     + abs(kol_management - kol_management_std) / manager_time 
                                     + abs(business_strategy_planning - business_strategy_planning_std) / manager_time 
                                     + abs(team_meeting - team_meeting_std) / manager_time 
                                     + abs(field_work - field_work_std) / manager_time 
                                     + abs(one_on_one_coaching - one_on_one_coaching_std) / manager_time) / 7 as score"), 
                          index_m = lit("manage_time"))
    
    
    ## manage_team ----
    manage_team <- distinct(select(cal_data_for_assessment, "representative", "general_ability", "p_product_knowledge", "p_sales_skills", "p_territory_management_ability", "p_work_motivation", "p_behavior_efficiency"))
    
    manage_team <- withColumn(manage_team, "p_general_ability", (manage_team$p_territory_management_ability * 0.2 + manage_team$p_sales_skills * 0.25 + manage_team$p_product_knowledge * 0.25
                                                                 + manage_team$p_behavior_efficiency * 0.15 + manage_team$p_work_motivation * 0.15) * 10)
    
    manage_team <- withColumn(manage_team, "space_delta", - manage_team$p_general_ability + 100)
    
    manage_team <- withColumn(manage_team, "growth_delta", manage_team$general_ability - manage_team$p_general_ability)
    
    manage_team <- mutate(selectExpr(manage_team, "0.2 - mean(growth_delta) / mean(space_delta) as score"), index_m = lit("manage_team"))
    
    
    assessments <- rbind(assessment_region_division, assessment_target_assigns, assessment_resource_assigns, manage_time, manage_team)
    
    level_data <- CastCol2Double(level_data, c("level1", "level2"))
    particular_assessment <- join(assessments, level_data, assessments$index_m == level_data$index, "left")
    
    particular_assessment <- mutate(particular_assessment, 
                                    level = cal_assessment_level(particular_assessment))
    
    
    particular_assessment <- select(particular_assessment, "index", "code", "level")
    
    
    ## general_assessment ----
    general_assessment <- mutate(selectExpr(particular_assessment, "cast(mean(level) as int) as level"), 
                                 index = lit("general_performance"),
                                 code = lit(5))
    
    assessment <- unionByName(particular_assessment, general_assessment)
    
    write.parquet(assessment, paste("hdfs://192.168.100.137:9000//test/TMTest/output", jobid, "Assessment", sep = "/"))
    
    unpersist(cal_data_for_assessment, blocking = FALSE)
}

TMCalProcess(
    cal_data_path = "hdfs://192.168.100.137:9000//test/TMTest/inputParquet/TMInputParquet0815/cal_data",
    weight_path = "hdfs://192.168.100.137:9000//test/TMTest/inputParquet/TMInputParquet0815/weightages",
    manage_path = "hdfs://192.168.100.137:9000//test/TMTest/inputParquet/TMInputParquet0815/manager",
    curves_path = "hdfs://192.168.100.137:9000//test/TMTest/inputParquet/TMInputParquet0815/curves-n",
    competitor_path = "hdfs://192.168.100.137:9000//test/TMTest/inputParquet/TMInputParquet0815/competitor",
    standard_time_path = "hdfs://192.168.100.137:9000//test/TMTest/inputParquet/TMInputParquet0815/standard_time",
    level_data_path = "hdfs://192.168.100.137:9000//test/TMTest/inputParquet/TMInputParquet0815/level_data"
)
