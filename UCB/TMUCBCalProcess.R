# TM UCB Cal Process Model
#

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

Sys.setenv(SPARK_HOME="D:/tools/spark-2.3.0-bin-hadoop2.7")
Sys.setenv(YARN_CONF_DIR="D:/tools/hadoop-3.0.3")

library(magrittr)
library(SparkR, lib.loc = "D:/tools/spark-2.3.0-bin-hadoop2.7/R/lib")
library(BPCalSession)
library(BPRDataLoading)
library(BPRSparkCalCommon)
library(uuid)

TMCalCurveSkeleton3 <- BPRSparkCalCommon::TMCalCurveSkeleton3
curve_func <- BPRSparkCalCommon::curve_func

source("./UCB/UCBDataBinding.R", encoding = "UTF-8")
source("./UCB/UCBCalFuncs.R", encoding = "UTF-8")

#' UCB Calculation
#' @export
TMUCBCalProcess <- function(
    cal_data_path,
    weight_path,
    curves_path,
    competitor_path,
    level_data_path,
    standard_time_path,
    jobid,
    proposalid,
    projectid,
    periodid) {
    
    # cal_data_path = "hdfs://192.168.100.137:9000/test/UCBTest/inputParquet/TMInputParquet0820/cal_data_20190904"
    # weight_path = "hdfs://192.168.100.137:9000/test/UCBTest/inputParquet/TMInputParquet0820/weightages"
    # curves_path = "hdfs://192.168.100.137:9000/test/UCBTest/inputParquet/TMInputParquet0820/curves-n"
    # competitor_path = "hdfs://192.168.100.137:9000/test/UCBTest/inputParquet/TMInputParquet0820/competitor"
    # level_data_path = "hdfs://192.168.100.137:9000/test/UCBTest/inputParquet/TMInputParquet0820/competitor"
    # standard_time_path = "hdfs://192.168.100.137:9000/test/UCBTest/inputParquet/TMInputParquet0820/competitor"
    # jobid = uuid::UUIDgenerate()
    # proposalid = uuid::UUIDgenerate()
    # projectid = uuid::UUIDgenerate()
    # periodid = uuid::UUIDgenerate()
    
    print(jobid)
    
    output_dir <- paste0("hdfs://192.168.100.137:9000/tmtest0831/jobs/", jobid, "/output/")
    #jobid <- uuid::UUIDgenerate()
    #ss <- sparkR.session(appName = "UCB-Submit")
    ss <- BPCalSession::GetOrCreateSparkSession("UCBCal_liu", "client")
    # cal_data <- read.parquet(cal_data_path)
    cal_data <- BPRDataLoading::LoadDataFromParquent(cal_data_path)
    #print(head(cal_data, 10))
    #weightages <- read.parquet(weight_path)
    weightages <- BPRDataLoading::LoadDataFromParquent(weight_path)
    curves <- BPRDataLoading::LoadDataFromParquent(curves_path)
    curves <- CastCol2Double(curves, c("x", "y"))
    curves <- collect(curves)

    cal_data <- UCBDataBinding(cal_data, weightages)

    cal_data <- mutate(cal_data,
                       total_budget = cal_total_budget(cal_data),
                       potential_m = cal_update_protential(cal_data),
                       potential_factor = cal_potential_factor(cal_data),
                       sales_factor = cal_sales_factor(cal_data),
                       max_oa = cal_mac_oa(cal_data)
    )

    cal_data <- mutate(cal_data,
                       budget_prop = cal_data$budget / cal_data$total_budget
    )
    persist(cal_data, "MEMORY_ONLY")
    up01 <- cal_data
    
    cal_dist_data <-  ColRename(agg(groupBy(cal_data, "product", "representative", "city"),
                                    potential_m="sum",
                                    p_sales="sum",
                                    hospital="count"),
                                c("sum(potential_m)", "sum(p_sales)", "count(hospital)"),
                                c("potential_dist", "sales_dist", "hospital_num_dist"))

    cal_dist_data <- CastCol2Double(
        ColRename(agg(groupBy(cal_dist_data, "product", "representative"),
                      potential_dist="sum",
                      sales_dist="sum",
                      hospital_num_dist="sum",
                      city="count"),
                  c("product", "representative", "sum(potential_dist)", "sum(sales_dist)", "count(city)", "sum(hospital_num_dist)"),
                  c("product_mm", "representative_m", "potential_dist", "sales_dist", "city_num_dist", "hospital_num_dist")),
        c("hospital_num_dist", "city_num_dist"))

    cal_market_data <- ColRename(agg(groupBy(cal_data, "product"),
                                     potential="sum",
                                     potential_m="sum",
                                     p_sales="sum",
                                     quota="sum"),
                                 c("product", "sum(potential)", "sum(potential_m)", "sum(p_sales)", "sum(quota)"),
                                 c("product_m", "sumptt", "sumpttm", "sumps", "sumqt"))

    cal_data <- join(cal_data, cal_market_data, cal_data$product == cal_market_data$product_m, "inner")
    cal_data <- join(cal_data, cal_dist_data, 
                     cal_data$product == cal_dist_data$product_mm & cal_data$representative == cal_dist_data$representative_m, "inner") ###有问题

    cal_data <- mutate(cal_data,
                       potential_contri = cal_data$potential_m / cal_data$sumpttm,
                       sales_contri = ifelse(cal_data$sumps == 0, 0.0, cal_data$p_sales/cal_data$sumps)
    )
    cal_data <- mutate(cal_data,
                       value_contri = cal_value_contri(cal_data)
    )
    cal_data <- mutate(cal_data,
                       budget_factor = cal_data$budget_prop / cal_data$value_contri
    )
    persist(cal_data, "MEMORY_ONLY")
    up02 <- cal_data

    cal_developed_data <- filter(cal_data, cal_data$status == "已开发" | cal_data$status == "正在开发")
    cal_developed_data <- mutate(cal_developed_data,
                                 # factor 1
                                 hospital_quota_base = cal_hospital_quota_base(cal_developed_data),
                                 quota_prop = cal_quota_prop(cal_developed_data),
                                 hospital_product_quota_growth_factor = cal_hospital_product_quota_growth_factor(cal_developed_data),
                                 # factor 2
                                 district_cross_factor = cal_district_cross_factor(cal_developed_data),
                                 district_potential_factor = cal_district_potential_factor(cal_developed_data),
                                 district_sales_factor = cal_district_sales_factor(cal_developed_data),
                                 district_hospital_factor = cal_district_hospital_factor(cal_developed_data)
    )
    cal_developed_data <- mutate(cal_developed_data,
                                 # factor 1
                                 hospital_quota_base_factor = cal_hospital_quota_base_factor(cal_developed_data))

    cal_developed_data <- mutate(cal_developed_data,
                                 # factor 1
                                 hospital_quota_base_factor_m = 
                                     cal_hospital_quota_base_factor_m(cal_developed_data),
                                 hospital_product_quota_growth_factor_m = 
                                     cal_hospital_product_quota_growth_factor_m(cal_developed_data)
    )
    cal_developed_data <- mutate(cal_developed_data,
                                 # factor 1
                                 factor1 = cal_factor_1(cal_developed_data),
                                 # factor 2
                                 factor2 = cal_factor_2(cal_developed_data)
    )

    # oa
    cal_developed_data <- mutate(cal_developed_data,
                                 #factor = cal_developed_data$factor1 * cal_developed_data$factor2,
                                 adjust_factor = lit(1.0) - cal_developed_data$factor1 * cal_developed_data$factor2,
                                 p_oa_factor = cal_p_oa_factor(cal_developed_data)
    )


    cal_developed_data <- TMCalCurveSkeleton2(cal_developed_data, curves,
                                              c(
                                                  "curve02:curve03:curve03", "oa_factor_base", "budget_factor", 
                                                  "product$开拓来:威芃可:优派西", "curve09", "p_offer_attractiveness", 
                                                  "p_oa_factor", "None"
                                              ), TMCalValue2String)

    cal_developed_data <- mutate(cal_developed_data,
                                 p_offer_attractiveness_m = cal_p_offer_attractiveness_m(cal_developed_data)
    )
    cal_developed_data <- mutate(cal_developed_data,
                                 offer_attractiveness = cal_offer_attractiveness(cal_developed_data)
    )
    cal_developed_data <- mutate(cal_developed_data,
                                 offer_attractiveness_adj = cal_offer_attractiveness_adj(cal_developed_data)
    )

    # market share
    cal_developed_data <- TMCalCurveSkeleton2(cal_developed_data, curves,
                                              c(
                                                  "curve01:curve05", "market_share", "offer_attractiveness_adj", 
                                                  "status$已开发:未开发"
                                              ), TMCalValue2String)

    cal_developed_data <- mutate(cal_developed_data,
                                 market_share_m = cal_market_share_m(cal_developed_data)
    )

    cal_developed_data <- mutate(cal_developed_data,
                                 sales = cal_developed_data$potential_m * cal_developed_data$market_share_m
    )

    cal_developed_data <- mutate(cal_developed_data,
                                 ytd_sales = cal_developed_data$p_ytd_sales + cal_developed_data$sales
    )

    cal_developed_data <- ColRename(select(cal_developed_data,
                                           c("city", "hospital", "hospital_level", "representative", "product", "product_area", 
                                             "potential", "patient", "status", "is_drugstore", "rep_num", "hosp_num", 
                                             "initial_budget", "p_quota", "p_budget", "p_sales", "pppp_sales", "p_ytd_sales", 
                                             "quota", "budget", "market_share_m", "sales", "ytd_sales")),
                                    c("market_share_m"), c("market_share"))

    # 未开发
    cal_undev_data <- filter(cal_data, cal_data$status == "未开发")

    cal_undev_data <- mutate(cal_undev_data,
                             budget_m = cal_undev_data$budget + cal_undev_data$p_budget,
                             market_share = lit(0.0),
                             sales = lit(0.0),
                             develop_fee = cal_develop_fee(cal_undev_data)
    )
    cal_undev_data <- mutate(cal_undev_data,
                             status_m = cal_update_status(cal_undev_data),
                             ytd_sales = cal_undev_data$p_ytd_sales + cal_undev_data$sales
    )

    cal_undev_data <- ColRename(select(cal_undev_data,
                                       c("city", "hospital", "hospital_level", "representative", "product", "product_area", 
                                         "potential", "patient", "status_m", "is_drugstore", "rep_num", "hosp_num", "initial_budget", 
                                         "p_quota", "p_budget", "p_sales", "pppp_sales", "p_ytd_sales", "quota", "budget_m", 
                                         "market_share", "sales", "ytd_sales")),
                                c("status_m", "budget_m"), c("status", "budget"))

    cal_data <- rbind(cal_developed_data, cal_undev_data)
    cal_data <- filter(cal_data, isNotNull(cal_data$status))
    cal_data <- mutate(cal_data,
                       in_sales = ifelse(cal_data$is_drugstore == 1, cal_data$sales * 0.5, cal_data$sales),
                       out_sales = ifelse(cal_data$is_drugstore == 1, cal_data$sales * 0.5, 0.0))
    cal_data <- mutate(cal_data,
                       account = ifelse(cal_data$status == "正在开发", 1, 0.0),
                       quota_achv = ifelse(cal_data$quota == 0 & cal_data$sales == 0,
                                           0.0,
                                           ifelse(cal_data$quota == 0 & cal_data$sales > 0,
                                                  1,
                                                  cal_data$sales / cal_data$quota))
    )

    persist(cal_data, "MEMORY_ONLY")
    up03 <- cal_data

    cal_calc_data <- head(ColRename(select(cal_data,
                                           sum(cal_data$account),
                                           sum(cal_data$budget),
                                           sum(cal_data$p_sales),
                                           sum(cal_data$sales)),
                                    c("sum(account)", "sum(budget)", "sum(p_sales)", "sum(sales)"),
                                    c("new_account", "total_budget", "sumps", "sums")), 1)

    cal_data <- mutate(cal_data,
                       new_account = lit(cal_calc_data$new_account),
                       total_budget = lit(cal_calc_data$total_budget),
                       sumps = lit(cal_calc_data$sumps),
                       sums = lit(cal_calc_data$sums),
                       job_id = lit(jobid),
                       project_id = lit(projectid),
                       period_id = lit(periodid)
    )
    
    cal_data <- mutate(cal_data,
                       sales_increase = cal_sale_increase(cal_data))
    cal_data <- mutate(cal_data,
                       next_budget = cal_next_budget(cal_data))

    cal_data <- distinct(cal_data)
    
    # cal_data <-  ColRename(agg(groupBy(cal_data, "product", "representative", "hospital", "is_drugstore"),
    #                            job_id = lit(jobid),
    #                            project_id = lit(projectid),
    #                            period_id = lit(periodid),
    #                            city = first(cal_data$city),
    #                            hospital_level = first(cal_data$hospital_level),
    #                            product_area = first(cal_data$product_area),
    #                            potential = max(cal_data$potential),
    #                            patient = max(cal_data$patient),
    #                            status = first(cal_data$status),
    #                            rep_num = max(cal_data$rep_num),
    #                            hosp_num = max(cal_data$hosp_num),
    #                            initial_budget = max(cal_data$initial_budget),
    #                            p_quota = max(cal_data$p_quota),
    #                            quota = max(cal_data$quota),
    #                            quota_achv = max(cal_data$quota_achv),
    #                            p_budget = max(cal_data$p_budget),
    #                            budget = max(cal_data$budget),
    #                            total_budget = max(cal_data$total_budget),
    #                            p_sales = max(cal_data$p_sales),
    #                            sales = max(cal_data$sales),
    #                            sums = max(cal_data$sums),
    #                            pppp_sales = max(cal_data$pppp_sales),
    #                            p_ytd_sales = max(cal_data$p_ytd_sales),
    #                            sumps = max(cal_data$sumps),
    #                            ytd_sales = max(cal_data$ytd_sales),
    #                            market_share = max(cal_data$market_share),
    #                            account = first(cal_data$account),
    #                            new_account = max(cal_data$new_account)
    #                            ),
    #                             c("max(potential)", "max(patient)", "first(status)", "max(rep_num)", "max(hosp_num)", 
    #                               "max(initial_budget)", "max(p_quota)", "max(p_budget)", "max(p_sales)", "max(pppp_sales)", 
    #                               "max(p_ytd_sales)", "max(quota)", "max(budget)", "max(market_share)", "max(sales)", 
    #                               "max(ytd_sales)", "max(new_account)", "max(total_budget", "max(sumps)", "max(sums)"),
    #                             c("potential", "patient", "status", "rep_num", "hosp_num", "initial_budget", 
    #                               "p_quota", "p_budget", "p_sales", "pppp_sales", "p_ytd_sales", "quota", "budget",
    #                               "market_share", "sales", "ytd_sales", "new_account", "total_budget", "sumps", "sums"))
    
    write.parquet(cal_data, paste0(output_dir, "cal_report"))

    persist(cal_data, "MEMORY_ONLY")
    up_result <- cal_data

    ## out hospital report
    # cal_hospital_report <- ColRename(agg(groupBy(cal_data, "product"),
    #                                      potential="sum"),
    #                                  c("sum(potential)"),
    #                                  c("potential"))
    # 
    # cal_hospital_report <- mutate(cal_hospital_report,
    #                               potential = cal_hospital_report$potential * (rand() / 100 + 0.01)
    # )
    # cal_hospital_report <- mutate(cal_hospital_report,
    #                               sales = cal_hospital_report$potential * (rand() / 100 + 0.015)
    # )
    # 
    # write.parquet(cal_hospital_report, paste0(output_dir, "hospital_report"))

    ## competitor product report
    cal_product_area <- select(ColRename(agg(groupBy(cal_data, "product_area"),
                                             potential="sum"),
                                         c("product_area", "sum(potential)"),
                                         c("product_area_m", "potential")),
                               "product_area_m", "potential")

    #competitor <- read.parquet(competitor_path)
    competitor <- BPRDataLoading::LoadDataFromParquent(competitor_path)
    competitor <- CastCol2Double(competitor, c("market_share_c"))

    cal_product_area <- join(cal_product_area, competitor, cal_product_area$product_area_m == competitor$product_area, "inner")

    cal_product_area <- mutate(cal_product_area,
                               market_share = cal_product_area$market_share_c * (rand() * 0.2 + 0.9)
    )
    cal_product_area <- mutate(cal_product_area,
                               sales = cal_product_area$potential * cal_product_area$market_share,
                               job_id = lit(jobid),
                               project_id = lit(projectid),
                               period_id = lit(periodid)
    )

    cal_product_area = distinct(cal_product_area)
    write.parquet(cal_product_area, paste0(output_dir, "competitor"))

    # final summary report 单周期
    cal_result_summary <- select(cal_data, "representative", "status", "p_sales", "pppp_sales", "sales", "quota", "budget", "account")
    cal_result_summary <- ColRename(agg(groupBy(cal_result_summary, "representative"),
                                        p_sales ="sum",
                                        pppp_sales ="sum",
                                        sales ="sum",
                                        quota ="sum",
                                        account ="sum",
                                        budget ="sum"),
                                    c("sum(p_sales)", "sum(pppp_sales)", "sum(sales)", "sum(quota)", "sum(account)", "sum(budget)"),
                                    c("p_sales", "pppp_sales", "sales", "quota", "new_account", "budget"))

    cal_result_summary <- ColRename(agg(cal_result_summary,
                                        sum(cal_result_summary$p_sales),
                                        sum(cal_result_summary$pppp_sales),
                                        sum(cal_result_summary$sales),
                                        sum(cal_result_summary$quota),
                                        sum(cal_result_summary$new_account),
                                        sum(cal_result_summary$budget),
                                        count(cal_result_summary$representative)),
                                    c("sum(p_sales)", "sum(pppp_sales)", "sum(sales)", "sum(quota)", "sum(new_account)", 
                                      "sum(budget)", "count(representative)"),
                                    c("p_sales", "pppp_sales", "sales", "quota", "new_account", "budget", "rep_num"))

    cal_result_summary <- mutate(cal_result_summary,
                                 quota_achv = ifelse(cal_result_summary$quota == 0 & cal_result_summary$sales == 0,
                                                     0.0,
                                                     ifelse(cal_result_summary$quota == 0 & cal_result_summary$sales > 0,
                                                            1,
                                                            cal_result_summary$sales / cal_result_summary$quota)),
                                 sales_force_productivity = cal_result_summary$sales / cal_result_summary$rep_num,
                                 return_on_investment = ifelse(cal_result_summary$budget == 0 & cal_result_summary$sales == 0,
                                                               0.0,
                                                               ifelse(cal_result_summary$budget == 0 & cal_result_summary$sales > 0,
                                                                      1,
                                                                      cal_result_summary$sales / cal_result_summary$budget)),
                                 growth_month_on_month = ifelse(cal_result_summary$p_sales == 0 & cal_result_summary$sales == 0,
                                                                0.0,
                                                                ifelse(cal_result_summary$p_sales == 0 & cal_result_summary$sales > 0,
                                                                       1,
                                                                       cal_result_summary$sales / cal_result_summary$p_sales - 1.0)),
                                 growth_year_on_year = ifelse(cal_result_summary$pppp_sales == 0 & cal_result_summary$sales == 0,
                                                              0.0,
                                                              ifelse(cal_result_summary$pppp_sales == 0 & cal_result_summary$sales > 0,
                                                                     1,
                                                                     cal_result_summary$sales / cal_result_summary$pppp_sales - 1.0)),
                                 job_id = lit(jobid),
                                 project_id = lit(projectid),
                                 period_id = lit(periodid)
    )

    cal_result_summary <- select(cal_result_summary,
                                 c("job_id", "project_id", "period_id", "sales", "quota", "budget", "new_account", "quota_achv",
                                   "growth_month_on_month", "growth_year_on_year",
                                   "sales_force_productivity", "return_on_investment"))

    cal_result_summary = distinct(cal_result_summary)
    write.parquet(cal_result_summary, paste0(output_dir, "summary"))

    unpersist(up01, blocking = FALSE)
    unpersist(up02, blocking = FALSE)
    unpersist(up03, blocking = FALSE)
    unpersist(up_result, blocking = FALSE)
}

TMUCBCalProcess(
    cal_data_path = "hdfs://192.168.100.137:9000/test/UCBTest/inputParquet/TMInputParquet0820/cal_data_20190904",
    weight_path = "hdfs://192.168.100.137:9000/test/UCBTest/inputParquet/TMInputParquet0820/weightages",
    curves_path = "hdfs://192.168.100.137:9000/test/UCBTest/inputParquet/TMInputParquet0820/curves-n",
    competitor_path = "hdfs://192.168.100.137:9000/test/UCBTest/inputParquet/TMInputParquet0820/competitor",
    level_data_path = "hdfs://192.168.100.137:9000/test/UCBTest/inputParquet/TMInputParquet0820/competitor",
    standard_time_path = "hdfs://192.168.100.137:9000/test/UCBTest/inputParquet/TMInputParquet0820/competitor",
    jobid = uuid::UUIDgenerate(),
    proposalid = uuid::UUIDgenerate(),
    projectid = uuid::UUIDgenerate(),
    periodid = uuid::UUIDgenerate())
