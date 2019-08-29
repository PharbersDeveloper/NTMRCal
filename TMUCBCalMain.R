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
source("UCBDataBinding.R")
source("TMCalCurveSkeleton2.R")
source("UCBCalFuncs.R")
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

TMUCBCalProcess <- function(
    cal_data_path, 
    weight_path, 
    curves_path, 
    competitor_path,
    level_data_path,
    standard_time_path) {

    ss <- BPCalSession::GetOrCreateSparkSession("UCBCal", "client")
    cal_data <- BPRDataLoading::LoadDataFromParquent(cal_data_path)
    weightages <- BPRDataLoading::LoadDataFromParquent(weight_path)
    
    curves <- CastCol2Double(BPRDataLoading::LoadDataFromParquent(curves_path), c("x", "y"))
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
    cal_data <- join(cal_data, cal_dist_data, cal_data$product == cal_dist_data$product_mm, "inner")
    
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
                                 hospital_quota_base_factor_m = cal_hospital_quota_base_factor_m(cal_developed_data),
                                 hospital_product_quota_growth_factor_m = cal_hospital_product_quota_growth_factor_m(cal_developed_data)
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
                                        "curve02:curve03:curve03", "oa_factor_base", "budget_factor", "product$开拓来:威芃可:优派西",
                                        "curve09", "p_offer_attractiveness", "p_oa_factor", "None"
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
                                        "curve01:curve05", "market_share", "offer_attractiveness_adj", "status$已开发:未开发"
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
                                 c("city", "hospital", "hospital_level", "representative", "product", "product_area", "potential", 
                                   "patient", "status", "rep_num", "hosp_num", "initial_budget", "p_quota", "p_budget", "p_sales", 
                                   "pppp_sales", "p_ytd_sales", "quota", "budget", "market_share_m", "sales", "ytd_sales")),
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
                             c("city", "hospital", "hospital_level", "representative", "product", "product_area", "potential", 
                               "patient", "status_m", "rep_num", "hosp_num", "initial_budget", "p_quota", "p_budget", "p_sales", 
                               "pppp_sales", "p_ytd_sales", "quota", "budget_m", "market_share", "sales", "ytd_sales")),
                             c("status_m", "budget_m"), c("status", "budget"))
   
    cal_data <- rbind(cal_developed_data, cal_undev_data)
    cal_data <- filter(cal_data, isNotNull(cal_data$status))
    cal_data <- mutate(cal_data,
                       account = ifelse(cal_data$status == "正在开发", 1, 0)
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
                       sums = lit(cal_calc_data$sums)
                       )
    
    # write.parquet(cal_data, "hdfs://192.168.100.137:9000//test/UCBTest/inputParquet/TMInputParquet0820/output/abcde-parquet")
    
    print(head(cal_data))
    unpersist(up01, blocking = FALSE)
    unpersist(up02, blocking = FALSE)
    unpersist(up03, blocking = FALSE)
}

TMUCBCalProcess(
    cal_data_path = "hdfs://192.168.100.137:9000//test/UCBTest/inputParquet/TMInputParquet0820/cal_data",
    weight_path = "hdfs://192.168.100.137:9000//test/UCBTest/inputParquet/TMInputParquet0820/weightages",
    curves_path = "hdfs://192.168.100.137:9000//test/UCBTest/inputParquet/TMInputParquet0820/curves-n"
)