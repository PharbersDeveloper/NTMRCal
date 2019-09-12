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

source("UCBDataBinding.R")
source("UCBCalFuncs.R")

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

    output_dir <- paste0("hdfs://192.168.100.137:9000/tmtest0831/jobs/", jobid, "/output/")
    #jobid <- uuid::UUIDgenerate()
    # ss <- sparkR.session(appName = "UCB-Submit")
    cal_data <- read.parquet(cal_data_path)
    
    weightages <- read.parquet(weight_path)

    curves <- CastCol2Double(read.parquet(curves_path), c("x", "y"))
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
    
    group_by_prc <- function(df) {
        return(list(product=df$product, representative=df$representative, city=df$city))
    }
    
    gb_prc_schema <- structType(
        structField("product", "string"),
        structField("representative", "string"),
        structField("city", "string"),
        structField("potential_dist", "double"),
        structField("sales_dist", "double"),
        structField("hospital_num_dist", "int")
    )
    
    cal_dist_data <- TMAggSchema(cal_data, 
                                 c(list(name=c("potential_m", "p_sales")), "sum",
                                   list(name=c("hospital")), "length"), 
                                 group_by_prc, 
                                 gb_prc_schema)

    group_by_pr <- function(df) {
        return(list(product=df$product, representative=df$representative))
    }
    
    gb_pr_schema <- structType(
        structField("product_mm", "string"),
        structField("representative_m", "string"),
        structField("potential_dist", "double"),
        structField("sales_dist", "double"),
        structField("hospital_num_dist", "int"),
        structField("city_num_dist", "int")
    )
    
    cal_dist_data <- TMAggSchema(cal_dist_data, 
                                 c(list(name=c("potential_dist", "sales_dist", "hospital_num_dist")), "sum",
                                   list(name=c("city")), "length"), 
                                 group_by_pr, 
                                 gb_pr_schema)
    
    cal_dist_data <- CastCol2Double(cal_dist_data, c("hospital_num_dist", "city_num_dist"))

    group_by_p <- function(df) {
        return(list(product=df$product))
    }
    
    gb_p_schema <- structType(
        structField("product_m", "string"),
        structField("sumptt", "double"),
        structField("sumpttm", "double"),
        structField("sumps", "double"),
        structField("sumqt", "double")
    )
    
    cal_market_data <- TMAggSchema(cal_data, 
                                   c(list(name=c("potential", "potential_m", "p_sales", "quota")), "sum"),
                                   group_by_p, 
                                   gb_p_schema)

    cal_data <- join(cal_data, cal_market_data, cal_data$product == cal_market_data$product_m, "inner")
    cal_data <- join(cal_data, cal_dist_data, 
                     cal_data$product == cal_dist_data$product_mm &
                     cal_data$representative == cal_dist_data$representative_m, "inner")

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
    
    cal_calc_data <- head(ColRename(select(cal_data,
                                       sum(cal_data$budget)),
                                c("sum(budget)"),
                                c("total_budget")), 1)

    cal_data <- mutate(cal_data,
                   total_budget = lit(cal_calc_data$total_budget))
    
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
                                             "patient", "status", "rep_num", "hosp_num", "initial_budget", "total_budget", "p_quota", 
                                             "p_budget", "p_sales","pppp_sales", "p_ytd_sales", "quota", "budget", "market_share_m", "sales", 
                                             "ytd_sales")),
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
                                         "patient", "status_m", "rep_num", "hosp_num", "initial_budget", "total_budget", "p_quota", 
                                         "p_budget", "p_sales", "pppp_sales", "p_ytd_sales", "quota", "budget_m", "market_share", "sales", 
                                         "ytd_sales")),
                                c("status_m", "budget_m"), c("status", "budget"))

    cal_data <- rbind(cal_developed_data, cal_undev_data)
    cal_data <- filter(cal_data, isNotNull(cal_data$status))
    cal_data <- mutate(cal_data,
                       account = ifelse(cal_data$status == "正在开发", 1, 0),
                       quota_achv = ifelse(cal_data$quota > 0, cal_data$sales / cal_data$quota, 0)
    )

    persist(cal_data, "MEMORY_ONLY")
    up03 <- cal_data

    cal_calc_data <- head(ColRename(select(cal_data,
                                           sum(cal_data$account),
                                           # sum(cal_data$budget),
                                           sum(cal_data$p_sales),
                                           sum(cal_data$sales)),
                                    c("sum(account)", "sum(p_sales)", "sum(sales)"),
                                    c("new_account", "sumps", "sums")), 1)

    cal_data <- mutate(cal_data,
                       new_account = lit(cal_calc_data$new_account),
                       # total_budget = lit(cal_calc_data$total_budget),
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

    persist(cal_data, "MEMORY_ONLY")
    up_result <- cal_data
    
    ## competitor product area
    group_by_ppa <- function(df) {
        return(list(product_area=df$product_area, product=df$product))
    }
    
    gb_ppa_schema <- structType(
        structField("product_area_m", "string"),
        structField("product", "string"),
        structField("potential", "double")
    )
    
    cal_product_area <- TMAggSchema(cal_data, 
                                   c(list(name=c("potential")), "sum"),
                                   group_by_ppa, 
                                   gb_ppa_schema)
    
    cal_product_area <- select(cal_product_area,
                               "product_area_m", "potential")

    competitor <- read.parquet(competitor_path)
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

    ## final summary report 单周期
    cal_result_summary <- select(cal_data, "representative", "status", "p_sales", "pppp_sales", "sales", "quota", "budget", "account")

    group_by_rs <- function(df) {
        return(list(representative=df$representative))
    }
    
    gb_rs_schema <- structType(
        structField("representative", "string"),
        structField("p_sales", "double"),
        structField("pppp_sales", "double"),
        structField("sales", "double"),
        structField("quota", "double"),
        structField("new_account", "double"),
        structField("budget", "double")
    )
    
    cal_result_summary <- TMAggSchema(cal_result_summary, 
                                 c(list(name=c("p_sales", "pppp_sales", "sales", "quota", "account", "budget")), "sum"),
                                 group_by_rs, 
                                 gb_rs_schema)

    cal_result_summary <- mutate(cal_result_summary, idx=lit(1L))

    group_by_frt <- function(df) {
        return(list(idx=df$idx))
    }
    
    gb_frt_schema <- structType(
        structField("idx", "int"),
        structField("p_sales", "double"),
        structField("pppp_sales", "double"),
        structField("sales", "double"),
        structField("quota", "double"),
        structField("new_account", "double"),
        structField("budget", "double"),
        structField("rep_num", "int")
    )   
    
    cal_result_summary <- TMAggSchema(cal_result_summary, 
                                      c(list(name=c("p_sales", "pppp_sales", "sales", "quota", "new_account", "budget")), "sum",
                                        list(name=c("representative")), "length"),
                                      group_by_frt, 
                                      gb_frt_schema)
    
    cal_result_summary <- mutate(cal_result_summary,
                                 quota_achv = cal_result_summary$sales / cal_result_summary$quota,
                                 sales_force_productivity = cal_result_summary$sales / cal_result_summary$rep_num,
                                 return_on_investment = cal_result_summary$sales / cal_result_summary$budget,
                                 growth_month_on_month = cal_result_summary$sales / cal_result_summary$p_sales - 1.0,
                                 growth_year_on_year = cal_result_summary$sales / cal_result_summary$pppp_sales - 1.0,
                                 job_id = lit(jobid),
                                 project_id = lit(projectid),
                                 period_id = lit(periodid)
    )

    cal_result_summary <- select(cal_result_summary,
                                 c("job_id", "project_id", "period_id", "sales", "quota", "budget", "new_account", "quota_achv",
                                   "growth_month_on_month", "growth_year_on_year",
                                   "sales_force_productivity", "return_on_investment"))

    # # write.parquet(cal_hospital_report, paste0(output_dir, "hospital_report"))
    write.parquet(cal_product_area, paste0(output_dir, "competitor"))
    write.parquet(cal_result_summary, paste0(output_dir, "summary"))
    write.parquet(cal_data, paste0(output_dir, "cal_report"))

    unpersist(up01, blocking = FALSE)
    unpersist(up02, blocking = FALSE)
    unpersist(up03, blocking = FALSE)
    unpersist(up_result, blocking = FALSE)
}
