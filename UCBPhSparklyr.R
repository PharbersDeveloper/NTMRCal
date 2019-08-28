
spark.curves <- phSparklyr.readParquet("/test/UCBTest/inputParquet/curves") %>% 
  phSparklyr.castColType(newColType = "double",
                         colNames = c("x1", "x2", "y1", "y2"))

spark.curves.mm <- spark.curves %>% 
  phSparklyr.group(groupCols = "curve_name", aggExprs = c("min(x1) as min_x1", "max(x2) as max_x2")) %>% 
  phSparklyr.rename(oldColName = "curve_name", newColName = "curve_mm")

## result ----
spark.cal.data <- phSparklyr.readParquet("/test/UCBTest/inputParquet/cal_data") %>% 
  phSparklyr.join(joinDF = phSparklyr.readParquet("/test/UCBTest/inputParquet/weightages"),
                  joinExpr = "TRUE",
                  joinType = "full") %>% 
  phSparklyr.castColType(newColType = "double",
                         colNames = c("potential", "patient", "rep_num", "hosp_num", "initial_budget", 
                                      "p_quota", "p_budget", "p_sales", "pppp_sales", "p_ytd_sales", 
                                      "quota", "budget", "hospital_quota_base_factor_w", 
                                      "hospital_product_quota_growth_factor_w", "district_potential_factor_w1", 
                                      "district_sales_factor_w", "factor2_w", "district_cross_factor_w1", 
                                      "district_hospital_factor_w1", "district_potential_factor_w2", 
                                      "district_cross_factor_w2", "district_hospital_factor_w2")) %>% 
  phSparklyr.mutate(newColName = "total_budget",
                    plugin = "case when product == '开拓来' then initial_budget * 0.6 else initial_budget * 0.12 end") %>% 
  phSparklyr.mutate(newColName = "budget_prop",
                    plugin = "budget / total_budget") %>% 
  phSparklyr.mutate(newColName = "potential_m",
                    plugin = "case when status == '未开发' then 0 else potential end") %>% 
  phSparklyr.mutate(newColName = "potential_factor",
                    plugin = "case when product == '开拓来' then 0.8 else 1 end") %>% 
  phSparklyr.mutate(newColName = "sales_factor",
                    plugin = "case when product == '开拓来' then 0.2 else 0 end") %>% 
  phSparklyr.mutate(newColName = "max_oa",
                    plugin = "case when product == '开拓来' then 100 else 40 end")

spark.dist.data <- spark.cal.data %>% 
  phSparklyr.group(groupCols = c("product", "representative", "city"),
                   aggExprs = c("sum(potential_m) as potential_dist",
                                "sum(p_sales) as sales_dist",
                                "count(hospital) as hospital_num_dist")) %>% 
  phSparklyr.group(groupCols = c("product", "representative"),
                   aggExprs = c("sum(potential_dist) as potential_dist",
                                "sum(sales_dist) as sales_dist",
                                "sum(hospital_num_dist) as hospital_num_dist",
                                "count(city) as city_num_dist")) %>% 
  phSparklyr.rename(oldColName = "product",
                    newColName = "product_m") %>% 
  phSparklyr.rename(oldColName = "representative",
                    newColName = "representative_m")

spark.market.data <- spark.cal.data %>% 
  phSparklyr.group(groupCols = "product",
                   aggExprs = c("sum(potential) as sumptt",
                                "sum(potential_m) as sumpttm",
                                "sum(p_sales) as sumps",
                                "sum(quota) as sumqt")) %>% 
  phSparklyr.rename(oldColName = "product",
                    newColName = "product_m")

spark.cal.data.m <- spark.cal.data %>% 
  phSparklyr.join(joinDF = spark.market.data,
                  joinExpr = "product_m == product",
                  joinType = "left") %>% 
  phSparklyr.dropClumns("product_m") %>% 
  phSparklyr.join(joinDF = spark.dist.data,
                  joinExpr = "product == product_m and representative == representative_m",
                  joinType = "left") %>% 
  phSparklyr.dropClumns("product_m", "representative_m") %>% 
  phSparklyr.mutate(newColName = "potential_contri",
                    plugin = "potential_m / sumpttm") %>% 
  phSparklyr.mutate(newColName = "sales_contri",
                    plugin = "case when sumps == 0 then 0 else p_sales / sumps end") %>% 
  phSparklyr.mutate(newColName = "value_contri",
                    plugin = "potential_contri * potential_factor + sales_contri * sales_factor") %>% 
  phSparklyr.mutate(newColName = "budget_factor",
                    plugin = "budget_prop / value_contri")

# 已开发 & 正在开发
spark.ykf.zzkf <- spark.cal.data.m %>% 
  phSparklyr.filter(filterExpr = "status == '已开发' or status == '正在开发'") %>%
  # oa factor base
  phSparklyr.mutate(newColName = "oa_factor_base_curve",
                    plugin = "case when product == '开拓来' then 'curve02' else 'curve03' end") %>% 
  phSparklyr.join(joinDF = spark.curves.mm,
                  joinExpr = "oa_factor_base_curve == curve_mm",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "x0",
                    plugin = strwrap("case when budget_factor <= min_x1 then min_x1+0.000001 
                                     when budget_factor > max_x2 then max_x2 
                                     else budget_factor end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "x00",
                    plugin = strwrap("case when budget_factor < min_x1 then min_x1 
                                     when budget_factor > max_x2 then max_x2 
                                     else budget_factor end",
                                     width = 10000)) %>% 
  phSparklyr.join(joinDF = spark.curves,
                  joinExpr = "curve_mm == curve_name and x0 > x1 and x0 <= x2",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "oa_factor_base",
                    plugin = "(y2 - y1) / (x2 - x1) * (x00 - x1) + y1") %>% 
  phSparklyr.dropClumns("oa_factor_base_curve", "curve_mm", "min_x1", "max_x2", "x0", "x00", 
                        "curve_name", "x1", "x2", "y1", "y2") %>% 
  # factor 1
  phSparklyr.mutate(newColName = "hospital_quota_base",
                    plugin = strwrap("case when status == '已开发' then 0.4 * p_sales / sumps + 0.6 * potential / sumptt 
                                     else potential / sumptt end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "quota_prop",
                    plugin = "case when sumqt == 0 then 0 else quota / sumqt end") %>% 
  phSparklyr.mutate(newColName = "hospital_quota_base_factor",
                    plugin = "1 - (abs(quota_prop - hospital_quota_base) / hospital_quota_base)") %>% 
  phSparklyr.mutate(newColName = "hospital_quota_base_factor_m",
                    plugin = "case when hospital_quota_base_factor < 0 then 0 else hospital_quota_base_factor end") %>% 
  phSparklyr.mutate(newColName = "hospital_product_quota_growth_factor",
                    plugin = strwrap("case when status == '已开发' then 1 - abs((quota - p_sales)/(sumqt - sumps) 
                                     - (potential_m - p_sales) / (sumptt - sumps)) else 0 end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "hospital_product_quota_growth_factor_m",
                    plugin = "case when hospital_product_quota_growth_factor < 0 then 0 else hospital_product_quota_growth_factor end") %>% 
  phSparklyr.mutate(newColName = "factor1",
                    plugin = strwrap("case when status == '已开发' then hospital_quota_base_factor_m * hospital_quota_base_factor_w 
                                     + hospital_product_quota_growth_factor_m * hospital_product_quota_growth_factor_w 
                                     else hospital_quota_base_factor_m end",
                                     width = 10000)) %>% 
  # factor 2
  phSparklyr.mutate(newColName = "district_cross_factor",
                    plugin = "case when city_num_dist == 1 then 1 else 0 end") %>% 
  phSparklyr.mutate(newColName = "district_potential_factor",
                    plugin = "1 / (abs(rep_num * potential_dist / sumptt - 1) + 1)") %>% 
  phSparklyr.mutate(newColName = "district_sales_factor",
                    plugin = "case when status == '已开发' then 1 / (abs(rep_num * sales_dist / sumps - 1) + 1) else 0 end") %>% 
  phSparklyr.mutate(newColName = "district_hospital_factor",
                    plugin = "1 / (abs(rep_num * hospital_num_dist / hosp_num - 1) + 1)") %>% 
  phSparklyr.mutate(newColName = "factor2",
                    plugin = strwrap("case when status == '已开发' then district_potential_factor * district_potential_factor_w1 * factor2_w 
                                     + district_sales_factor * district_sales_factor_w * factor2_w 
                                     + district_cross_factor * district_cross_factor_w1 
                                     + district_hospital_factor * district_hospital_factor_w1 
                                     else district_potential_factor * district_potential_factor_w2 
                                     + district_cross_factor * district_cross_factor_w2 
                                     + district_hospital_factor * district_hospital_factor_w2 end",
                                     width = 10000)) %>% 
  # oa
  phSparklyr.mutate(newColName = "factor",
                    plugin = "factor1 * factor2") %>% 
  phSparklyr.mutate(newColName = "adjust_factor",
                    plugin = "1 - factor") %>% 
  phSparklyr.mutate(newColName = "p_oa_factor",
                    plugin = "case when status == '已开发' then p_sales / potential_m * 100 else 0 end") %>% 
  phSparklyr.join(joinDF = phSparklyr.filter(spark.curves.mm,
                                             filterExpr = "curve_mm == 'curve09'"),
                  joinExpr = "TRUE",
                  joinType = "full") %>% 
  phSparklyr.mutate(newColName = "x0",
                    plugin = strwrap("case when p_oa_factor <= min_x1 then min_x1+0.000001 
                                     when p_oa_factor > max_x2 then max_x2 
                                     else p_oa_factor end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "x00",
                    plugin = strwrap("case when p_oa_factor < min_x1 then min_x1 
                                     when p_oa_factor > max_x2 then max_x2 
                                     else p_oa_factor end",
                                     width = 10000)) %>% 
  phSparklyr.join(joinDF = spark.curves,
                  joinExpr = "curve_mm == curve_name and x0 > x1 and x0 <= x2",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "p_offer_attractiveness",
                    plugin = "(y2 - y1) / (x2 - x1) * (x00 - x1) + y1") %>% 
  phSparklyr.dropClumns("curve_mm", "min_x1", "max_x2", "x0", "x00", "curve_name", "x1", "x2", "y1", "y2") %>% 
  phSparklyr.mutate(newColName = "p_offer_attractiveness_m",
                    plugin = "case when status == '已开发' then p_offer_attractiveness else rand() * 4 + 3 end") %>% 
  phSparklyr.mutate(newColName = "offer_attractiveness",
                    plugin = strwrap("case when oa_factor_base < 0 then p_offer_attractiveness_m * (1 + oa_factor_base) 
                                     else p_offer_attractiveness_m + (max_oa - p_offer_attractiveness_m) * oa_factor_base end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "offer_attractiveness_adj",
                    plugin = strwrap("case when status == '已开发' then offer_attractiveness * (1 - 0.8 * adjust_factor) 
                                     else offer_attractiveness * (1 - 0.5 * adjust_factor) end",
                                     width = 10000)) %>% 
  # market share
  phSparklyr.mutate(newColName = "market_share_curve",
                    plugin = "case when status == '已开发' then 'curve01' else 'curve05' end") %>% 
  phSparklyr.join(joinDF = spark.curves.mm,
                  joinExpr = "market_share_curve == curve_mm",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "x0",
                    plugin = strwrap("case when offer_attractiveness_adj <= min_x1 then min_x1+0.000001
                                     when offer_attractiveness_adj > max_x2 then max_x2
                                     else offer_attractiveness_adj end",
                                     width = 10000)) %>%
  phSparklyr.mutate(newColName = "x00",
                    plugin = strwrap("case when offer_attractiveness_adj < min_x1 then min_x1
                                     when offer_attractiveness_adj > max_x2 then max_x2
                                     else offer_attractiveness_adj end",
                                     width = 10000)) %>%
  phSparklyr.join(joinDF = spark.curves,
                  joinExpr = "curve_mm == curve_name and x0 > x1 and x0 <= x2",
                  joinType = "left") %>%
  phSparklyr.mutate(newColName = "market_share",
                    plugin = "(y2 - y1) / (x2 - x1) * (x00 - x1) + y1") %>%
  phSparklyr.dropClumns("market_share_curve", "curve_mm", "min_x1", "max_x2", "x0", "x00", "curve_name", "x1", "x2", "y1", "y2") %>%
  phSparklyr.mutate(newColName = "market_share_m",
                    plugin = "case when representative == 0 then 0 else market_share / 100 end") %>% 
  phSparklyr.mutate(newColName = "sales",
                    plugin = "potential_m * market_share_m") %>% 
  phSparklyr.mutate(newColName = "ytd_sales",
                    plugin = "p_ytd_sales + sales") %>% 
  phSparklyr.select("city", "hospital", "hospital_level", "representative", "product", "product_area", "potential", 
                    "patient", "status", "rep_num", "hosp_num", "initial_budget", "p_quota", "p_budget", "p_sales", 
                    "pppp_sales", "p_ytd_sales", "quota", "budget", "market_share_m", "sales", "ytd_sales") %>% 
  phSparklyr.rename(oldColName = "market_share_m",
                    newColName = "market_share")

# 未开发
spark.wkf <- spark.cal.data.m %>% 
  phSparklyr.filter(filterExpr = "status == '未开发'") %>%
  phSparklyr.mutate(newColName = "budget_m",
                    plugin = "budget + p_budget") %>% 
  phSparklyr.mutate(newColName = "market_share",
                    plugin = "0") %>% 
  phSparklyr.mutate(newColName = "sales",
                    plugin = "0") %>% 
  phSparklyr.mutate(newColName = "develop_fee",
                    plugin = "case when hospital_level == '三级' then 12000 when hospital_level == '二级' then 8000 else 3000 end") %>% 
  phSparklyr.mutate(newColName = "status_m",
                    plugin = "case when budget_m >= develop_fee then '正在开发' else '未开发' end") %>% 
  phSparklyr.mutate(newColName = "ytd_sales",
                    plugin = "p_ytd_sales + sales") %>% 
  phSparklyr.select("city", "hospital", "hospital_level", "representative", "product", "product_area", "potential", 
                    "patient", "status_m", "rep_num", "hosp_num", "initial_budget", "p_quota", "p_budget", "p_sales", 
                    "pppp_sales", "p_ytd_sales", "quota", "budget_m", "market_share", "sales", "ytd_sales") %>% 
  phSparklyr.rename(oldColName = "status_m",
                    newColName = "status") %>% 
  phSparklyr.rename(oldColName = "budget_m",
                    newColName = "budget")

spark.result <- phSparklyr.bind(spark.ykf.zzkf, spark.wkf) %>% 
  phSparklyr.filter(filterExpr = "status != null") %>% 
  phSparklyr.mutate(newColName = "account",
                    plugin = "case when status == '正在开发' then 1 else 0 end") %>% 
  phSparklyr.colExprCalc("sum(account) as new_account",
                         "sum(budget) as total_budget",
                         "sum(p_sales) as sumps",
                         "sum(sales) as sums") %>% 
  phSparklyr.mutate(newColName = "quota_achv",
                    plugin = "case when quota is null then 0 else sales / quota end") %>% 
  phSparklyr.mutate(newColName = "sales_increase",
                    plugin = "case when sums > sumps then sums - sumps else 0 end") %>% 
  phSparklyr.mutate(newColName = "next_budget",
                    plugin = "total_budget + 0.06 * sales_increase")

spark.result.parquet <- phSparklyr.saveParquet(spark.result, "/test/UCBTest/output/UCBResult")

## other results
spark.out.hospital.report <- spark.result %>% 
  phSparklyr.group(groupCols = "product",
                   aggExprs = "sum(potential) * (rand() / 100 + 0.01) as potential") %>% 
  phSparklyr.mutate(newColName = "sales",
                    plugin = "potential * (rand() / 100 + 0.015)") %>% 
  phSparklyr.saveParquet("/test/UCBTest/output/HospitalReport")

spark.product.area <- spark.result %>% 
  phSparklyr.group(groupCols = c("product_area", "product"),
                   aggExprs = "sum(potential) as potential") %>% 
  phSparklyr.select("product_area", "potential") %>% 
  phSparklyr.distinct() %>% 
  phSparklyr.rename(oldColName = "product_area",
                    newColName = "product_area_m")

spark.competitor.report <- phSparklyr.readParquet("/test/UCBTest/inputParquet/competitor") %>% 
  phSparklyr.castColType(newColType = "double",
                         colNames = "market_share_c") %>% 
  phSparklyr.join(joinDF = spark.product.area,
                  joinExpr = "product_area == product_area_m",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "market_share",
                    plugin = "market_share_c * (rand() * 0.2 + 0.9)") %>% 
  phSparklyr.mutate(newColName = "sales",
                    plugin = "potential * market_share") %>% 
  phSparklyr.select("product", "market_share", "sales") %>% 
  phSparklyr.saveParquet("/test/UCBTest/output/CompetitorReport")

## final summary report ----
spark.result.summary <- spark.result %>% 
  phSparklyr.select("representative", "status", "p_sales", "pppp_sales", "sales", "quota", "budget") %>% 
  phSparklyr.mutate(newColName = "account",
                    plugin = "case when status == '正在开发' then 1 else 0 end") %>% 
  phSparklyr.group(groupCols = "representative",
                   aggExprs = c("sum(p_sales) as p_sales",
                                "sum(pppp_sales) as pppp_sales",
                                "sum(sales) as sales",
                                "sum(quota) as quota",
                                "sum(account) as new_account",
                                "sum(budget) as budget")) %>% 
  phSparklyr.mutate(newColName = "group",
                    plugin = "1") %>% 
  phSparklyr.group(groupCols = "group",
                   aggExprs = c("sum(p_sales) as p_sales",
                                "sum(pppp_sales) as pppp_sales",
                                "sum(sales) as sales",
                                "sum(quota) as quota",
                                "sum(new_account) as new_account",
                                "sum(budget) as budget",
                                "count(representative) as rep_num"))

spark.current.summary <- spark.result.summary %>% 
  phSparklyr.mutate(newColName = "quota_achv",
                    plugin = "sales / quota") %>% 
  phSparklyr.mutate(newColName = "sales_force_productivity",
                    plugin = "sales / rep_num") %>% 
  phSparklyr.mutate(newColName = "return_on_investment",
                    plugin = "sales / budget") %>% 
  phSparklyr.mutate(newColName = "growth_month_on_month",
                    plugin = "sales / p_sales - 1") %>% 
  phSparklyr.mutate(newColName = "growth_year_on_year",
                    plugin = "sales / pppp_sales - 1") %>% 
  phSparklyr.select("sales", "quota", "budget", "new_account", "quota_achv", 
                    "growth_month_on_month", "growth_year_on_year", 
                    "sales_force_productivity", "return_on_investment") %>% 
  phSparklyr.mutate(newColName = "phase",
                    plugin = "current")

spark.pdata1 <- phSparklyr.readParquet("/test/UCBTest/inputParquet/p_data1") %>% 
  phSparklyr.select("status", "sales", "quota", "budget") %>% 
  phSparklyr.castColType(newColType = "double",
                         colNames = c("sales", "quota", "budget")) %>% 
  phSparklyr.mutate(newColName = "account",
                    plugin = "case when status == '正在开发' then 1 else 0 end") %>% 
  phSparklyr.castColType(newColType = "double",
                         colNames = c("sales", "quota", "budget")) %>% 
  phSparklyr.mutate(newColName = "group",
                    plugin = "1") %>% 
  phSparklyr.group(groupCols = "group",
                   aggExprs = c("sum(sales) as sales",
                                "sum(quota) as quota",
                                "sum(budget) as budget",
                                "sum(account) as new_account")) %>% 
  phSparklyr.mutate(newColName = "rep_num",
                    plugin = "0")

spark.pdata2 <- phSparklyr.readParquet("/test/UCBTest/inputParquet/p_data2") %>% 
  phSparklyr.select("status", "sales", "quota", "budget") %>% 
  phSparklyr.castColType(newColType = "double",
                         colNames = c("sales", "quota", "budget")) %>% 
  phSparklyr.mutate(newColName = "account",
                    plugin = "case when status == '正在开发' then 1 else 0 end") %>% 
  phSparklyr.castColType(newColType = "double",
                         colNames = c("sales", "quota", "budget")) %>% 
  phSparklyr.mutate(newColName = "group",
                    plugin = "1") %>% 
  phSparklyr.group(groupCols = "group",
                   aggExprs = c("sum(sales) as sales",
                                "sum(quota) as quota",
                                "sum(budget) as budget",
                                "sum(account) as new_account")) %>% 
  phSparklyr.mutate(newColName = "rep_num",
                    plugin = "0")

spark.pdata3 <- phSparklyr.readParquet("/test/UCBTest/inputParquet/p_data3") %>%
  phSparklyr.select("status", "sales", "quota", "budget") %>%
  phSparklyr.castColType(newColType = "double",
                         colNames = c("sales", "quota", "budget")) %>%
  phSparklyr.mutate(newColName = "account",
                    plugin = "case when status == '正在开发' then 1 else 0 end") %>%
  phSparklyr.castColType(newColType = "double",
                         colNames = c("sales", "quota", "budget")) %>%
  phSparklyr.mutate(newColName = "group",
                    plugin = "2") %>%
  phSparklyr.group(groupCols = "group",
                   aggExprs = c("sum(sales) as sales",
                                "sum(quota) as quota",
                                "sum(budget) as budget",
                                "sum(account) as new_account")) %>%
  phSparklyr.mutate(newColName = "rep_num",
                    plugin = "0")

spark.pdata4 <- phSparklyr.readParquet("/test/UCBTest/inputParquet/p_data4") %>%
  phSparklyr.select("status", "sales", "quota", "budget") %>%
  phSparklyr.castColType(newColType = "double",
                         colNames = c("sales", "quota", "budget")) %>%
  phSparklyr.mutate(newColName = "account",
                    plugin = "case when status == '正在开发' then 1 else 0 end") %>%
  phSparklyr.castColType(newColType = "double",
                         colNames = c("sales", "quota", "budget")) %>%
  phSparklyr.mutate(newColName = "group",
                    plugin = "2") %>%
  phSparklyr.group(groupCols = "group",
                   aggExprs = c("sum(sales) as sales",
                                "sum(quota) as quota",
                                "sum(budget) as budget",
                                "sum(account) as new_account")) %>%
  phSparklyr.mutate(newColName = "rep_num",
                    plugin = "0")

spark.pdata5 <- phSparklyr.readParquet("/test/UCBTest/inputParquet/p_data5") %>%
  phSparklyr.select("status", "sales", "quota", "budget") %>%
  phSparklyr.castColType(newColType = "double",
                         colNames = c("sales", "quota", "budget")) %>%
  phSparklyr.mutate(newColName = "account",
                    plugin = "case when status == '正在开发' then 1 else 0 end") %>%
  phSparklyr.castColType(newColType = "double",
                         colNames = c("sales", "quota", "budget")) %>%
  phSparklyr.mutate(newColName = "group",
                    plugin = "2") %>%
  phSparklyr.group(groupCols = "group",
                   aggExprs = c("sum(sales) as sales",
                                "sum(quota) as quota",
                                "sum(budget) as budget",
                                "sum(account) as new_account")) %>%
  phSparklyr.mutate(newColName = "rep_num",
                    plugin = "0")

spark.pdata6 <- phSparklyr.readParquet("/test/UCBTest/inputParquet/p_data6") %>%
  phSparklyr.select("status", "sales", "quota", "budget") %>%
  phSparklyr.castColType(newColType = "double",
                         colNames = c("sales", "quota", "budget")) %>%
  phSparklyr.mutate(newColName = "account",
                    plugin = "case when status == '正在开发' then 1 else 0 end") %>%
  phSparklyr.castColType(newColType = "double",
                         colNames = c("sales", "quota", "budget")) %>%
  phSparklyr.mutate(newColName = "group",
                    plugin = "2") %>%
  phSparklyr.group(groupCols = "group",
                   aggExprs = c("sum(sales) as sales",
                                "sum(quota) as quota",
                                "sum(budget) as budget",
                                "sum(account) as new_account")) %>%
  phSparklyr.mutate(newColName = "rep_num",
                    plugin = "0")

spark.final.chain <- spark.pdata3 %>% 
  phSparklyr.bind(spark.pdata4) %>% 
  phSparklyr.bind(spark.pdata5) %>% 
  phSparklyr.group(groupCols = "group",
                   aggExprs = "sum(sales) as chain_sales") %>% 
  phSparklyr.dropClumns("group")

spark.final.year <- spark.pdata4 %>% 
  phSparklyr.bind(spark.pdata5) %>% 
  phSparklyr.bind(spark.pdata6) %>% 
  phSparklyr.group(groupCols = "group",
                   aggExprs = "sum(sales) as year_sales") %>% 
  phSparklyr.dropClumns("group")

spark.final.summary <- spark.result.summary %>% 
  phSparklyr.bind(spark.pdata1) %>% 
  phSparklyr.bind(spark.pdata2) %>% 
  phSparklyr.group(groupCols = "group",
                   aggExprs = c("sum(sales) as sales",
                                "sum(quota) as quota",
                                "sum(budget) as budget",
                                "sum(new_account) as new_account",
                                "sum(rep_num) as rep_num")) %>% 
  phSparklyr.mutate(newColName = "quota_achv",
                    plugin = "sales / quota") %>% 
  phSparklyr.mutate(newColName = "sales_force_productivity",
                    plugin = "sales / rep_num") %>% 
  phSparklyr.mutate(newColName = "return_on_investment",
                    plugin = "sales / budget") %>% 
  phSparklyr.join(joinDF = spark.final.chain,
                  joinExpr = "TRUE",
                  joinType = "full") %>% 
  phSparklyr.mutate(newColName = "growth_month_on_month",
                    plugin = "sales / chain_sales - 1") %>% 
  phSparklyr.join(joinDF = spark.final.year,
                  joinExpr = "TRUE",
                  joinType = "full") %>% 
  phSparklyr.mutate(newColName = "growth_year_on_year",
                    plugin = "sales / year_sales - 1") %>% 
  phSparklyr.select("sales", "quota", "budget", "new_account", "quota_achv", 
                    "growth_month_on_month", "growth_year_on_year", 
                    "sales_force_productivity", "return_on_investment") %>% 
  phSparklyr.mutate(newColName = "phase",
                    plugin = "total")

spark.summary <- phSparklyr.bind(spark.current.summary, spark.final.summary) %>% 
  phSparklyr.saveParquet("/test/UCBTest/output/FinalSummary")

## json ----
Json(phSparklyr.execute(spark.result.parquet, spark.out.hospital.report, 
                        spark.competitor.report, spark.summary),
     path = "UCBtest/UCBtest.json")









