
spark.curves <- phSparklyr.readParquet("/test/TMTest/inputParquet/curves") %>% 
  phSparklyr.castColType(newColType = "double",
                         colNames = c("x1", "x2", "y1", "y2"))

spark.curves.mm <- spark.curves %>% 
  phSparklyr.group(groupCols = "curve_name", aggExprs = c("min(x1) as min_x1", "max(x2) as max_x2")) %>% 
  phSparklyr.rename(oldColName = "curve_name", newColName = "curve_mm")

## sales ----
spark.result <- phSparklyr.readParquet("/test/TMTest/inputParquet/cal_data") %>% 
  ## weightages, manager info
  phSparklyr.join(joinDF = phSparklyr.readParquet("/test/TMTest/inputParquet/weightages"),
                  joinExpr = "TRUE",
                  joinType = "full") %>% 
  phSparklyr.join(joinDF = phSparklyr.readParquet("/test/TMTest/inputParquet/manager"),
                  joinExpr = "TRUE",
                  joinType = "full") %>% 
  phSparklyr.castColType(newColType = "double",
                         colNames = c("budget", "meeting_attendance", "quota", "call_time", 
                                      "one_on_one_coaching", "field_work",	"performance_review",	
                                      "product_knowledge_training",	"territory_management_training", 
                                      "sales_skills_training",	"career_development_guide",	
                                      "employee_kpi_and_compliance_check", "admin_work",	
                                      "kol_management",	"business_strategy_planning",	
                                      "team_meeting",	"potential", "p_sales", "p_quota", 
                                      "p_share", "representative_time",	"p_territory_management_ability",	
                                      "p_sales_skills",	"p_product_knowledge", "p_behavior_efficiency",	
                                      "p_work_motivation", "p_target", "p_target_coverage", "p_high_target", 
                                      "p_middle_target", "p_low_target", "territory_management_ability_w", 
                                      "sales_skills_w",	"product_knowledge_w", "behavior_efficiency_w", 
                                      "work_motivation_w", "general_ability_w", "call_time_index_w", 
                                      "quota_restriction_index_w", "business_strategy_planning_index_w", 
                                      "admin_work_index_w",	"employee_kpi_and_compliance_check_index_w", 
                                      "team_meeting_index_w",	"kol_management_index_w",	
                                      "rep_ability_efficiency_w",	"field_work_index_w",	
                                      "deployment_quality_w",	"budget_factor_w", 
                                      "meeting_attendance_factor_w", "sales_performance_w",	
                                      "customer_relationship_w", "total_kpi",	"manager_time",	
                                      "total_budget",	"total_quota", "total_place")) %>% 
  ## level
  phSparklyr.colExprCalc("sum(potential) as total_potential",
                         "sum(p_sales) as total_p_sales") %>% 
  phSparklyr.mutate(newColName = "level_factor",
                    plugin = "0.8 * potential / total_potential + 0.2 * p_sales / total_p_sales") %>% 
  phSparklyr.mutate(newColName = "level",
                    plugin = "case when level_factor > 0.15 then 3 when level_factor <= 0.05 then 1 else 2 end") %>% 
  ## rep ability
  phSparklyr.mutate(newColName = "work_motivation",
                    plugin = strwrap("p_work_motivation + (10 - p_work_motivation) * 0.15 
                                     * (performance_review + career_development_guide)",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "territory_management_ability",
                    plugin = strwrap("p_territory_management_ability + (10 - p_territory_management_ability) * 0.3 
                                     * territory_management_training",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "sales_skills",
                    plugin = "p_sales_skills + (10 - p_sales_skills) * 0.3 * sales_skills_training") %>% 
  phSparklyr.mutate(newColName = "product_knowledge",
                    plugin = strwrap("p_product_knowledge + (10 - p_product_knowledge) * 0.3 
                                     * product_knowledge_training",
                                     width = 10000)) %>% 
  phSparklyr.join(joinDF = phSparklyr.filter(spark.curves.mm,
                                             filterExpr = "curve_mm == 'curve09'"),
                  joinExpr = "TRUE",
                  joinType = "full") %>% 
  phSparklyr.mutate(newColName = "x0",
                    plugin = strwrap("case when one_on_one_coaching <= min_x1 then min_x1+0.000001 
                                     when one_on_one_coaching > max_x2 then max_x2 
                                     else one_on_one_coaching end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "x00",
                    plugin = strwrap("case when one_on_one_coaching < min_x1 then min_x1 
                                     when one_on_one_coaching > max_x2 then max_x2 
                                     else one_on_one_coaching end",
                                     width = 10000)) %>% 
  phSparklyr.join(joinDF = spark.curves,
                  joinExpr = "curve_mm == curve_name and x0 > x1 and x0 <= x2",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "behavior_efficiency_factor",
                    plugin = "(y2 - y1) / (x2 - x1) * (x00 - x1) + y1") %>% 
  phSparklyr.dropClumns("curve_mm", "min_x1", "max_x2", "x0", "x00", "curve_name", "x1", "x2", "y1", "y2") %>% 
  phSparklyr.mutate(newColName = "behavior_efficiency",
                    plugin = strwrap("p_behavior_efficiency + (10 - p_behavior_efficiency) * 0.3 
                                     * behavior_efficiency_factor",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "general_ability",
                    plugin = strwrap("(territory_management_ability * territory_management_ability_w + sales_skills * sales_skills_w 
                                     + product_knowledge * product_knowledge_w + behavior_efficiency * behavior_efficiency_w 
                                     + work_motivation * work_motivation_w) * 10",
                                     width = 10000)) %>% 
  ## call time
  phSparklyr.mutate(newColName = "call_time_index_curve",
                    plugin = "case when level == 1 then 'curve10' when level == 2 then 'curve11' else 'curve12' end") %>% 
  phSparklyr.join(joinDF = spark.curves.mm,
                  joinExpr = "call_time_index_curve == curve_mm",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "x0",
                    plugin = "case when call_time <= min_x1 then min_x1+0.000001 when call_time > max_x2 then max_x2 else call_time end") %>% 
  phSparklyr.mutate(newColName = "x00",
                    plugin = "case when call_time < min_x1 then min_x1 when call_time > max_x2 then max_x2 else call_time end") %>% 
  phSparklyr.join(joinDF = spark.curves,
                  joinExpr = "curve_mm == curve_name and x0 > x1 and x0 <= x2",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "call_time_index",
                    plugin = "(y2 - y1) / (x2 - x1) * (x00 - x1) + y1") %>% 
  phSparklyr.dropClumns("call_time_index_curve", "curve_mm", "min_x1", "max_x2", "x0", "x00", 
                        "curve_name", "x1", "x2", "y1", "y2") %>% 
  ## quota restriction
  phSparklyr.mutate(newColName = "quota_growth",
                    plugin = "quota / p_sales") %>% 
  phSparklyr.mutate(newColName = "quota_restriction_factor",
                    plugin = "case when quota_growth >= 0.5 and quota_growth <= 2 then 1 else 0.6 end") %>% 
  phSparklyr.join(joinDF = phSparklyr.filter(spark.curves.mm,
                                             filterExpr = "curve_mm == 'curve14'"),
                  joinExpr = "TRUE",
                  joinType = "full") %>% 
  phSparklyr.mutate(newColName = "x0",
                    plugin = strwrap("case when quota_restriction_factor <= min_x1 then min_x1+0.000001 
                                     when quota_restriction_factor > max_x2 then max_x2 
                                     else quota_restriction_factor end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "x00",
                    plugin = strwrap("case when quota_restriction_factor < min_x1 then min_x1 
                                     when quota_restriction_factor > max_x2 then max_x2 
                                     else quota_restriction_factor end",
                                     width = 10000)) %>% 
  phSparklyr.join(joinDF = spark.curves,
                  joinExpr = "curve_mm == curve_name and x0 > x1 and x0 <= x2",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "quota_restriction_index",
                    plugin = "(y2 - y1) / (x2 - x1) * (x00 - x1) + y1") %>% 
  phSparklyr.dropClumns("curve_mm", "min_x1", "max_x2", "x0", "x00", "curve_name", "x1", "x2", "y1", "y2") %>% 
  ## rep efficiency
  phSparklyr.mutate(newColName = "rep_ability_efficiency",
                    plugin = strwrap("general_ability * general_ability_w + call_time_index * call_time_index_w 
                                     + quota_restriction_index * quota_restriction_index_w",
                                     width = 10000)) %>% 
  ## field work
  phSparklyr.join(joinDF = phSparklyr.filter(spark.curves.mm,
                                             filterExpr = "curve_mm == 'curve16'"),
                  joinExpr = "TRUE",
                  joinType = "full") %>% 
  phSparklyr.mutate(newColName = "x0",
                    plugin = "case when field_work <= min_x1 then min_x1+0.000001 when field_work > max_x2 then max_x2 else field_work end") %>% 
  phSparklyr.mutate(newColName = "x00",
                    plugin = "case when field_work < min_x1 then min_x1 when field_work > max_x2 then max_x2 else field_work end") %>% 
  phSparklyr.join(joinDF = spark.curves,
                  joinExpr = "curve_mm == curve_name and x0 > x1 and x0 <= x2",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "field_work_index",
                    plugin = "(y2 - y1) / (x2 - x1) * (x00 - x1) + y1") %>% 
  phSparklyr.dropClumns("curve_mm", "min_x1", "max_x2", "x0", "x00", "curve_name", "x1", "x2", "y1", "y2") %>% 
  # strategy planning
  phSparklyr.join(joinDF = phSparklyr.filter(spark.curves.mm,
                                             filterExpr = "curve_mm == 'curve18'"),
                  joinExpr = "TRUE",
                  joinType = "full") %>% 
  phSparklyr.mutate(newColName = "xx1",
                    plugin = strwrap("case when business_strategy_planning <= min_x1 then min_x1+0.000001 
                                     when business_strategy_planning > max_x2 then max_x2 
                                     else business_strategy_planning end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "xx10",
                    plugin = strwrap("case when business_strategy_planning < min_x1 then min_x1 
                                     when business_strategy_planning > max_x2 then max_x2 
                                     else business_strategy_planning end",
                                     width = 10000)) %>% 
  phSparklyr.join(joinDF = spark.curves,
                  joinExpr = "curve_mm == curve_name and xx1 > x1 and xx1 <= x2",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "business_strategy_planning_index",
                    plugin = "(y2 - y1) / (x2 - x1) * (xx10 - x1) + y1") %>% 
  phSparklyr.dropClumns("xx1", "xx10", "curve_name", "x1", "x2", "y1", "y2") %>% 
  # admin work
  phSparklyr.mutate(newColName = "xx2",
                    plugin = strwrap("case when admin_work <= min_x1 then min_x1+0.000001 
                                     when admin_work > max_x2 then max_x2 
                                     else admin_work end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "xx20",
                    plugin = strwrap("case when admin_work < min_x1 then min_x1 
                                     when admin_work > max_x2 then max_x2 
                                     else admin_work end",
                                     width = 10000)) %>% 
  phSparklyr.join(joinDF = spark.curves,
                  joinExpr = "curve_mm == curve_name and xx2 > x1 and xx2 <= x2",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "admin_work_index",
                    plugin = "(y2 - y1) / (x2 - x1) * (xx20 - x1) + y1") %>% 
  phSparklyr.dropClumns("xx2", "xx20", "curve_name", "x1", "x2", "y1", "y2") %>% 
  # kpi check
  phSparklyr.mutate(newColName = "xx3",
                    plugin = strwrap("case when employee_kpi_and_compliance_check <= min_x1 then min_x1+0.000001 
                                     when employee_kpi_and_compliance_check > max_x2 then max_x2 
                                     else employee_kpi_and_compliance_check end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "xx30",
                    plugin = strwrap("case when employee_kpi_and_compliance_check < min_x1 then min_x1 
                                     when employee_kpi_and_compliance_check > max_x2 then max_x2 
                                     else employee_kpi_and_compliance_check end",
                                     width = 10000)) %>% 
  phSparklyr.join(joinDF = spark.curves,
                  joinExpr = "curve_mm == curve_name and xx3 > x1 and xx3 <= x2",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "employee_kpi_and_compliance_check_index",
                    plugin = "(y2 - y1) / (x2 - x1) * (xx30 - x1) + y1") %>% 
  phSparklyr.dropClumns("xx3", "xx30", "curve_name", "x1", "x2", "y1", "y2") %>% 
  # teem meeting
  phSparklyr.mutate(newColName = "xx4",
                    plugin = strwrap("case when team_meeting <= min_x1 then min_x1+0.000001 
                                     when team_meeting > max_x2 then max_x2 
                                     else team_meeting end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "xx40",
                    plugin = strwrap("case when team_meeting < min_x1 then min_x1 
                                     when team_meeting > max_x2 then max_x2 
                                     else team_meeting end",
                                     width = 10000)) %>% 
  phSparklyr.join(joinDF = spark.curves,
                  joinExpr = "curve_mm == curve_name and xx4 > x1 and xx4 <= x2",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "team_meeting_index",
                    plugin = "(y2 - y1) / (x2 - x1) * (xx40 - x1) + y1") %>% 
  phSparklyr.dropClumns("xx4", "xx40", "curve_name", "x1", "x2", "y1", "y2") %>% 
  # kol management
  phSparklyr.mutate(newColName = "xx5",
                    plugin = strwrap("case when kol_management <= min_x1 then min_x1+0.000001 
                                     when kol_management > max_x2 then max_x2 
                                     else kol_management end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "xx50",
                    plugin = strwrap("case when kol_management < min_x1 then min_x1 
                                     when kol_management > max_x2 then max_x2 
                                     else kol_management end",
                                     width = 10000)) %>% 
  phSparklyr.join(joinDF = spark.curves,
                  joinExpr = "curve_mm == curve_name and xx5 > x1 and xx5 <= x2",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "kol_management_index",
                    plugin = "(y2 - y1) / (x2 - x1) * (xx5 - x1) + y1") %>% 
  phSparklyr.dropClumns("curve_mm", "min_x1", "max_x2", "xx5", "xx50", "curve_name", "x1", "x2", "y1", "y2") %>% 
  ## manager decision
  phSparklyr.mutate(newColName = "deployment_quality",
                    plugin = strwrap("business_strategy_planning_index * business_strategy_planning_index_w 
                                     + admin_work_index * admin_work_index_w 
                                     + employee_kpi_and_compliance_check_index * employee_kpi_and_compliance_check_index_w 
                                     + team_meeting_index * team_meeting_index_w 
                                     + kol_management_index * kol_management_index_w",
                                     width = 10000)) %>% 
  ## sales performance
  phSparklyr.mutate(newColName = "sales_performance",
                    plugin = strwrap("rep_ability_efficiency * rep_ability_efficiency_w 
                                     + field_work_index * field_work_index_w 
                                     + deployment_quality * deployment_quality_w",
                                     width = 10000)) %>% 
  ## budget
  phSparklyr.mutate(newColName = "budget_prop",
                    plugin = "budget / total_budget * 100") %>% 
  phSparklyr.mutate(newColName = "budget_factor_curve",
                    plugin = "case when level == 1 then 'curve02' when level == 2 then 'curve03' else 'curve04' end") %>% 
  phSparklyr.join(joinDF = spark.curves.mm,
                  joinExpr = "budget_factor_curve == curve_mm",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "x0",
                    plugin = strwrap("case when budget_prop <= min_x1 then min_x1+0.000001 
                                     when budget_prop > max_x2 then max_x2 
                                     else budget_prop end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "x00",
                    plugin = strwrap("case when budget_prop < min_x1 then min_x1 
                                     when budget_prop > max_x2 then max_x2 
                                     else budget_prop end",
                                     width = 10000)) %>% 
  phSparklyr.join(joinDF = spark.curves,
                  joinExpr = "curve_mm == curve_name and x0 > x1 and x0 <= x2",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "budget_factor",
                    plugin = "(y2 - y1) / (x2 - x1) * (x00 - x1) + y1") %>% 
  phSparklyr.dropClumns("budget_factor_curve", "curve_mm", "min_x1", "max_x2", "x0", "x00", 
                        "curve_name", "x1", "x2", "y1", "y2") %>% 
  ## meeting attendance
  phSparklyr.mutate(newColName = "meeting_attendance_factor_curve",
                    plugin = "case when level == 1 then 'curve05' when level == 2 then 'curve06' else 'curve07' end") %>% 
  phSparklyr.join(joinDF = spark.curves.mm,
                  joinExpr = "meeting_attendance_factor_curve == curve_mm",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "x0",
                    plugin = strwrap("case when meeting_attendance <= min_x1 then min_x1+0.000001 
                                     when meeting_attendance > max_x2 then max_x2 
                                     else meeting_attendance end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "x00",
                    plugin = strwrap("case when meeting_attendance < min_x1 then min_x1 
                                     when meeting_attendance > max_x2 then max_x2 
                                     else meeting_attendance end",
                                     width = 10000)) %>% 
  phSparklyr.join(joinDF = spark.curves,
                  joinExpr = "curve_mm == curve_name and x0 > x1 and x0 <= x2",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "meeting_attendance_factor",
                    plugin = "(y2 - y1) / (x2 - x1) * (x00 - x1) + y1") %>% 
  phSparklyr.dropClumns("meeting_attendance_factor_curve", "curve_mm", "min_x1", "max_x2", "x0", "x00", 
                        "curve_name", "x1", "x2", "y1", "y2") %>% 
  ## customer relationship
  phSparklyr.mutate(newColName = "customer_relationship",
                    plugin = strwrap("(budget_factor * budget_factor_w + meeting_attendance_factor 
                                     * meeting_attendance_factor_w) * 100",
                                     width = 10000)) %>% 
  ## oa
  phSparklyr.mutate(newColName = "offer_attractiveness",
                    plugin = strwrap("sales_performance * sales_performance_w + customer_relationship 
                                     * customer_relationship_w",
                                     width = 10000)) %>% 
  ## share delta
  phSparklyr.join(joinDF = phSparklyr.filter(spark.curves.mm,
                                             filterExpr = "curve_mm == 'curve28'"),
                  joinExpr = "TRUE",
                  joinType = "full") %>% 
  phSparklyr.mutate(newColName = "x0",
                    plugin = strwrap("case when offer_attractiveness <= min_x1 then min_x1+0.000001 
                                     when offer_attractiveness > max_x2 then max_x2 
                                     else offer_attractiveness end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "x00",
                    plugin = strwrap("case when offer_attractiveness < min_x1 then min_x1 
                                     when offer_attractiveness > max_x2 then max_x2 
                                     else offer_attractiveness end",
                                     width = 10000)) %>% 
  phSparklyr.join(joinDF = spark.curves,
                  joinExpr = "curve_mm == curve_name and x0 > x1 and x0 <= x2",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "share_delta_factor",
                    plugin = "(y2 - y1) / (x2 - x1) * (x00 - x1) + y1") %>% 
  phSparklyr.dropClumns("curve_mm", "min_x1", "max_x2", "x0", "x00", "curve_name", "x1", "x2", "y1", "y2") %>% 
  ## share, sales
  phSparklyr.mutate(newColName = "share",
                    plugin = "p_share * (1 + share_delta_factor)") %>% 
  phSparklyr.mutate(newColName = "sales",
                    plugin = "potential / 4 * share")

spark.rep.result <- spark.result %>% 
  phSparklyr.group(groupCols = "representative_id",
                   aggExprs = c("sum(sales) as rep_sales",
                                "sum(quota) as rep_quota")) %>% 
  phSparklyr.rename(oldColName = "representative_id",
                    newColName = "representative_id_m")

spark.result.m <- spark.result %>% 
  phSparklyr.join(joinDF = spark.rep.result,
                  joinExpr = "representative_id == representative_id_m",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "rep_quota_achv",
                    plugin = "rep_sales / rep_quota") %>% 
  phSparklyr.mutate(newColName = "work_motivation_m",
                    plugin = strwrap("case when rep_quota_achv >=  0.9 and rep_quota_achv <= 1.2 then work_motivation + (10 - work_motivation) * 0.2 
                                     else work_motivation end",
                                     width = 10000)) %>% 
  phSparklyr.dropClumns("representative_id_m", "work_motivation") %>% 
  phSparklyr.rename(oldColName = "work_motivation_m",
                    newColName = "work_motivation") %>% 
  phSparklyr.rename(oldColName = "p_target",
                    newColName = "target") %>% 
  phSparklyr.rename(oldColName = "p_target_coverage",
                    newColName = "target_coverage") %>% 
  phSparklyr.mutate(newColName = "class1",
                    plugin = strwrap("case when behavior_efficiency >= 0 and behavior_efficiency < 3 then 1 
                                     when behavior_efficiency >= 3 and behavior_efficiency < 6 then 2 
                                     when behavior_efficiency >= 6 and behavior_efficiency < 8 then 3 
                                     else 4 end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "target_coverage",
                    plugin = strwrap("case when class1 == 1 then target_coverage - cast((rand() * 5 + 5) as int) 
                                     when class1 == 2 then target_coverage - cast((rand() * 5) as int) 
                                     when class1 == 3 then target_coverage + cast((rand() * 5) as int) 
                                     else target_coverage + cast((rand() * 5 + 5) as int) end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "class2",
                    plugin = strwrap("case when work_motivation >= 0 and work_motivation < 3 then 1 
                                     when work_motivation >= 3 and work_motivation < 6 then 2 
                                     when work_motivation >= 6 and work_motivation < 8 then 3 
                                     else 4 end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "high_target_m",
                    plugin = strwrap("case when class1 == 1 then cast((rand() + 13) as int) 
                                     when class1 == 2 then cast((rand() + 14) as int) 
                                     when class1 == 3 then cast((rand() * 2 + 16) as int) 
                                     else cast((rand() * 3 + 19) as int) end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "high_target",
                    plugin = strwrap("case when class2 == 1 then high_target_m - cast((rand() + 1) as int) 
                                     when class2 == 2 then high_target_m - cast((rand()) as int) 
                                     when class2 == 3 then high_target_m + cast((rand()) as int) 
                                     else high_target_m + 1 end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "middle_target_m",
                    plugin = strwrap("case when class1 == 1 then cast((rand() + 13) as int) 
                                     when class1 == 2 then cast((rand() + 13) as int) 
                                     when class1 == 3 then cast((rand() + 12) as int) 
                                     else cast((rand() + 12) as int) end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "middle_target",
                    plugin = strwrap("case when class2 == 1 then middle_target_m - 2 
                                     when class2 == 2 then middle_target_m - 1 
                                     when class2 == 3 then middle_target_m + cast((rand()) as int) 
                                     else middle_target_m + 1 end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "low_target_m",
                    plugin = strwrap("case when class1 == 1 then cast((rand() + 13) as int) 
                                     when class1 == 2 then cast((rand() + 12) as int) 
                                     when class1 == 3 then cast((rand() + 12) as int) 
                                     else cast((rand() + 11) as int) end",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "low_target",
                    plugin = strwrap("case when class2 == 1 then low_target_m - 2 
                                     when class2 == 2 then low_target_m - 1 
                                     when class2 == 3 then low_target_m + cast((rand()) as int) 
                                     else low_target_m + 1 end",
                                     width = 10000)) %>% 
  phSparklyr.select("hospital", "hospital_level", "budget", "meeting_attendance", "product", "quota", "call_time",
                    "one_on_one_coaching", "field_work", "performance_review", "product_knowledge_training",
                    "territory_management_training", "representative", "sales_skills_training", "career_development_guide",
                    "employee_kpi_and_compliance_check", "admin_work", "kol_management", "business_strategy_planning",
                    "team_meeting", "potential", "p_sales", "p_quota", "p_share", "life_cycle", "representative_time",
                    "p_territory_management_ability", "p_sales_skills", "p_product_knowledge", "p_behavior_efficiency",
                    "p_work_motivation", "total_potential", "total_p_sales", "total_quota", "total_place", "manager_time", 
                    "work_motivation", "territory_management_ability", "sales_skills", 
                    "product_knowledge", "behavior_efficiency", "general_ability", "target", "target_coverage", 
                    "high_target", "middle_target", "low_target", "share", "sales")

spark.result.output <- spark.result.m %>% 
  phSparklyr.saveParquet("/test/TMTest/output/TMResult")

## competitor ----
spark.competitor.report <- phSparklyr.readParquet("/test/TMTest/inputParquet/competitor") %>% 
  phSparklyr.castColType(newColType = "double",
                         colNames = "p_share") %>% 
  phSparklyr.join(joinDF = phSparklyr.distinct(phSparklyr.select(spark.result.m, "total_potential")),
                  joinExpr = "TRUE",
                  joinType = "full") %>%
  phSparklyr.mutate(newColName = "p_sales",
                    plugin = "total_potential / 4 * p_share") %>%
  phSparklyr.mutate(newColName = "share",
                    plugin = "p_share * (rand() / 5 + 0.9)") %>%
  phSparklyr.mutate(newColName = "sales",
                    plugin = "total_potential / 4 * share") %>%
  phSparklyr.mutate(newColName = "sales_growth",
                    plugin = "sales / p_sales - 1") %>%
  phSparklyr.select("product", "sales", "share", "sales_growth") %>% 
  phSparklyr.saveParquet("/test/TMTest/output/TMCompetitor")

## assessment ----
spark.region.division <- spark.result %>% 
  phSparklyr.group(groupCols = c("representative", "general_ability", "total_potential", "total_p_sales"),
                   aggExprs = c("sum(potential) as potential",
                                "sum(p_sales) as p_sales")) %>% 
  phSparklyr.colExprCalc("sum(general_ability - 50) as sumga_scale") %>% 
  phSparklyr.mutate(newColName = "potential_prop",
                    plugin = "potential / total_potential") %>% 
  phSparklyr.mutate(newColName = "p_sales_prop",
                    plugin = "p_sales / total_p_sales") %>% 
  phSparklyr.mutate(newColName = "ptt_ps_prop",
                    plugin = "0.6 * potential_prop + 0.4 * p_sales_prop") %>% 
  phSparklyr.mutate(newColName = "ga_prop",
                    plugin = "(general_ability - 50) / sumga_scale") %>% 
  phSparklyr.mutate(newColName = "score_s",
                    plugin = "abs(ptt_ps_prop - ga_prop)") %>% 
  phSparklyr.colExprCalc("mean(score_s) as score") %>% 
  phSparklyr.select("score") %>% 
  phSparklyr.distinct() %>% 
  phSparklyr.mutate(newColName = "index_m",
                    plugin = "'region_division'")

spark.target.assigns <- spark.result %>% 
  phSparklyr.select("potential", "p_sales", "quota", "sales", "total_potential", "total_p_sales", "total_quota") %>% 
  phSparklyr.mutate(newColName = "potential_prop",
                    plugin = "potential / total_potential") %>% 
  phSparklyr.mutate(newColName = "p_sales_prop",
                    plugin = "p_sales / total_p_sales") %>% 
  phSparklyr.mutate(newColName = "ptt_ps_prop",
                    plugin = "0.6 * potential_prop + 0.4 * p_sales_prop") %>% 
  phSparklyr.mutate(newColName = "quota_prop",
                    plugin = "quota / total_quota") %>% 
  phSparklyr.mutate(newColName = "ptt_ps_score",
                    plugin = "abs(ptt_ps_prop - quota_prop)") %>% 
  phSparklyr.mutate(newColName = "quota_growth",
                    plugin = "quota - p_sales") %>% 
  phSparklyr.mutate(newColName = "sales_growth",
                    plugin = "sales - p_sales") %>% 
  phSparklyr.colExprCalc("sum(sales) as total_sales") %>% 
  phSparklyr.mutate(newColName = "qg_prop",
                    plugin = "quota_growth / (total_quota - total_p_sales)") %>% 
  phSparklyr.mutate(newColName = "sg_prop",
                    plugin = "sales_growth / (total_sales - total_p_sales)") %>% 
  phSparklyr.mutate(newColName = "q_s_score",
                    plugin = "abs(qg_prop - sg_prop)") %>% 
  phSparklyr.colExprCalc("0.7 * mean(ptt_ps_score) + 0.3 * mean(q_s_score) as score") %>% 
  phSparklyr.select("score") %>% 
  phSparklyr.distinct() %>% 
  phSparklyr.mutate(newColName = "index_m",
                    plugin = "'target_assigns'")

spark.resource.assigns <- spark.result %>% 
  phSparklyr.select("potential", "p_sales", "budget", "call_time", "representative_time", "meeting_attendance", 
                    "total_potential", "total_p_sales", "total_budget", "total_place") %>% 
  phSparklyr.mutate(newColName = "potential_prop",
                    plugin = "potential / total_potential") %>% 
  phSparklyr.mutate(newColName = "p_sales_prop",
                    plugin = "p_sales / total_p_sales") %>% 
  phSparklyr.mutate(newColName = "ptt_ps_prop",
                    plugin = "0.6 * potential_prop + 0.4 * p_sales_prop") %>% 
  phSparklyr.mutate(newColName = "budget_prop",
                    plugin = "budget / total_budget") %>% 
  phSparklyr.mutate(newColName = "time_prop",
                    plugin = "call_time / (representative_time * 5)") %>% 
  phSparklyr.mutate(newColName = "place_prop",
                    plugin = "meeting_attendance / total_place") %>% 
  phSparklyr.mutate(newColName = "budget_score",
                    plugin = "abs(ptt_ps_prop - budget_prop)") %>% 
  phSparklyr.mutate(newColName = "time_score",
                    plugin = "abs(ptt_ps_prop - time_prop)") %>% 
  phSparklyr.mutate(newColName = "place_score",
                    plugin = "abs(ptt_ps_prop - place_prop)") %>% 
  phSparklyr.colExprCalc("0.45 * mean(budget_score) + 0.25 * mean(time_score) + 0.3 * mean(place_score) as score") %>% 
  phSparklyr.select("score") %>% 
  phSparklyr.distinct() %>% 
  phSparklyr.mutate(newColName = "index_m",
                    plugin = "'resource_assigns'")

spark.manage.time <- spark.result %>% 
  phSparklyr.select("representative", "field_work", "one_on_one_coaching", "employee_kpi_and_compliance_check", 
                    "admin_work", "kol_management", "business_strategy_planning", "team_meeting", "manager_time") %>% 
  phSparklyr.distinct() %>% 
  phSparklyr.group(groupCols = c("employee_kpi_and_compliance_check", "admin_work", "kol_management", 
                                 "business_strategy_planning", "team_meeting", "manager_time"),
                   aggExprs = c("sum(field_work) as field_work",
                                "sum(one_on_one_coaching) as one_on_one_coaching")) %>% 
  phSparklyr.join(joinDF = phSparklyr.castColType(phSparklyr.readParquet("/test/TMTest/inputParquet/standard_time"),
                                                  newColType = "double",
                                                  colNames = c("employee_kpi_and_compliance_check_std", 
                                                               "admin_work_std", "kol_management_std", 
                                                               "business_strategy_planning_std", 
                                                               "team_meeting_std", "field_work_std", 
                                                               "one_on_one_coaching_std")),
                  joinExpr = "TRUE",
                  joinType = "full") %>% 
  phSparklyr.mutate(newColName = "score",
                    plugin = strwrap("(abs(employee_kpi_and_compliance_check - employee_kpi_and_compliance_check_std) / manager_time 
                                     + abs(admin_work - admin_work_std) / manager_time 
                                     + abs(kol_management - kol_management_std) / manager_time 
                                     + abs(business_strategy_planning - business_strategy_planning_std) / manager_time 
                                     + abs(team_meeting - team_meeting_std) / manager_time 
                                     + abs(field_work - field_work_std) / manager_time 
                                     + abs(one_on_one_coaching - one_on_one_coaching_std) / manager_time) / 7",
                                     width = 10000)) %>% 
  phSparklyr.select("score") %>% 
  phSparklyr.mutate(newColName = "index_m",
                    plugin = "'manage_time'")

spark.manage.team <- spark.result %>% 
  phSparklyr.select("representative", "general_ability", "p_product_knowledge", "p_sales_skills", 
                    "p_territory_management_ability", "p_work_motivation", "p_behavior_efficiency") %>% 
  phSparklyr.distinct() %>% 
  phSparklyr.mutate(newColName = "p_general_ability",
                    plugin = strwrap("(0.2 * p_territory_management_ability + 0.25 * p_sales_skills + 0.25 * p_product_knowledge 
                                     + 0.15 * p_behavior_efficiency + 0.15 * p_work_motivation) * 10",
                                     width = 10000)) %>% 
  phSparklyr.mutate(newColName = "space_delta",
                    plugin = "100 - p_general_ability") %>% 
  phSparklyr.mutate(newColName = "growth_delta",
                    plugin = "general_ability - p_general_ability") %>% 
  phSparklyr.colExprCalc("0.2 - mean(growth_delta) / mean(space_delta) as score") %>% 
  phSparklyr.select("score") %>% 
  phSparklyr.distinct() %>% 
  phSparklyr.mutate(newColName = "index_m",
                    plugin = "'manage_team'")

spark.particular.assessment <- phSparklyr.bind(spark.region.division, spark.target.assigns) %>% 
  phSparklyr.bind(spark.resource.assigns) %>% 
  phSparklyr.bind(spark.manage.time) %>% 
  phSparklyr.bind(spark.manage.team) %>% 
  phSparklyr.join(joinDF = phSparklyr.castColType(phSparklyr.readParquet("/test/TMTest/inputParquet/level_data"),
                                                  newColType = "double",
                                                  colNames = c("level1", "level2")),
                  joinExpr = "index_m == index",
                  joinType = "left") %>% 
  phSparklyr.mutate(newColName = "level",
                    plugin = "case when score < level1 then 1 when score > level2 then 3 else 2 end") %>% 
  phSparklyr.select("index", "code", "level")

spark.general.assessment <- spark.particular.assessment %>% 
  phSparklyr.colExprCalc("cast(mean(level) as int) as level_m") %>% 
  phSparklyr.select("level_m") %>% 
  phSparklyr.distinct() %>% 
  phSparklyr.rename(oldColName = "level_m",
                    newColName = "level") %>% 
  phSparklyr.mutate(newColName = "index",
                    plugin = "'general_performance'") %>% 
  phSparklyr.mutate(newColName = "code",
                    plugin = "5")

spark.assessment <- phSparklyr.bind(spark.particular.assessment, spark.general.assessment) %>% 
  phSparklyr.saveParquet("/test/TMTest/output/Assessment")

Json(phSparklyr.execute(spark.result.output, spark.competitor.report, spark.assessment),
     path = "TMtest/TMtest.json")


