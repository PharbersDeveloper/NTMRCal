# 2. 将cal 以及参数进行列添加, 并ID 化
TMDataCbind <- function(cal_data, weightages, manager) {
    c_p_w_m <- CastCol2Double(
        AddCols(AddCols(cal_data, head(weightages, 1), columns(weightages)), head(manager, 1), columns(manager)),
        c("budget", "meeting_attendance", "quota", "call_time", 
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
          "total_budget",	"total_quota", "total_place"))
    
    # 3. 计算level和上期总销售
    total_potential = ColSum(c_p_w_m, "potential")
    total_p_sales = ColSum(c_p_w_m, "p_sales")
    c_p_w_m <- ColRename(
        AddCols(
            AddCols(c_p_w_m, 
                    head(total_potential, 1), 
                    columns(total_potential)), 
            head(total_p_sales, 1), 
            columns(total_p_sales)), c("sum(potential)", "sum(p_sales)"),
        c("total_potential", "total_p_sales"))
    
    return(c_p_w_m)
}