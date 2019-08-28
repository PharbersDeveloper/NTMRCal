cal_budget_prop <- function(df) {
    df[["budget"]]/df[["total_budget"]] * 100
}

cal_customer_relationship <- function(df) {
    (df$budget_factor * df$budget_factor_w + 
         df$meeting_attendance_factor * df$meeting_attendance_factor_w) * 100
}

cal_offer_attractiveness <- function(df) {
    (df$sales_performance * df$sales_performance_w + 
         df$customer_relationship * df$customer_relationship_w)
}

cal_level_factor <- function(df) {
    df[["potential"]]/df[["total_potential"]]*0.8 + df[["p_sales"]]/df[["total_p_sales"]]*0.2
}

cal_level <- function(df) {
    ifelse(df$level_factor > 0.15, 3, ifelse(df$level_factor <= 0.05, 1, 2))
}

cal_deployment_quality <- function(df) {
    df$business_strategy_planning_index * df$business_strategy_planning_index_w + 
        df$admin_work_index * df$admin_work_index_w +
        df$employee_kpi_and_compliance_check_index * df$employee_kpi_and_compliance_check_index_w +
        df$kol_management_index * df$kol_management_index_w +
        df$team_meeting_index * df$team_meeting_index_w
}

cal_sales_performance <- function(df) {
    df$rep_ability_efficiency * df$rep_ability_efficiency_w + 
        df$field_work_index * df$field_work_index_w +
        df$deployment_quality * df$deployment_quality_w    
}

cal_quota_growth <- function(df) {
    df$quota/df$p_quota
}

cal_quota_restriction_factor <- function(df) {
    ifelse((df$quota_growth >= 0.5) & (df$quota_growth <= 2.0), 1.0, 0.6)
}

cal_rep_ability_eff <- function(df) {
    df$general_ability * df$general_ability_w + 
        df$call_time_index * df$call_time_index_w +
        df$quota_restriction_index * df$quota_restriction_index_w
}

cal_work_motivation <- function(df) {
    df$p_work_motivation + 
        (-df$p_work_motivation + 10) * 0.15 * 
        (df$performance_review + df$career_development_guide)
}

cal_tma <- function(df) {
    df[["p_territory_management_ability"]] + (-df[["p_territory_management_ability"]] + 10) * 0.3 * df[["territory_management_training"]]
}

cal_sales_skill <- function(df) {
    df[["p_sales_skills"]] + (-df[["p_sales_skills"]] + 10) * 0.3 * df[["sales_skills_training"]]
}

cal_product_knowledge <- function(df) {
    df[["p_product_knowledge"]] + (-df[["p_product_knowledge"]] + 10) * 0.3 * df[["product_knowledge_training"]]
}

cal_b_e <- function(df) {
    df[["p_behavior_efficiency"]] + (-df[["p_behavior_efficiency"]] + 10) * 0.3 * df[["behavior_efficiency_factor"]]
}

cal_general_ability <- function(df) {
    (df$territory_management_ability * df$territory_management_ability_w +
         df$sales_skills * df$sales_skills_w + 
         df$product_knowledge * df$product_knowledge_w + 
         df$behavior_efficiency * df$behavior_efficiency_w + 
         df$work_motivation * df$work_motivation_w) * 10
}

cal_update_work_motivation <- function(df) {
    ifelse(df$rep_quota_achv >= 0.9 & df$rep_quota_achv <= 1.2, 
           df$work_motivation + (-df$work_motivation + 10) * 0.2,
           df$work_motivation)
}

cal_classABC <- function(df) {
    ifelse(df$behavior_efficiency >= 0 & df$behavior_efficiency < 3, 1,
           ifelse(df$behavior_efficiency >= 3 & df$behavior_efficiency < 6, 2,
           ifelse(df$behavior_efficiency >= 6 & df$behavior_efficiency < 8, 3, 4)))
}

cal_target_with_class <- function(df, c, t) {
     ifelse(df[[c]] == 1, df[[t]] - randn() * 5 + 5, 
           ifelse(df[[c]] == 2, df[[t]] - randn() * 5, 
           ifelse(df[[c]] == 3, df[[t]] + randn() * 5,
                  df[[t]] + randn() * 5)))   
}

cal_target_coverage <- function(df) {
    ifelse(df$class1 == 1, df$target_coverage - rand() * 5 + 5, 
           ifelse(df$class1 == 2, df$target_coverage - rand() * 5, 
           ifelse(df$class1 == 3, df$target_coverage + rand() * 5,
                  df$target_coverage + randn() * 5)))
}

cal_high_target_m <- function(df) {
    ifelse(df$class1 == 1, rand() + 13, 
           ifelse(df$class1 == 2, rand() + 14, 
                  ifelse(df$class1 == 3, rand() * 2 + 16,
                         rand() * 3 + 19)))
}

cal_high_target <- function(df) {
    ifelse(df$class2 == 1, df$high_target_m - (rand() + 1), 
           ifelse(df$class2 == 2, df$high_target_m - (rand()), 
                  ifelse(df$class2 == 3, df$high_target_m + (rand()),
                         df$high_target_m + 1)))
}

cal_middle_target_m <- function(df) {
    ifelse(df$class1 == 1, randn() + 13, 
           ifelse(df$class1 == 2, randn() + 13, 
                  ifelse(df$class1 == 3, randn() + 12,
                         randn() + 12)))
}

cal_middle_target <- function(df) {
    ifelse(df$class2 == 1, df$middle_target_m - 2, 
           ifelse(df$class2 == 2, df$middle_target_m - 1, 
                  ifelse(df$class2 == 3, df$middle_target_m + (rand()),
                         df$middle_target_m + 1)))
}

cal_low_target_m <- function(df) {
    ifelse(df$class1 == 1, randn() + 13, 
           ifelse(df$class1 == 2, randn() + 13, 
                  ifelse(df$class1 == 3, randn() + 12,
                         randn() + 11)))
}

cal_low_target <- function(df) {
    ifelse(df$class2 == 1, df$low_target_m - 2, 
           ifelse(df$class2 == 2, df$low_target_m - 1, 
                  ifelse(df$class2 == 3, df$low_target_m + (rand()),
                         df$low_target_m + 1)))
}