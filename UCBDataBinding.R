UCBDataBinding <- function(cal_data, weightages) {
    CastCol2Double(
        AddCols(cal_data, head(weightages, 1), columns(weightages)),
        c("potential", "patient", "rep_num", "hosp_num", "initial_budget", 
          "p_quota", "p_budget", "p_sales", "pppp_sales", "p_ytd_sales", 
          "quota", "budget", "hospital_quota_base_factor_w", 
          "hospital_product_quota_growth_factor_w", "district_potential_factor_w1", 
          "district_sales_factor_w", "factor2_w", "district_cross_factor_w1", 
          "district_hospital_factor_w1", "district_potential_factor_w2", 
          "district_cross_factor_w2", "district_hospital_factor_w2"))
}