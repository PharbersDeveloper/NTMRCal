cal_total_budget <- function(df) {
    ifelse(df$product == "开拓来", df$initial_budget * 0.6, df$initial_budget * 0.12)
}

cal_update_protential <- function(df) {
    ifelse(df$status == "未开发", 0, df$potential)
}

cal_potential_factor <- function(df) {
    ifelse(df$product == "开拓来", 0.8, 1.0)
}

cal_sales_factor <- function(df) {
    ifelse(df$product == "开拓来", 0.2, 0.0)
}

cal_mac_oa <- function(df) {
    ifelse(df$product == "开拓来", 100.0, 40.0)
}

cal_value_contri <-function(df) {
    df$potential_contri * df$potential_factor + df$sales_contri * df$sales_factor
}

cal_hospital_quota_base <- function(df) {
    ifelse(df$status == "已开发",
            df$p_sales * 0.4 / df$sumps + df$potential * 0.6,
            df$potential / df$sumptt)
}

cal_quota_prop <- function(df) {
    ifelse(df$sumqt == 0, 0, df$quota / df$sumqt)
}

# factor 1
cal_hospital_product_quota_growth_factor <- function(df) {
    ifelse(df$status == '已开发',
           - abs((df$quota - df$p_sales)/(df$sumqt - df$sumps) - (df$potential_m - df$p_sales)/(df$sumptt - df$sumps)) + 1,
    0)
}

cal_hospital_quota_base_factor <- function(df) {
    -(abs(df$quota_prop - df$hospital_quota_base) / df$hospital_quota_base) + 1
}

cal_hospital_quota_base_factor_m <- function(df) {
    ifelse(df$hospital_quota_base_factor < 0, 0, df$hospital_quota_base_factor)
}

cal_hospital_product_quota_growth_factor_m <- function(df) {
    ifelse(df$hospital_product_quota_growth_factor < 0, 0, df$hospital_product_quota_growth_factor)
}

cal_factor_1 <- function(df) {
    ifelse(df$status == "已开发",
           df$hospital_quota_base_factor_m * df$hospital_quota_base_factor_w +
           df$hospital_product_quota_growth_factor_m * df$hospital_product_quota_growth_factor_w,
           df$hospital_quota_base_factor_m)
}

# factor 2
cal_district_cross_factor <- function(df) {
    ifelse(df$city_num_dist == 1, 1, 0)
}

cal_district_potential_factor <- function(df) {
    lit(1.0)/(abs(df$rep_num * df$potential_dist / df$sumptt - 1) + 1)
}

cal_district_sales_factor <- function(df) {
    ifelse(df$status == "已开发", lit(1.0) / (abs(df$rep_num * df$sales_dist / df$sumps - 1) + 1), 0)
}

cal_district_hospital_factor <- function(df) {
    lit(1.0) / (abs(df$rep_num * df$hospital_num_dist / df$hosp_num - 1) + 1)
}

cal_factor_2 <- function(df) {
    ifelse(df$status == "已开发",
           # yes
           df$district_potential_factor * df$district_potential_factor_w1 * df$factor2_w +
           df$district_sales_factor * df$district_sales_factor_w * df$factor2_w +
           df$district_cross_factor * df$district_cross_factor_w1 +
           df$district_hospital_factor * df$district_hospital_factor_w1,
           # no
           df$district_potential_factor * df$district_potential_factor_w2 +
           df$district_cross_factor * df$district_cross_factor_w2 +
           df$district_hospital_factor * df$district_hospital_factor_w2)
}

# oa
cal_p_oa_factor <- function(df) {
    ifelse(df$status == "已开发", df$p_sales / df$potential_m * 100, 0)
}

cal_offer_attractiveness <- function(df) {
    ifelse(df$oa_factor_base < 0,
           df$p_offer_attractiveness_m * (df$oa_factor_base + 1),
           df$p_offer_attractiveness_m + (df$max_oa - df$p_offer_attractiveness_m) * df$oa_factor_base)
}

cal_p_offer_attractiveness_m <- function(df) {
    ifelse(df$status == "已开发",
           df$p_offer_attractiveness,
           rand() * 4 + 3)
}

cal_offer_attractiveness_adj <- function(df) {
    ifelse(df$status == "已开发",
           df$offer_attractiveness * (df$adjust_factor * (-0.8) + 1),
           df$offer_attractiveness * (df$adjust_factor * (-0.5) + 1))
}

# market share
cal_market_share_m <- function(df) {
    ifelse(df$representative == 0, 0, df$market_share / 100)
}

# undev
cal_develop_fee <- function(df) {
    ifelse(df$hospital_level == "三级", 12000,
           ifelse(df$hospital_level == "二级", 8000,
                  3000))
}

cal_update_status <- function(df) {
    ifelse(df$budget_m >= df$develop_fee, "正在开发", "未开发")
}
