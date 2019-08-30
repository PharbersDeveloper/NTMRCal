TMCalResAchv <- function(cal_data) {
    cal_data_res <- ColRename(agg(groupBy(cal_data, "representative_id"), sales="sum", quota="sum"), 
                              c("representative_id", "sum(quota)", "sum(sales)"), 
                              c("representative_id_m", "rep_quota", "rep_sales"))
    
    cal_data <- join(cal_data, cal_data_res, cal_data$representative_id == cal_data_res$representative_id_m, "inner")
    
    return(cal_data)
}