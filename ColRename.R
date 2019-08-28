ColRename <- function(df, cols, n_cols) {
    result <- df
    for (idx in 1:length(cols)) {
        result <- withColumnRenamed(result, cols[idx], n_cols[idx])
    }
    return(result)
}