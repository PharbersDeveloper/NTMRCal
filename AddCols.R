AddCols <- function(df, cols, col_names) {
    result <- df
    for (nc in col_names) {
        result <- withColumn(result, nc, lit(cols[[nc]]))
    }
    return(result)
}

AddColFunc <- function(df, new_col, func_col) {
    return(withColumn(df, new_col, func_col(df)))
}

AddIdCol <- function(df) {
    return(withColumn(df, "id", monotonically_increasing_id()))
}