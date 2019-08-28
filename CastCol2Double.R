CastCol2Double <- function(df, cols) {
    result <- df
    col_names <- columns(df)
    for (nc in cols) {
        result <- withColumn(result, nc, cast(df[[nc]], as.character("double")))
    }
    return(result)
}