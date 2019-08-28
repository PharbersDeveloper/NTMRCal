ColMin <- function(df, col_name) {
    result <- select(df, min(df[[col_name]]))
    return(result)
}