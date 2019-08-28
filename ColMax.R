ColMax <- function(df, col_name) {
    result <- select(df, max(df[[col_name]]))
    return(result)
}