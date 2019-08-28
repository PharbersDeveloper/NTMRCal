ColSum <- function(df, col_name) {
    return(select(df, sum(df[[col_name]])))
}