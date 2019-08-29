TMCalCurveSkeleton2 <- function(df, curves, arr, cast_func = TMCalValue2Numeric) {
    narr <- array(arr, dim = c(4, length(arr)/4))
    # c_curves <- collect(curves)
    c_curves <- curves
    
    # shema
    schemadf <- df
    for (idx in 1:(length(arr)/4)) {
        iter <- narr[,idx]
        n_col = iter[2]
        schemadf <- withColumn(schemadf, n_col, lit(0.0))
    }
    dfSchema <- schema(schemadf)
   
    # value 
    df <- dapply(df, 
                 function(x) {
                     result <- x
                     for (idx in 1:(length(arr)/4)) {
                         iter <- narr[,idx]
                         
                         curve_name = iter[1]
                         arg_col = iter[3]
                         condi = iter[4]
                         
                         if (condi == "None") {
                             t_curves <- c_curves[c_curves[1] == curve_name, ]
                             result <- TMCalCurveSkeleton3(result, t_curves, arg_col)

                         } else {
                             # result <- TMCalCurveSkeleton4(result, c_curves, condi, curve_name, arg_col)
                             condi_split <- unlist(strsplit(condi, "[$]"))
                             left <- condi_split[1]
                             right <- condi_split[2]
                             
                             values <- unlist(strsplit(right, ":"))
                             curve_name_values <- unlist(strsplit(curve_name, ":"))
                             
                             reval <- NULL 
                             for (idx in 1:length(values)) {
                                 cur_name <- curve_name_values[idx]
                                 t_curves <- c_curves[c_curves[1] == cur_name, ]
                                 v <- values[idx]
                                 x <- result[result[left] == cast_func(v), ]
                                 x <- TMCalCurveSkeleton3(x, t_curves, arg_col)
                                 if (is.null(reval)) {
                                     reval <- x
                                 } else {
                                     reval <- rbind(reval, x)
                                 }
                             }
                             result <- reval
                         }
                     }
                     result
                       
                 }, dfSchema )
   
    return(df)
}

TMCalCurveSkeleton3 <- function(x, c, arg_col) {
    return(cbind(x, sapply(x[[arg_col]], function(y) { curve_func(c, y) } )))
}

TMCalValue2Numeric <- function(v) {
    as.numeric(v)
}

TMCalValue2String <- function(v) {
    as.character(v)
}
