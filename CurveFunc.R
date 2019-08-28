curve_func <- function(curve_data, input) {
    
    if (input < min(curve_data$x))
        return(curve_data[which.min(curve_data$x), 2])
    
    if (input > max(curve_data$x))
        return(curve_data[which.max(curve_data$x), 2])
    
    left <- curve_data[which.min(abs(input - curve_data$x)), ]
    tmp <- curve_data[-which.min(abs(input - curve_data$x)), ]
    right <- tmp[which.min(abs(input - tmp$x)), ]
    
    y <- ifelse(left$x <= right$x,
                (1.0 - (input - left$x) / (right$x - left$x)) * left$y + (1.0 - (right$x - input) / (right$x - left$x)) * right$y, 
                (1.0 - (input - right$x) / (left$x - right$x)) * right$y + (1.0 - (left$x - input) / (left$x - right$x)) * left$y)
    
    return(y)
}
