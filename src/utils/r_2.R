# Documentation: r2_calc_rk
# Usage: r2_calc_rk(k)
# Description: Calculates R_k from the snn_flow-Paper
# Args/Options: k
# Returns: R_k
# Output: ...
# Action: ...
r2_calc_rk <- function(k) {
	true_value <- k
	ratio_value <- (true_value + 1 - ((2 * true_value + 1) / (2 * true_value))^2 *
										r2_calc_fk(true_value)^2) / 
		(true_value - r2_calc_fk(true_value)^2)
	if(is.na(ratio_value)){
		ratio_value <- 1.000018
	}
	# cat("Ratio Value for k =", true_value, "is", ratio_value, "\n")
	return(ratio_value)
}



# Documentation: r3_calc_fk 
# Usage: r3_calc_fk(k)
# Description: Returns F_k (https://sci-hub.ee/10.1007/s11004-011-9325-x)
# Args/Options: k
# Returns: F_k
# Output: ...
# Action: ...
r2_calc_fk <- function(k) {
	k_value <- k * factorial(2 * k) * (pi^0.5) / ((2^k * factorial(k))^2)
	return(k_value)
}