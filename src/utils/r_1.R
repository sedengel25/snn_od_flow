# Documentation: r1_create_chunks
# Usage: r1_create_chunks(cores, n)
# Description: Splits the ids in squared increasing chunks for parallelization
# Args/Options: cores, n
# Returns: int
# Output: ...
# Action: ... 
r1_create_chunks <- function(cores, n) {
	chunk_size <- n / cores
	
	# Calculate the breaks
	breaks <- round(seq(1, n, by = chunk_size))
	
	# Ensure the last break is equal to 'n' to include all elements
	if(breaks[length(breaks)] != n) {
		breaks <- c(breaks, n)
	}
	
	# Return the breaks
	return(breaks[-1])
}



# Documentation: r1_get_knn_ind
# Usage: r1_get_knn_ind(distances, k)
# Description: Gets indexes of knn
# Args/Options: distances, k
# Returns: numeric
# Output: ...
# Action: ... 
r1_get_knn_ind <- function(distances, k) {
	sort.index <- order(distances)
	knn <- sort.index[1:k]
	return(knn)
}

# Documentation: r1_get_knn_dist
# Usage: r1_get_knn_dist(distances, k)
# Description: Gets distances of knn
# Args/Options: distances, k
# Returns: numeric
# Output: ...
# Action: ... 
r1_get_knn_dist <- function(distances, k) {
	sort.index <- order(distances)
	knn <- sort.index[1:k]
	knn_dist <- distances[knn]
	return(knn_dist)
}

# Documentation: r1_create_rkd_dt
# Usage: r1_create_rkd_dt(k_max, matrix_knn_dist
# Description: Creates at dt with the cols 'k' and 'rkd'
# Args/Options: con, k_max, city_prefix
# Returns: data.table
# Output: ...
# Action: ...
r1_create_rkd_dt <- function(k_max, matrix_knn_dist) {
	
	num_variances <- c()
	num_rks <- c()

	
	for(k in 1:k_max){
		col_idx = k
		var_k = var(matrix_knn_dist[,col_idx]) %>% as.numeric
		num_variances[k] <- var_k
		num_rks[k] <- r2_calc_rk(k = k)
	}
	
	num_variances_k <- num_variances
	num_variances_k_1 <- num_variances_k %>% lead
	num_variances_k_1 <- num_variances_k_1[-length(num_variances_k_1)]
	num_variances_k <- num_variances_k[-length(num_variances_k)]
	
	num_var_ratio <- num_variances_k_1/num_variances_k
	num_theo_var_ratio <- num_rks[-length(num_rks)]
	num_rkd <- num_var_ratio/ num_theo_var_ratio
	
	dt_rkd <- data.table(
		k = c(1:k_max),
		rkd = num_rkd
	)
	
	return(dt_rkd)
}



# Documentation: r1_datatable_nd_to_matrix_nd
# Usage: r1_datatable_nd_to_matrix_nd(dt_knn)
# Description: Transform datatable with OD-flow distances into a matrix
# Args/Options: dt_knn
# Returns: data.table
# Output: ...
# Action: ...
r1_datatable_nd_to_matrix_nd <- function(dt_flows_nd, complete = TRUE) {
	
	dt_knn <- dt_knn %>%
		arrange(flow_ref, distance, flow_other)
	
	dt_knn <- dt_knn %>%
		group_by(flow_ref) %>%
		mutate(row_id = row_number()) %>%
		ungroup() %>%
		as.data.table
	
	dt_knn_r <- dcast(dt_knn, flow_ref ~ row_id, value.var = "flow_other")
	dt_knn_r_dist <- dcast(dt_knn, flow_ref ~ row_id, value.var = "distance")

	return(list("dt_flows" = dt_knn_r, "dt_nd" = dt_knn_r_dist))
}
