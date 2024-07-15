source("./src/functions.R")


# Documentation: main_calc_flow_nd_dist_mat
# Usage: main_calc_flow_nd_dist_mat(sf_trips, dt_network, dt_dist_mat)
# Description: Creates a matrix containing the ND between OD flwos depending
	# on the given raod network and the given local node distance matrix
# Args/Options: sf_trips, dt_network, dt_dist_mat
# Returns: matrix
# Output: ...
# Action: ...
main_calc_flow_nd_dist_mat <- function(sf_trips, dt_network, dt_dist_mat) {
	
	### 1. Prepare the data ------------------------------------------------------
	sf_trips$origin_geom <- lwgeom::st_startpoint(sf_trips$geometry)
	sf_trips$dest_geom <- lwgeom::st_endpoint(sf_trips$geometry)
	
	
	dt_origin <- sf_trips %>%
		st_set_geometry("origin_geom") %>%
		select(flow_id, origin_id, origin_geom) %>%
		rename("id" = "flow_id",
					 "id_edge" = "origin_id",
					 "geom" = "origin_geom") %>%
		as.data.table()
	
	dt_origin <- add_dist_start_end(dt_origin)
	
	dt_dest <- sf_trips %>%
		st_set_geometry("dest_geom") %>%
		select(flow_id, dest_id, dest_geom) %>%
		rename("id" = "flow_id",
					 "id_edge" = "dest_id",
					 "geom" = "dest_geom") %>% 
		as.data.table
	
	dt_dest <- add_dist_start_end(dt_dest)
	
	
	
	dt_network <- dt_network %>%
		select(source, target, id, geom_way)
	
	
	
	### 2. Calc ND between OD flows ----------------------------------------------
	# ND between origin points
	dt_o_pts_nd <- parallel_process_networks(dt_origin, 
																					dt_network,
																					dt_dist_mat,
																					int_cores)
	
	#print("ND between origin points calculated")
	# ND between dest points
	dt_d_pts_nd <- parallel_process_networks(dt_dest, 
																					 dt_network,
																					 dt_dist_mat,
																					 int_cores)
	
	
	#print("ND between dest points calculated")
	# ND between OD flows
	dt_flow_nd <- dt_o_pts_nd %>%
		inner_join(dt_d_pts_nd, by = c("from" = "from", "to" = "to")) %>%
		mutate(distance = distance.x + distance.y) %>%
		select(flow_m = from, flow_n = to, distance) %>%
		as.data.table
	
	dt_flow_nd <- dt_flow_nd %>%
		group_by(flow_m) %>%
		mutate(row_id = row_number()) %>%
		ungroup() %>%
		as.data.table
	return(dt_flow_nd)
	### 3. Calc OD-flow ND matrix ------------------------------------------------
	# matrix_flow_dist <- calc_flow_nd_dist_mat(dt_flow_nd)
	# print("matrix conversion successful")
	# sym_mat <- matrix_flow_dist
	# sym_mat[is.na(sym_mat)] <- 0
	# sym_mat <- pmax(sym_mat, t(sym_mat))  # Elementweise das Maximum der Matrix und ihrer Transponierten
	# sym_mat[sym_mat == 0] <- NA
	# print("symmat successful")
	# 
	# return(sym_mat)
}


# Documentation: main_calc_flow_euclid_dist_mat
# Usage: main_calc_flow_euclid_dist_mat(sf_trips)
# Description: Creates a matrix containing the euclidean distance between OD 
	# flows depending
# Args/Options: sf_trips
# Returns: matrix
# Output: ...
# Action: ...
main_calc_flow_euclid_dist_mat <- function(sf_trips) {
	sf_trips$origin <- lwgeom::st_startpoint(sf_trips$geometry)
	sf_trips$dest <- lwgeom::st_endpoint(sf_trips$geometry)
	sf_origin_distances <- st_distance(sf_trips$origin, by_element = FALSE)
	sf_dest_distances <- st_distance(sf_trips$dest, by_element = FALSE)
	matrix_distances <- drop_units(sf_origin_distances + sf_dest_distances)
	return(matrix_distances)
}


# Documentation: snn_flow
# Usage: snn_flow(flow_dist_mat, k, eps, minpts)
# Description: Executes the SNN_flow algorithm (Liu et al. (2022))
# Args/Options: flow_dist_mat, k, eps, minpts
# Returns: datatable
# Output: ...
# Action: ...
snn_flow <- function(dt_flow_nd, sf_trips, k, eps, minpts) {

	### 1. Calculate SNN Density -------------------------------------------------

	# matrix_knn_dist <- t(apply(dt_flow_nd, 1, r1_get_knn_dist, k))
	# matrix_knn_ind <- t(apply(dt_flow_nd, 1, r1_get_knn_ind, k))
	# 
	# print("matrix_knn_ind: ")
	# print(matrix_knn_ind)
	# # Get a boolean matrix (TRUE = distance matrix contains value)...
	# num_flows <- nrow(matrix_knn_ind)
	# max_index <- max(matrix_knn_ind)
	# boolean_knn <- !is.na(matrix_knn_dist)
	# 
	# # ...to get NAs also in the knn-matrix containing the indices
	# matrix_knn_ind <- ifelse(boolean_knn, matrix_knn_ind,  NA)
	# 
	# 
	# 
	# 
	# 
	# dt_knn <- as.data.table(matrix_knn_ind)
	# dt_knn$flow_ref <- 1:nrow(matrix_knn_ind)
	# print("dt_knn: ")
	# print(dt_knn)
	# matrix_knn <- dt_knn %>%
	# 	select(flow_ref, everything()) %>%
	# 	as.matrix
	p1 <- proc.time()
	all_flow_ids <- unique(sf_trips$flow_id)
	
	matrix_knn <- cpp_find_knn(df = dt_flow_nd, k = k, all_flow_ids) %>%
		as.matrix

	
# 	p1 <- proc.time()
# 	nns <- dt_flow_nd[, head(.SD[order(distance)], k), by = flow_m]
# 	p2 <- proc.time()
# 	time_diff <- p2-p1
# 	cat("nns: ", time_diff[[3]], " seconds.\n")
# 	
# 	p1 <- proc.time()
# 	
#   dt_knn <- nns[, {
# 		nn <- .SD[1:k, flow_n]
# 		c(list(flow_m = .BY[[1]]), as.list(nn))
# 	}, by = flow_m]
# 	dt_knn <- dt_knn[,-1]
# 	setnames(dt_knn, c("flow_m", paste0("NN", 1:k)))
# 	all_flow_m <- unique(sf_trips$flow_id)
# 	missing_flow_m <- setdiff(all_flow_m, dt_knn$flow_m)
# 	missing_entries <- data.table(flow_m = missing_flow_m)
# 	missing_entries[, (paste0("NN", 1:k)) := NA]
# 	dt_knn <- rbindlist(list(dt_knn, missing_entries), fill = TRUE)
# 	setnames(dt_knn, "flow_m", "flow_ref")
# 	setorder(dt_knn, flow_ref)
# 	matrix_knn <- dt_knn %>%
# 		as.matrix
# 	
# 	print(matrix_knn[1184,])
# 	diff_matrix <- matrix_knn_test - matrix_knn
# 	return(diff_matrix)
	p2 <- proc.time()
	time_diff <- p2-p1
	cat("matrix_knn: ", time_diff[[3]], " seconds.\n")
	
	p1 <- proc.time()
	list_df <- cpp_calc_density_n_get_dr_df(dt_knn = matrix_knn,
																					eps = eps,
																					int_k = k)
	dt_snn_density <- list_df$snn_density %>% as.data.table()
	
	p2 <- proc.time()
	time_diff <- p2-p1
	cat("dt_snn_density: ", time_diff[[3]], " seconds.\n")
	

	### 2. Assign clusters using density connectivity mechanism ------------------
	p1 <- proc.time()
	
	dt_dr_flows <- list_df$dr_flows %>% as.data.table

	p2 <- proc.time()
	time_diff <- p2-p1
	cat("dt_dr_flows: ", time_diff[[3]], " seconds.\n")
	
	# Flows with a SNNDensity higher than the threshold are 'core-flows'
	dt_snn_density <- dt_snn_density %>%
		mutate(core_flow = case_when(shared_density >= minpts ~ "yes",
																 TRUE ~ "no"))
	
	setorder(dt_snn_density, flow)

	# If no core-flows are found, all flows are noise
	if(length(which(dt_snn_density$core_flow == "yes")) == 0){
		dt_cluster_final <- dt_snn_density %>%
			mutate(cluster_pred = 0) %>%
			select(flow, cluster_pred)
		return(dt_cluster_final)
	}
	
	p1 <- proc.time()
	
	# Get all core flows
	total_core_flows <- dt_snn_density %>%
		filter(core_flow == "yes") %>%
		select(flow) %>%
		pull %>%
		as.integer
	
	p2 <- proc.time()
	time_diff <- p2-p1
	cat("total_core_flows: ", time_diff[[3]], " seconds.\n")
	
	p1 <- proc.time()
	
	# Get all flows directly reachable from the core flows
	flows_reachable_from_core_flow <- dt_dr_flows %>%
		left_join(dt_snn_density, by = c("from" = "flow")) %>%
		filter(core_flow == "yes") %>%
		select(to) %>%
		pull %>%
		as.integer %>%
		unique
	
	
	p2 <- proc.time()
	time_diff <- p2-p1
	cat("flows_reachable_from_core_flow: ", time_diff[[3]], " seconds.\n")

	p1 <- proc.time()
	
		# Create a table with all directly reachable core-flow pairs...
	dt_dr_flows_cf <- dt_dr_flows[from %in% total_core_flows & 
																	to %in% total_core_flows]
	
	p2 <- proc.time()
	time_diff <- p2-p1
	cat("dt_dr_flows_cf: ", time_diff[[3]], " seconds.\n")
	
	p1 <- proc.time()
	# ...create a graph from that dataframe... 
	graph <- graph_from_data_frame(dt_dr_flows_cf, directed = FALSE)
	
	#...and get all the components...
	components <- components(graph)
	
	#...which equals the number of clusters according to the density connectivity mechanism.
	dt_cluster_coreflows <- data.table(flow = names(components$membership) %>% as.integer, 
																		 cluster_pred = components$membership)
	
	# Get all the flows that are not yet a part of a cluster...
	dt_cluster_0 <- dt_snn_density[which(!dt_snn_density$flow 
																			 %in% dt_cluster_coreflows$flow), "flow"]
	
	#...assign them cluster 0...
	dt_cluster_0$cluster_pred <- 0
	
	
	#...and combine them into a dataframe with the coreflows having a cluster.
	dt_cluster <- rbind(dt_cluster_coreflows, dt_cluster_0) %>%
		arrange(flow)
	
	p2 <- proc.time()
	time_diff <- p2-p1
	cat("Get coreflows: ", time_diff[[3]], " seconds.\n")
	
	p1 <- proc.time()
	# Now assign all the non-core flows
	list_cluster_final <- cpp_assign_clusters(dt_cluster = dt_cluster,
																						dt_knn_r = matrix_knn,
																						flows_reachable_from_core_flow = flows_reachable_from_core_flow,
																						int_k = k)
	
	
	list_length_clusters <- lapply(names(list_cluster_final), function(x){
		idx <- as.integer(x)
		n <- length(list_cluster_final[[idx]])
		rep(idx,n)
	})
	

	dt_cluster_flows <- cbind(unlist(list_length_clusters), 
														unlist(list_cluster_final)) %>% 
		as.data.table %>%
		rename("cluster_pred" = "V1",
					 "flow" = "V2") %>%
		select(flow, cluster_pred)
	
	
	dt_noise_flows <- dt_cluster[which(!dt_cluster$flow 
																		 %in% dt_cluster_flows$flow),"flow"]
	
	
	dt_noise_flows$cluster_pred <- 0 
	
	dt_cluster_final <- rbind(dt_cluster_flows, dt_noise_flows)
	
	dt_cluster_final <- dt_cluster_final %>%
		arrange(flow)
	
	p2 <- proc.time()
	time_diff <- p2-p1
	cat("Assign non-core flows: ", time_diff[[3]], " seconds.\n")
	return(dt_cluster_final)
}







