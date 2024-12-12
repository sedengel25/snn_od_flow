source("./src/functions.R")

# Documentation: main_calc_flow_nd_dist_mat
# Usage: main_calc_flow_nd_dist_mat(sf_trips, dt_network, dt_dist_mat)
# Description: Creates a matrix containing the ND between OD flwos depending
# on the given raod network and the given local node distance matrix
# Args/Options: sf_trips, dt_network, dt_dist_mat
# Returns: matrix
# Output: ...
# Action: ...
main_calc_flow_nd_dist_mat_synth_data <- function(sf_trips, dt_network, dt_dist_mat) {
	
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
	
	dt_origin <- dt_origin %>%
		arrange(id)
	
	dt_dest <- dt_dest %>%
		arrange(id)
	
	
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
	# dt_flow_nd <- dt_o_pts_nd %>%
	# 	inner_join(dt_d_pts_nd, by = c("from" = "from", "to" = "to")) %>%
	# 	mutate(distance = distance.x + distance.y) %>%
	# 	select(flow_m = from, flow_n = to, distance) %>%
	# 	as.data.table
	# 
	# dt_flow_nd <- dt_flow_nd %>%
	# 	group_by(flow_m) %>%
	# 	mutate(row_id = row_number()) %>%
	# 	ungroup() %>%
	# 	as.data.table
	return(list("dt_o_pts_nd" = dt_o_pts_nd,
							"dt_d_pts_nd" = dt_d_pts_nd))
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
	# sf_trips$origin_geom <- lwgeom::st_startpoint(sf_trips$geometry)
	# sf_trips$dest_geom <- lwgeom::st_endpoint(sf_trips$geometry)



	dt_origin <- sf_trips %>%
		st_set_geometry("o_closest_point") %>%
		select(flow_id, origin_id, o_closest_point) %>%
		rename("id" = "flow_id",
					 "id_edge" = "origin_id",
					 "geom" = "o_closest_point") %>%
		as.data.table()

	# dt_origin <- sf_trips %>%
	# 	st_set_geometry("origin_geom") %>%
	# 	select(flow_id, origin_id, origin_geom) %>%
	# 	rename("id" = "flow_id",
	# 				 "id_edge" = "origin_id",
	# 				 "geom" = "origin_geom") %>%
	# 	as.data.table()
	dt_origin <- add_dist_start_end(dt_origin)
	

	
	dt_dest <- sf_trips %>%
		st_set_geometry("d_closest_point") %>%
		select(flow_id, dest_id, d_closest_point) %>%
		rename("id" = "flow_id",
					 "id_edge" = "dest_id",
					 "geom" = "d_closest_point") %>%
		as.data.table

	# dt_dest <- sf_trips %>%
	# 	st_set_geometry("dest_geom") %>%
	# 	select(flow_id, dest_id, dest_geom) %>%
	# 	rename("id" = "flow_id",
	# 				 "id_edge" = "dest_id",
	# 				 "geom" = "dest_geom") %>%
	# 	as.data.table
	dt_dest <- add_dist_start_end(dt_dest)
	
	
	
	dt_network <- dt_network %>%
		select(source, target, id, geom_way)
	
	dt_origin <- dt_origin %>%
		arrange(id)

	dt_dest <- dt_dest %>%
		arrange(id)
	

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
	# dt_flow_nd <- dt_o_pts_nd %>%
	# 	inner_join(dt_d_pts_nd, by = c("from" = "from", "to" = "to")) %>%
	# 	mutate(distance = distance.x + distance.y) %>%
	# 	select(flow_m = from, flow_n = to, distance) %>%
	# 	as.data.table
	# 
	# dt_flow_nd <- dt_flow_nd %>%
	# 	group_by(flow_m) %>%
	# 	mutate(row_id = row_number()) %>%
	# 	ungroup() %>%
	# 	as.data.table
	return(list("dt_o_pts_nd" = dt_o_pts_nd,
							 "dt_d_pts_nd" = dt_d_pts_nd))
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




# Documentation: main_calc_flow_nd_dist_mat
# Usage: main_calc_flow_nd_dist_mat(sf_trips, dt_network, dt_dist_mat)
# Description: Creates a matrix containing the ND between OD flwos depending
# on the given raod network and the given local node distance matrix
# Args/Options: sf_trips, dt_network, dt_dist_mat
# Returns: matrix
# Output: ...
# Action: ...
# main_calc_flow_nd_dist_mat <- function(dt_points, dt_network, dt_dist_mat) {
# 	
# 	### 1. Prepare the data ------------------------------------------------------
# 	# sf_trips$origin_geom <- lwgeom::st_startpoint(sf_trips$geometry)
# 	# sf_trips$dest_geom <- lwgeom::st_endpoint(sf_trips$geometry)
# 	
# 	
# 	
# 	
# 	dt_origin <- sf_trips %>%
# 		st_set_geometry("o_closest_point") %>%
# 		select(flow_id, origin_id, o_closest_point) %>%
# 		rename("id" = "flow_id",
# 					 "id_edge" = "origin_id",
# 					 "geom" = "o_closest_point") %>%
# 		as.data.table()
# 	
# 
# 		
# 	
# 	# dt_origin <- sf_trips %>%
# 	# 	st_set_geometry("origin_geom") %>%
# 	# 	select(flow_id, origin_id, origin_geom) %>%
# 	# 	rename("id" = "flow_id",
# 	# 				 "id_edge" = "origin_id",
# 	# 				 "geom" = "origin_geom") %>%
# 	# 	as.data.table()
# 	dt_origin <- add_dist_start_end(dt_origin)
# 	
# 	
# 	
# 	dt_dest <- sf_trips %>%
# 		st_set_geometry("d_closest_point") %>%
# 		select(flow_id, dest_id, d_closest_point) %>%
# 		rename("id" = "flow_id",
# 					 "id_edge" = "dest_id",
# 					 "geom" = "d_closest_point") %>%
# 		as.data.table
# 	
# 	# dt_dest <- sf_trips %>%
# 	# 	st_set_geometry("dest_geom") %>%
# 	# 	select(flow_id, dest_id, dest_geom) %>%
# 	# 	rename("id" = "flow_id",
# 	# 				 "id_edge" = "dest_id",
# 	# 				 "geom" = "dest_geom") %>%
# 	# 	as.data.table
# 	dt_dest <- add_dist_start_end(dt_dest)
# 	
# 	
# 	
# 	dt_network <- dt_network %>%
# 		select(source, target, id, geom_way)
# 	
# 	dt_origin <- dt_origin %>%
# 		arrange(id)
# 	
# 	dt_dest <- dt_dest %>%
# 		arrange(id)
# 	
# 	
# 	### 2. Calc ND between OD flows ----------------------------------------------
# 	# ND between origin points
# 	
# 	dt_o_pts_nd <- parallel_process_networks(dt_origin, 
# 																					 dt_network,
# 																					 dt_dist_mat,
# 																					 int_cores)
# 	
# 	
# 	#print("ND between origin points calculated")
# 	# ND between dest points
# 	dt_d_pts_nd <- parallel_process_networks(dt_dest, 
# 																					 dt_network,
# 																					 dt_dist_mat,
# 																					 int_cores)
# 	
# 	
# 	#print("ND between dest points calculated")
# 	# ND between OD flows
# 	# dt_flow_nd <- dt_o_pts_nd %>%
# 	# 	inner_join(dt_d_pts_nd, by = c("from" = "from", "to" = "to")) %>%
# 	# 	mutate(distance = distance.x + distance.y) %>%
# 	# 	select(flow_m = from, flow_n = to, distance) %>%
# 	# 	as.data.table
# 	# 
# 	# dt_flow_nd <- dt_flow_nd %>%
# 	# 	group_by(flow_m) %>%
# 	# 	mutate(row_id = row_number()) %>%
# 	# 	ungroup() %>%
# 	# 	as.data.table
# 	return(list("dt_o_pts_nd" = dt_o_pts_nd,
# 							"dt_d_pts_nd" = dt_d_pts_nd))
# 	### 3. Calc OD-flow ND matrix ------------------------------------------------
# 	# matrix_flow_dist <- calc_flow_nd_dist_mat(dt_flow_nd)
# 	# print("matrix conversion successful")
# 	# sym_mat <- matrix_flow_dist
# 	# sym_mat[is.na(sym_mat)] <- 0
# 	# sym_mat <- pmax(sym_mat, t(sym_mat))  # Elementweise das Maximum der Matrix und ihrer Transponierten
# 	# sym_mat[sym_mat == 0] <- NA
# 	# print("symmat successful")
# 	# 
# 	# return(sym_mat)
# }


# Documentation: main_calc_flow_euclid_dist_mat
# Usage: main_calc_flow_euclid_dist_mat(sf_trips)
# Description: Creates a matrix containing the euclidean distance between OD 
	# flows depending
# Args/Options: sf_trips
# Returns: matrix
# Output: ...
# Action: ...
# main_calc_flow_euclid_dist_mat <- function(sf_org) {
# 	sf_org$origin <- lwgeom::st_startpoint(sf_org$geometry)
# 	sf_org$dest <- lwgeom::st_endpoint(sf_org$geometry)
# 	sf_origin_distances <- st_distance(sf_org$origin, by_element = FALSE)
# 	sf_dest_distances <- st_distance(sf_org$dest, by_element = FALSE)
# 	matrix_distances <- drop_units(sf_origin_distances + sf_dest_distances)
# 	matrix_geom_dist <- calc_geom_dist_mat(sf_org)
# 	matrix_distances <- matrix_geom_dist
# }


main_calc_flow_euclid_dist_mat <- function(sf_trips, buffer) {
	
	# Get sf-vectors of start- and endpoints
	sf_trips$startpoint <- lwgeom::st_startpoint(sf_trips$geometry)
	sf_trips$endpoint <- lwgeom::st_endpoint(sf_trips$geometry)
	sf_points <- sf_trips %>%
		select(flow_id, startpoint, endpoint)
	sf_points$startpoint <- st_transform(st_sfc(sf_points$startpoint, crs = 32632), 
																			 crs = 4326)
	
	sf_points$endpoint <- st_transform(st_sfc(sf_points$endpoint, crs = 32632), 
																 crs = 4326)
	startpoints <- data.table(flow_id = sf_points$flow_id, 
														startpoint = st_coordinates(sf_points$startpoint))
	endpoints <- data.table(flow_id = sf_points$flow_id, 
													endpoint = st_coordinates(sf_points$endpoint))
	
	setnames(startpoints, c("flow_id", "startpoint_X", "startpoint_Y"))
	setnames(endpoints, c("flow_id", "endpoint_X", "endpoint_Y"))
	sf_coords <- merge(startpoints, endpoints, by = "flow_id")

	
	
	# Get sf-linestrings within buffer-area für each linestring
	sf_centroids <- st_centroid(sf_trips$geometry)
	sf_buffer <- st_buffer(sf_centroids, dist = buffer)
	intersections <- st_intersects(sf_buffer, sparse = TRUE)
	pairs_list <- lapply(seq_along(intersections), function(i) {
		data.table(flow_m = i, flow_n = unlist(intersections[[i]]))
	})
	dt_flow_euclid <- rbindlist(pairs_list)
	dt_flow_euclid <- dt_flow_euclid[flow_m != flow_n]
	dt_flow_euclid <- dt_flow_euclid[, .(flow_m = pmin(flow_m, flow_n),
													 flow_n = pmax(flow_m, flow_n))]
	
	# 'dt_flow_euclid' contains all the combinations for which dists are calculated
	dt_flow_euclid <- unique(dt_flow_euclid)
	
	

	# 'dt_merged' combines the combinations with the corresponding geometric features
	dt_merged <- merge(dt_flow_euclid, sf_coords, 
										 by.x = "flow_m", 
										 by.y = "flow_id",
										 suffixes = c("_m", "_n"))


	dt_merged <- merge(dt_merged,
										 sf_coords,
										 by.x = "flow_n",
										 by.y = "flow_id",
										 suffixes = c("_m", "_n"))


	
	# Parallelize distance calculation
	# Function to split indices into blocks
	split_indices <- function(n, nb) {
		split(1:n, cut(1:n, nb, labels = FALSE))
	}
	
	# Function to calculate haversine distances for a block of indices
	calculate_haversine_block <- function(block_indices, dt) {
		result <- t(sapply(block_indices, function(i) {
			start_dist <- distHaversine(c(dt$startpoint_X_m[i], dt$startpoint_Y_m[i]), c(dt$startpoint_X_n[i], dt$startpoint_Y_n[i]))
			end_dist <- distHaversine(c(dt$endpoint_X_m[i], dt$endpoint_Y_m[i]), c(dt$endpoint_X_n[i], dt$endpoint_Y_n[i]))
			c(start_dist, end_dist)
		}))
		return(result)
	}
	
	
	# Create a cluster with 14 cores
	cl <- makeCluster(14)
	registerDoParallel(cl)
	
	# Ensure the cluster is stopped after use
	on.exit(stopCluster(cl), add = TRUE)
	
	# Split the data into blocks
	n_blocks <- 14
	blocks <- split_indices(nrow(dt_merged), n_blocks)
	
	# Using foreach to calculate distances in parallel for each block
	distances <- foreach(block = blocks, .combine = rbind, .packages = 'geosphere') %dopar% {
		calculate_haversine_block(block, dt_merged)
	}

	# Add the distances to dt_merged
	dt_merged$start_dist <- distances[, 1]
	dt_merged$end_dist <- distances[, 2]
	dt_merged$distance <- rowSums(distances)
	print(head(dt_merged))

	dt_merged <- dt_merged %>%
		select(flow_m, flow_n, distance)
	
	
	dt_sym <- rbind(
		dt_merged,
		dt_merged[, .(flow_m = flow_n, flow_n = flow_m, distance = distance)]
	)
	

	return(dt_sym)
}



# Documentation: snn_flow
# Usage: snn_flow(flow_dist_mat, k, eps, minpts)
# Description: Executes the SNN_flow algorithm (Liu et al. (2022))
# Args/Options: flow_dist_mat, k, eps, minpts
# Returns: datatable
# Output: ...
# Action: ...
snn_flow <- function(ids, k, eps, minpts, dt_flow_distance) {
	matrix_knn <- cpp_find_knn(df = dt_flow_distance, k = k, ids) %>% 
		as.matrix
	### 1. Calculate SNN Density -------------------------------------------------
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
	
	#return(dt_cluster)
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
					 "id" = "V2") %>%
		select(id, cluster_pred)
	
	
	dt_noise_flows <- dt_cluster[which(!dt_cluster$flow 
																		 %in% dt_cluster_flows$id),"flow"] %>%
		rename(id = flow)
	
	
	dt_noise_flows$cluster_pred <- 0 
	
	dt_cluster_final <- rbind(dt_cluster_flows, dt_noise_flows)
	
	dt_cluster_final <- dt_cluster_final %>%
		arrange(id)
	
	p2 <- proc.time()
	time_diff <- p2-p1
	cat("Assign non-core flows: ", time_diff[[3]], " seconds.\n")
	return(dt_cluster_final)
}


# Documentation: snn_flow
# Usage: snn_flow(flow_dist_mat, k, eps, minpts)
# Description: Executes the SNN_flow algorithm (Liu et al. (2022))
# Args/Options: flow_dist_mat, k, eps, minpts
# Returns: datatable
# Output: ...
# Action: ...
snn_polygon <- function(ids, k, eps, minpts, dt_flow_distance) {
	matrix_knn <- cpp_find_knn_polygons(df = dt_flow_distance, k = k, ids) %>% 
		as.matrix

	### 1. Calculate SNN Density -------------------------------------------------
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
	
	#return(dt_cluster)
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
					 "id" = "V2") %>%
		select(id, cluster_pred)
	
	
	dt_noise_flows <- dt_cluster[which(!dt_cluster$flow 
																		 %in% dt_cluster_flows$id),"flow"] %>%
		rename(id = flow)
	
	
	dt_noise_flows$cluster_pred <- 0 
	
	dt_cluster_final <- rbind(dt_cluster_flows, dt_noise_flows)
	
	dt_cluster_final <- dt_cluster_final %>%
		arrange(id)
	
	p2 <- proc.time()
	time_diff <- p2-p1
	cat("Assign non-core flows: ", time_diff[[3]], " seconds.\n")
	return(dt_cluster_final)
}




# Documentation: snn_flow
# Usage: snn_flow(flow_dist_mat, k, eps, minpts)
# Description: Executes the SNN_flow algorithm (Liu et al. (2022))
# Args/Options: flow_dist_mat, k, eps, minpts
# Returns: datatable
# Output: ...
# Action: ...
snn_flow_opt <- function(sf_trips, k, eps, minpts, dt_flow_distance) {
	all_flow_ids <- unique(sf_trips$flow_id)
	matrix_knn <- cpp_find_knn_flows(df = dt_flow_distance, k = k, all_flow_ids) %>% as.matrix
	
	
	### 1. Calculate SNN Density -------------------------------------------------
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






main_map_od_points_on_cluster_networks <- function(files) {
	con <- dbConnect(RPostgres::Postgres(),
									 dbname = dbname,
									 host = host,
									 user = user,
									 password = pw,
									 port = port,
									 options = "-c client_min_messages=warning")
	
	on.exit(dbDisconnect(con))
	processed_files <- list()
	
	for (file in files) {
		temp_table_data <- paste0("temp_nb_timestamp_data_", gsub("[^a-zA-Z0-9]", "_", basename(file)))
		temp_table_filtered <- paste0("temp_nb_timestamp_data_filtered_", gsub("[^a-zA-Z0-9]", "_", basename(file)))
		
		# Erstelle eindeutige temporäre Tabellen
		query <- paste0("DROP TABLE IF EXISTS ", temp_table_data, ";")
		dbExecute(con, query)
		query <- paste0("
            CREATE TEMP TABLE ", temp_table_data, " (
                time_stamp TIMESTAMP,
                lng DOUBLE PRECISION,
                lat DOUBLE PRECISION,
                geometry GEOMETRY(POINT, 32632)
            );
        ")
		dbExecute(con, query)
		
		query <- paste0("DROP TABLE IF EXISTS ", temp_table_filtered, ";")
		dbExecute(con, query)
		query <- paste0("
            CREATE TEMP TABLE ", temp_table_filtered, " (
                time_stamp TIMESTAMP,
                lng DOUBLE PRECISION,
                lat DOUBLE PRECISION,
                geometry GEOMETRY(POINT, 32632)
            );
        ")
		dbExecute(con, query)
		
		# Prozessiere Dateien
		dt_nb <- read_rds(here::here(file)) %>% as.data.table
		posixct_ts <- dt_nb[1, "time_stamp"]
		dt_nb <- dt_nb[, .(time_stamp, lat, lng)]
		processed_files[[file]] <- posixct_ts
		
		sf_nb <- st_as_sf(
			dt_nb,
			coords = c("lng", "lat"), 
			crs = 4326,               
			remove = FALSE
		)
		sf_nb <- st_transform(sf_nb, crs = 32632)
		
		# Schreibe Daten in die eindeutige temporäre Tabelle
		dbWriteTable(con, temp_table_data, sf_nb, append = TRUE, row.names = FALSE)
		
		# Filtere Punkte innerhalb der convex_hull
		query <- paste0("
            INSERT INTO ", temp_table_filtered, "
            SELECT * FROM ", temp_table_data, "
            WHERE ST_Within(
                geometry,
                (SELECT geom_convex_hull FROM ", char_schema, ".", char_network, "_convex_hull)
            );
        ")
		dbExecute(con, query)
		
		# Füge einen räumlichen Index zur gefilterten Tabelle hinzu
		query <- paste0("CREATE INDEX ON ", temp_table_filtered, " USING GIST (geometry);")
		dbExecute(con, query)
		
		# Schreibe Ergebnisse in die time_series Tabelle
		query <- paste0("
            INSERT INTO ", char_schema, ".time_series (timestamp, cluster, count)
            SELECT 
                time_stamp AS timestamp, 
                n.cluster_pred AS cluster,
                COUNT(*) AS count
            FROM ", temp_table_filtered, "
            CROSS JOIN LATERAL (
                SELECT
                    id,
                    cluster_pred,
                    geom_way
                FROM ", char_schema, ".", char_network_clusters, " AS n
                ORDER BY
                    geom_way <-> ", temp_table_filtered, ".geometry
                LIMIT 1
            ) AS n
            GROUP BY time_stamp, n.cluster_pred;
        ")
		dbExecute(con, query)
		
		# Lösche temporäre Tabellen
		dbExecute(con, paste0("DROP TABLE IF EXISTS ", temp_table_data, ";"))
		dbExecute(con, paste0("DROP TABLE IF EXISTS ", temp_table_filtered, ";"))
	}
	
	return(processed_files)
}
