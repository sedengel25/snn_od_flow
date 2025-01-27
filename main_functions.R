source("./src/functions.R")

# Documentation: main_nd_dist_mat_ram_synth
# Usage: main_nd_dist_mat_ram_synth(sf_trips, dt_network, dt_dist_mat)
# Description: Creates a (ram-heavy) matrix containing the ND between OD flows depending
	# on the given road network and the given local node distance matrix
# Args/Options: sf_trips, dt_network, dt_dist_mat
# Returns: matrix
# Output: ...
# Action: ...
main_nd_dist_mat_ram_synth <- function(sf_trips, dt_network, dt_dist_mat) {
	
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
	
	dt_d_pts_nd <- parallel_process_networks(dt_dest, 
																					 dt_network,
																					 dt_dist_mat,
																					 int_cores)
	
	
	return(list("dt_o_pts_nd" = dt_o_pts_nd,
							"dt_d_pts_nd" = dt_d_pts_nd))
}



# Documentation: main_nd_dist_mat_ram
# Usage: main_nd_dist_mat_ram(sf_trips, dt_network, dt_dist_mat)
# Description: Creates a (ram-heavy) matrix containing the ND between OD flows depending
	# on the given road network and the given local node distance matrix
# Args/Options: sf_trips, dt_network, dt_dist_mat
# Returns: matrix
# Output: ...
# Action: ...
main_nd_dist_mat_ram <- function(sf_trips, dt_network, dt_dist_mat) {
	
	dt_origin <- sf_trips %>%
		st_set_geometry("o_closest_point") %>%
		select(flow_id, origin_id, o_closest_point) %>%
		rename("id" = "flow_id",
					 "id_edge" = "origin_id",
					 "geom" = "o_closest_point") %>%
		as.data.table()


	

	
	dt_dest <- sf_trips %>%
		st_set_geometry("d_closest_point") %>%
		select(flow_id, dest_id, d_closest_point) %>%
		rename("id" = "flow_id",
					 "id_edge" = "dest_id",
					 "geom" = "d_closest_point") %>%
		as.data.table


	dt_origin <- add_dist_start_end(dt_origin)
	dt_dest <- add_dist_start_end(dt_dest)
	
	
	dt_network <- dt_network %>%
		select(source, target, id, geom_way)
	
	dt_origin <- dt_origin %>%
		arrange(id)

	dt_dest <- dt_dest %>%
		arrange(id)
	print(dt_origin)
	print(dt_dest)
	### 2. Calc ND between OD flows ----------------------------------------------
	# ND between origin points
	gc()
	dt_o_pts_nd <- parallel_process_networks(dt_origin, 
																					dt_network,
																					dt_dist_mat,
																					int_cores)

	gc()
	# ND between dest points
	dt_d_pts_nd <- parallel_process_networks(dt_dest, 
																					 dt_network,
																					 dt_dist_mat,
																					 int_cores)
	gc()
	
	rm(dt_dist_mat)
	gc()
	dt_o_pts_nd <- dt_o_pts_nd %>% as.data.table()
	dt_d_pts_nd <- dt_d_pts_nd %>% as.data.table()
	gc()
	
	dt_flow_nd <- merge(dt_o_pts_nd, dt_d_pts_nd, by = c("from", "to"))
	rm(dt_o_pts_nd)
	rm(dt_d_pts_nd)
	gc()
	dt_flow_nd[, distance := distance.x + distance.y]
	dt_flow_nd <- dt_flow_nd[, .(flow_m = from, flow_n = to, distance)]
	gc()
	
	dt_sym <- rbind(
		dt_flow_nd,
		dt_flow_nd[, .(flow_m = flow_n, flow_n = flow_m, distance = distance)]
	)
	rm(dt_flow_nd)
	gc()
	
	dt_flow_nd <- dt_sym
	rm(dt_sym)
	gc()
	
	dt_flow_nd <- dt_flow_nd %>%
		rename(from = flow_m,
					 to = flow_n)
	
	return(dt_flow_nd)
	
	# convert_distmat_dt_to_matrix(dt_dist_mat = dt_flow_nd)
	
	# return(matrix_flow_nd)
}

# Documentation: main_euclid_dist_mat_cpu
# Usage: main_euclid_dist_mat_cpu(char_schema, char_trips, n, cores)
# Description: Creates a (cpu-heavy) matrix containing the ED between all 
# OD flows of the considered dataset
# Args/Options: char_schema, char_trips, n, cores
# Returns: ...
# Output: ...
# Action: psql-query
main_calc_diff_flow_distances <- function(char_schema, char_trips, n, cores){
	
	
	
	query <- paste0("DROP INDEX IF EXISTS ", paste0(char_schema,
																									".data_origin_geom_idx"))
	cat(query,"\n")
	dbExecute(con, query)
	
	
	query <- paste0("CREATE INDEX ON ", paste0(char_schema, ".data"),
									" USING GIST(origin_geom);")
	cat(query,"\n")
	dbExecute(con, query)
	
	query <- paste0("DROP INDEX IF EXISTS ", paste0(char_schema,
																									".data_dest_geom_idx"))
	cat(query,"\n")
	dbExecute(con, query)
	
	query <- paste0("CREATE INDEX ON ", paste0(char_schema, ".data"),
									" USING GIST(dest_geom);")
	cat(query,"\n")
	dbExecute(con, query)
	
	query <- paste0("DROP TABLE IF EXISTS ", paste0(char_schema, ".flow_distances"))
	cat(query,"\n")
	dbExecute(con, query)
	
	
# 	query <- paste0("CREATE TABLE ", paste0(char_schema, ".flow_distances"),
# 									" (flow_id_i INTEGER,
# 									flow_id_j INTEGER,
# 									flow_manhattan_pts_euclid INTEGER,
#         				  flow_chebyshev_pts_euclid INTEGER,
#         			    flow_euclid_norm DOUBLE PRECISION,
#         					flow_euclid INTEGER,
#         					length_similarity INTEGER,
#         					cosine_similarity DOUBLE PRECISION,
# 									flow_manhattan_pts_network INTEGER);")
# 	cat(query)
# 	dbExecute(con, query)
	
	
	query <- paste0("CREATE TABLE ", paste0(char_schema, ".flow_distances"),
									" (flow_id_i INTEGER,
									flow_id_j INTEGER,
									flow_manhattan_pts_euclid INTEGER,
									flow_manhattan_pts_network INTEGER);")
	cat(query)
	dbExecute(con, query)
	
	
	chunks <- r1_create_chunks(cores = cores, n = n)
	print(chunks)
	### Calculate euclidean distance between origin points
	t1 <- proc.time()
	parallel::mclapply(1:length(chunks), function(i) {
		
		local_con <- dbConnect(Postgres(),
													 dbname = dbname,
													 host = host,
													 user = user,
													 password = pw,
													 sslmode = "require")
		on.exit(dbDisconnect(local_con), add = TRUE) 
		id_start <- ifelse(i == 1, 1, chunks[i - 1] + 1)
		id_end <- chunks[i]
		
		# psql1_calc_distances_all(char_schema = char_schema,
		# 										 id_start = id_start,
		# 										 id_end = id_end,
		# 										 local_con = local_con)
		psql1_calc_euclidean_flow_dist(char_schema = char_schema,
		id_start = id_start,
		id_end = id_end,
		local_con = local_con)
		

		
	}, mc.cores = cores)
	
	t2 <- proc.time()
	diff_time <- t2-t1
	diff_time <- diff_time[3] %>% as.numeric
	cat("Time for calculating flow_distances between ", n, "OD flows: ",
			diff_time, "\n")
	
}
	
	
main_nd_dist_mat_cpu <- function(char_schema, 
																 char_network,
																 char_dist_mat, 
																 n, 
																 cores){
	
	

	
	chunks <- r1_create_chunks(cores = cores, n = n)
	calc_nd_between_od_points(char_schema = char_schema,
														cores = cores,
														n = n,
														char_network = char_network,
														char_dist_mat = char_dist_mat,
														chunks = chunks)
	
	# calc_nd2_between_od_points(char_schema = char_schema,
	# 													cores = cores,
	# 													n = n,
	# 													char_network = char_network,
	# 													char_dist_mat = char_dist_mat,
	# 													chunks = chunks)

	t1 <- proc.time()
	print(chunks)
	insert_flow_nd_in_distance_table(char_schema = char_schema,
																	 chunks = chunks,
																	 cores = cores)
	
}






main_psql_dist_mat_to_matrix <- function(char_schema, dist_measure, n, cores){
	
	chunks <- r1_create_chunks(cores = cores, n = n)
	print(chunks)
	results <- mclapply(1:length(chunks), function(i) {
		
		# Lokale PostgreSQL-Verbindung in jedem Worker
		local_con <- dbConnect(Postgres(),
													 dbname = dbname,
													 host = host,
													 user = user,
													 password = pw,
													 sslmode = "require")
		on.exit(dbDisconnect(local_con), add = TRUE)
		
		id_start <- ifelse(i == 1, 1, chunks[i - 1] + 1)
		id_end <- chunks[i]

		
  	json_file <- here::here(path_pacmap, dist_measure, paste0("chunk_", id_start, "_", id_end, ".json"))
		
		if (file.exists(json_file)) {
			file.remove(json_file)
		}

		query <- paste0("COPY (
		    SELECT json_agg(row_to_json(t))
		    FROM (
		        SELECT 
		            flow_id_i AS key,
		            jsonb_object_agg(flow_id_j, ", dist_measure, ") AS value
		        FROM ", char_schema, ".flow_distances
		        WHERE flow_id_i BETWEEN ", id_start, " AND ", id_end, "
		        GROUP BY flow_id_i
		        ORDER BY flow_id_i
		    ) t
		) TO '", json_file, "';")
		cat(query)
		dbExecute(local_con, query)
		
	}, mc.cores = cores)
	
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


main_remove_dangling_edges <- function(sf_network) {
	sf_network_sfs <- as_sfnetwork(sf_network)
	
	from_degrees <- sf_network_sfs %>%
		activate(edges) %>%
		as_tibble() %>%
		count(from, name = "from_degree") %>%
		as.data.frame() %>%
		select(from, from_degree) %>%
		rename(node = from)
	
	
	to_degrees <- sf_network_sfs %>%
		activate(edges) %>%
		as_tibble() %>%
		count(to, name = "to_degree") %>%
		as.data.frame() %>%
		select(to, to_degree) %>%
		rename(node = to)
	
	node_degrees <- full_join(from_degrees, 
														to_degrees, 
														by = "node")
	node_degrees <- node_degrees %>%
		mutate(total_degree = coalesce(from_degree, 0) + coalesce(to_degree, 0)) %>%
		select(node, total_degree)
	
	
	dangling_edges <- sf_network_sfs %>%
		activate(edges) %>%
		as_tibble() %>%
		left_join(node_degrees, by = c("from" = "node")) %>%
		rename(from_degree = total_degree) %>%
		left_join(node_degrees, by = c("to" = "node")) %>%
		rename(to_degree = total_degree) %>%
		mutate(is_dangling = (from_degree == 1 | to_degree == 1)) %>%
		filter(is_dangling)
	
	dangling_edge_ids <- dangling_edges$id
	sf_network_cleaned <- sf_network_sfs %>%
		activate(edges) %>%
		filter(!id %in% dangling_edge_ids) %>%
		st_as_sf()
}
