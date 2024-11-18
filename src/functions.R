source("./src/utils/cmd.R")
source("./src/utils/psql_1.R")
source("./src/utils/psql_2.R")
source("./src/utils/r_1.R")
source("./src/utils/r_2.R")

################################################################################
# main_input.R
################################################################################
# Documentation: osmconvert_create_sub_osm_pbf
# Usage: osmconvert_create_sub_osm_pbf()
# Description: Creates a sub osm.pbf-file based on the polygon specified
# Args/Options: ...
# Returns: ...
# Output: ...
# Action: Executes a 'osmconvert' command
osmconvert_create_sub_osm_pbf <- function(file_pbf) {

	
	
	char_cmd_osmconvert <- paste(
		"osmconvert", 
		shQuote(file_pbf),
		paste("-B=", shQuote(char_polygon_file), sep=""), 
		paste("-o=", shQuote(char_pbf_file), sep="")
	)
	
	print(char_cmd_osmconvert)
	
	
	int_exit_status <- system(char_cmd_osmconvert)
	
	if(int_exit_status == 0){
		print(paste0(char_pbf_file, " successfully created in ", path_osm_pbf))
	}
}


# Documentation: osm2po_create_routable_network
# Usage: osm2po_create_routable_network()
# Description: Creates in osm2po a sql-file that generates a network based on the
# chosen region in the corresponding or sh-file
# Args/Options: ...
# Returns: ...
# Output: ...
# Action: Executing several cmd- and psql-queries
osm2po_create_routable_network <- function(int_crs) {
	
	# Prepare system command for executing the sh-file
	char_cmd_osm2po <- paste(
		"java -Xmx1g -jar", 
		shQuote(file_osm2po_jar),
		paste("prefix=", shQuote(char_region_abb), sep=""),
		paste("tileSize=", shQuote("x"), sep=""),
		shQuote(char_pbf_file),
		"postp.0.class=de.cm.osm2po.plugins.postp.PgRoutingWriter",
		paste("workDir=", shQuote(here::here(path_osm_sql,
																				 char_region_abb)), sep="")
)
	
	print(char_cmd_osm2po)
	# Use 'processx'-lib to control system processes in the backrgound
	p <- processx::process$new("bash", 
														 args = c("-c", char_cmd_osm2po),
														 stdout = "|", stderr = "|")
	proc_time_1 <- proc.time()
	
	while (!p$is_alive() || p$is_alive()) {
		
		Sys.sleep(1)
		timeout <- 180
		# Get output of the process each second
		output <- p$read_output_lines()
		error_output <- p$read_error_lines()
		finished_message <- "INFO  All Services STARTED. Press Ctrl-C to stop them."
		# If output contains the finishing message...
		if(length(output) > 0 && finished_message %in% output){
			
			#...and if the sql-file got created...
			if(char_sql_filename %in% list.files(path = here::here(path_osm_sql,
																														 char_region_abb),
																					 pattern = "*.sql")){
				print("SQL file for routable road network successfully created")
				#...the process is killed the while-loop breaks.
				p$kill()
				break
			}
			
		}
		proc_time_2 <- proc.time()
		time_diff <- ((proc_time_2 - proc_time_1) %>% as.numeric)[[3]]  
		# If the sql-file is not created after 3 minutes a timeout is thrown
		if(time_diff > timeout){
			print("Timeout")
			p$kill()
		}
	}
	
	
	# Execute SQL file to write table in PSQL database
	bash_execute_sql_file(path_to_sql_file = here::here(path_osm_sql, 
																											char_region_abb,
																											char_sql_filename))
	
	
	
	# Transform coordinate system
	srid <- psql1_get_srid(con, table = char_network)
	cat("SRID: ", srid, "\n")
	psql1_set_srid(con, table = char_network, srid)
	psql1_transform_coordinates(con, table = char_network, crs = int_crs)
	psql1_update_srid(con, table = char_network, crs = int_crs)
	psql1_create_spatial_index(con, table = char_network)
}






# Documentation: calc_local_node_dist_mat
# Usage: calc_local_node_dist_mat(buffer)
# Description: Calculates a distance matrix between the intersection of the given
	# road network and the given buffer
# Args/Options: buffer
# Returns: ...
# Output: ...
# Action: Executing several cmd- and psql-queries
calc_local_node_dist_mat <- function(buffer) {
	# Check whether osm2po has created a coherent graph
	igraph_network <- sf_network %>%
		as.data.frame() %>%
		rename("from" = "source",
					 "to" = "target") %>%
		select(from, to) %>% 
		igraph::graph_from_data_frame() 
	
	igraph_network_components <- igraph::components(igraph_network)
	
	cat("Total components: ", 
			igraph_network_components$csize %>% sum, "\n")
	
	cat("Biggest number of cohrerent components: ",
			max(igraph_network_components$csize), "\n")
	
	#reticulate::install_python()
	#reticulate::install_miniconda()
	# Initiate the environment
	reticulate::py_install("networkx", 
												 envname = "r-reticulate", 
												 method = "virtualenv",
												 pip = TRUE)

	reticulate::py_install("numpy==1.26.00", 
												 envname = "r-reticulate", 
												 method = "virtualenv",
												 pip = TRUE)
	reticulate::use_virtualenv("r-reticulate", required = TRUE)
	nx <- reticulate::import("networkx")
	np <- reticulate::import("numpy")
	
	config <- py_config()
	config$numpy
	# Create a graph from the sub street network
	g <- nx$from_pandas_edgelist(df = sf_network,
															 source = "source",
															 target = "target",
															 edge_attr = "m",
															 edge_key = "id")
	
	nodes <- c(sf_network$source, sf_network$target) %>% unique
	
	list_dt <- vector("list", length(nodes))
	
	shortest_paths_per_node <- function(node, buffer) {
		res = nx$single_source_dijkstra_path_length(g, 
																								source = node, 
																								weight = "m", 
																								cutoff = buffer)
		
		targets = names(res) %>% as.integer
		sources = rep(node, length(targets))
		distances = res %>% as.numeric
		
		data.table(source = sources, target = targets, m = distances)
	}
	
	list_dt <- parallel::mclapply(nodes, 
																shortest_paths_per_node,
																buffer = int_buffer,
																mc.cores = int_cores)
	
	dt_dist_mat <- rbindlist(list_dt)
}



# Documentation: calc_flow_nd_dist_mat
# Usage: calc_flow_nd_dist_mat(dt_flow_nd)
# Description: Calculates a distance matrix for OD flows based on their actual 
# network distance  
# Args/Options: dt_flow_nd
# Returns: matrix
# Output: ...
# Action: ...
calc_flow_nd_dist_mat <- function(dt_flow_nd) {
	int_max_value <- max(dt_flow_nd$flow_m, dt_flow_nd$flow_n)
	
	int_big_m <- 9e10
	print("step_1")
	# Initiate a matrix with BIG M values
	matrix_flow_nd <- matrix(int_big_m, nrow = int_max_value, ncol = int_max_value)
	print("step_2")
	# Fill the matrix at the cells for which with real distances exist  
	matrix_flow_nd[cbind(dt_flow_nd$flow_m, dt_flow_nd$flow_n)] <- dt_flow_nd$distance
	print("step_3")
	# Create a matrix with TRUEs where real distances exist
	matrix_flow_nd_boolean <- matrix_flow_nd < int_big_m
	print("step_4")
	# Fills matrix with NAs where no real distances exist
	matrix_flow_nd_true <- ifelse(matrix_flow_nd_boolean, matrix_flow_nd,  NA)
	print("step_5")
	return(matrix_flow_nd_true)
}






# Documentation: calc_geom_dist_mat
# Usage: calc_geom_dist_mat(sf_trips, matrix_flow_dist)
# Description: Calculates a distance matrix for OD flows based on their scaled 
	# network distance as well as the scaled differences between their angles 
	# and their lengths 
# Args/Options: sf_trips, matrix_flow_dist
# Returns: matrix
# Output: ...
# Action: ...
calc_geom_dist_mat <- function(sf_trips, matrix_flow_dist) {
	# Scale matrix between 0 and 1
	matrix_flow_dist_true_scaled <- matrix_flow_dist %>%
		rescale(to = c(0, 1))
	
	# Calculate angles for each flow
	angles <- sapply(st_geometry(sf_trips), function(line) {
		coords <- st_coordinates(line)
		angle <- atan2(diff(coords[,2]), diff(coords[,1])) * (180 / pi)
		ifelse(angle < 0, angle + 360, angle)
	})
	
	# Calculate a matrix containing the differences in angles and scale it
	angle_diff_mat <- outer(angles, angles, FUN = function(x, y) abs(x - y)) %>%
		rescale(to = c(0, 1))
	# Calculate the lengths of all flows
	lengths <- sapply(st_geometry(sf_trips), function(line) {
		length <- st_length(line)
	})
	
	# Calculate a matrix containing the differences in lengths and scale it
	length_diff_mat <- outer(lengths, lengths, FUN = function(x, y) abs(x - y)) %>%
		rescale(to = c(0, 1))
	
	# Combine all the matrices into one
	final_dist_mat <- matrix_flow_dist_true_scaled + angle_diff_mat + length_diff_mat
	
	return(final_dist_mat)
}



################################################################################
# euclidean_snn.R
################################################################################
plot_optimal_k <- function(k_max, matrix_flow_distances) {
	
	
	matrix_knn_dist <- t(apply(matrix_flow_distances, 1, r1_get_knn_dist, k_max))

	dt_rkd <- r1_create_rkd_dt(k_max = k_max, matrix_knn_dist)
	
	plot <- ggplot(data = dt_rkd, aes(x = k, y = rkd)) +
		geom_point() +
		geom_line() +
		labs(x = "k",
				 y = "RKD") +
		theme_minimal(base_size = 40) +
		theme(axis.text.x = element_text(hjust = 0.5), 
					axis.title.x = element_text(margin = margin(t = 15)),
					axis.text.y = element_text(hjust = 0.5), 
					axis.title.y = element_text(margin = margin(r = 15)))
	
	
	return(plot)
}




calc_sil_score <- function(sf_trips) {
	# Calcualte silhoutte scores
	if(length(unique(sf_trips$cluster_pred)) <= 1){
		return(list("sil" = -1, 
								"geom_sil" = -1))
	}
	sf_trips <- sf_trips[sf_trips$cluster_pred!=0,]
	
	sf_trips$origin <- lwgeom::st_startpoint(sf_trips$geometry)
	sf_trips$dest <- lwgeom::st_endpoint(sf_trips$geometry)
	
	# Get distances between origin- and destination points...
	origin_distances <- st_distance(sf_trips$origin, by_element = FALSE)
	dest_distances <- st_distance(sf_trips$dest, by_element = FALSE)
	
	# ...in order to calculate a distance matrix for the flow
	dist_mat <- drop_units(origin_distances + dest_distances) 
	
	df_sil_scores <- cluster::silhouette(sf_trips$cluster_pred, 
																						dist_mat) %>% 
		as.data.frame() 
	
	if(ncol(df_sil_scores)!=3){
		return(list("sil" = -1, 
								"geom_sil" = -1))
	}
	average_sil_score <- mean(df_sil_scores[, 3])
	
	geom_dist_mat <- calc_geom_dist_mat(sf_trips, dist_mat)
	df_geom_sil_scores <- cluster::silhouette(sf_trips$cluster_pred, 
																			 geom_dist_mat) %>% 
		as.data.frame() 

	if(ncol(df_geom_sil_scores)!=3){
		return(list("sil" = -1, 
								"geom_sil" = -1))
	}
	average_geom_sil_score <- mean(df_geom_sil_scores[, 3])
	return(list("sil" = average_sil_score, 
								 "geom_sil" = average_geom_sil_score))
}





# Documentation: add_dist_start_end
# Usage: add_dist_start_end(dt_points)
# Description: Calculates distances between OD-poitns to start and end of the 
# corresponding road segment
# Args/Options: dt_points
# Returns: datatable
# Output: ...
# Action: ...
add_dist_start_end <- function(dt_points) {
	dt_points <- dt_points %>%
		left_join(dt_network, by = c("id_edge" ="id")) 
	
	dt_points <- dt_points%>%
		mutate(geom_origin = lwgeom::st_startpoint(geom_way),
					 geom_dest = lwgeom::st_endpoint(geom_way))
	
	dt_points <- dt_points %>%
		mutate(dist_to_start = round(st_distance(dt_points$geom, 
																			 dt_points$geom_origin,
																			 by_element = TRUE) %>% as.numeric(),0) %>%
					 	as.integer(),
					 dist_to_end = round(st_distance(dt_points$geom, 
					 													dt_points$geom_dest,
					 													by_element = TRUE) %>% as.numeric(),0)  %>%
					 	as.integer()) %>%
		select(id, id_edge, dist_to_start, dist_to_end)
	
	return(dt_points)
}
