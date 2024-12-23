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
	srid <- psql1_get_srid(con, table = char_network, "public")
	cat("SRID: ", srid, "\n")
	psql1_set_srid(con, table = char_network, srid, "public")
	psql1_transform_coordinates(con, table = char_network, crs = int_crs, "public")
	psql1_update_srid(con, table = char_network, crs = int_crs, "public")
	psql1_create_spatial_index(con, table = char_network, schema = "public")
}


instantiate_py <- function() {
  reticulate::py_install("networkx", 
  											 envname = "r-reticulate", 
  											 method = "virtualenv",
  											 pip = TRUE)
  
  reticulate::py_install("numpy==1.26.00", 
  											 envname = "r-reticulate", 
  											 method = "virtualenv",
  											 pip = TRUE)
  reticulate::use_virtualenv("r-reticulate", required = TRUE)
  nx <<- reticulate::import("networkx")
  np <<- reticulate::import("numpy")
  
  config <<- py_config()
  config$numpy
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
	instantiate_py()
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

################################################################################
# 10_snn_od_points.R
################################################################################
# Documentation: clean_point_clusters_lof
# Usage: clean_point_clusters_lof(sf_cluster)
# Description: Outlier removing from clusters based on LOF
# Args/Options: sf_cluster
# Returns: sf-dataframe
# Output: ...
# Action: ...
clean_point_clusters_lof <- function(sf_cluster) {
	total_clusters <- 1:max(sf_cluster$cluster_pred)
	
	list_cleaned_clusters <- list()
	for (cluster_id in total_clusters) {
		cluster_points <- sf_cluster %>% 
			filter(cluster_pred == cluster_id)
		coords <- st_coordinates(cluster_points)
		
		k <- ceiling(0.75*nrow(coords))
		lof_scores <- lof(coords, minPts = k)
		
		cluster_points$lof <- lof_scores
		
		cluster_points <- cluster_points %>% 
			filter(lof < 2)
		
		list_cleaned_clusters[[cluster_id]] <- cluster_points
	}
	
	sf_cluster <- rbindlist(list_cleaned_clusters) %>%
		as.data.frame() %>%
		st_as_sf()
	return(sf_cluster)
}

################################################################################
# 11_create_network_clusters.R
################################################################################
# Documentation: get_networks_per_cluster
# Usage: get_networks_per_cluster(sf_cluster_points, sf_network, g)
# Description: Get MST-Linestrings for cluster points
# Args/Options: sf_cluster_points, sf_network, g
# Returns: sf-dataframe
# Output: ...
# Action: ...
get_networks_per_cluster <- function(sf_cluster_points, 
																		 sf_network,
																		 g){
	int_total_ids <- 1:max(sf_cluster_points$cluster_pred)
	list_of_sf_subnetwork <- list()
	for (i in int_total_ids) {
		cat("Cluster: ", i, "\n")
		# Get initial subgraph for cluster i based on the edges where points lie on
		edges_cl <- list_cluster_edges[[i]]
		selected_edges <- E(g)[edge_id %in% edges_cl]
		subgraph <- subgraph.edges(g, selected_edges)
		
		# Get components of subgraph
		comp <- components(subgraph)
		
		# As long as there are multiple components...
		while (comp$no > 1) {
			component_nodes <- split(V(subgraph), comp$membership)
			
			# Iterate over component pairs
			for (j in 1:(length(component_nodes) - 1)) {
				for (k in (j + 1):length(component_nodes)) {
					node_j <- names(component_nodes[[j]])
					node_k <- names(component_nodes[[k]])
					
					# Create a grid containing node pairs
					pairs <- expand.grid(node_j, node_k)
					
					# Process each pair of nodes
					for (l in 1:nrow(pairs)) {
						# Get the shortest path
						sp <- shortest_paths(
							g,
							from = as.character(pairs[l, 1]),
							to = as.character(pairs[l, 2]),
							weights = E(g)$weight
						)
						
						# Extract the shortest path's edges
						edges_of_path <- E(g, path = sp$vpath[[1]])
						
						# Add edges to the subgraph
						subgraph <- induced_subgraph(
							g,
							vids = unique(c(as_ids(V(subgraph)), as.character(ends(g, edges_of_path))))
						)
						
						# Recalculate the components
						comp <- components(subgraph)
						
						# If components have changed, break all loops
						if (comp$no < length(component_nodes)) {
							#print("Components changed")
							break
						}
					}
					if (comp$no < length(component_nodes)) break
				}
				if (comp$no < length(component_nodes)) break
			}
		}
		sf_subnetwork <- sf_network %>%
			filter(id %in% E(subgraph)$edge_id) %>%
			mutate(cluster_pred = i)
		list_of_sf_subnetwork[[i]] <- sf_subnetwork 
	}
	
	sf_cluster_networks <- rbindlist(list_of_sf_subnetwork)
	return(sf_cluster_networks)
}

# Documentation: get_outer_points_per_cluster
# Usage: get_outer_points_per_cluster(sf_cluster_points, sf_cluster_networks)
# Description: Get most outer points of each cluster
# Args/Options: sf_cluster_points, sf_cluster_networks
# Returns: sf-dataframe
# Output: ...
# Action: ...
get_outer_points_per_cluster <- function(sf_cluster_points, sf_cluster_networks) {
	
	cluster_ids <- 1:max(sf_cluster_points$cluster_pred)
	
	list_of_outer_points <- lapply(cluster_ids, function(i){
		cat("Cluster: ", i, "\n")
		i=22
		points <- sf_cluster_points %>%
			filter(cluster_pred == i) %>%
			as.data.table()
		
		lines <- sf_cluster_networks %>%
			filter(cluster_pred==i)
		
		node_counts <- data.table(node = c(lines$source, lines$target)) %>%
			group_by(node) %>%
			summarise(count = n())
		
		
		outer_edges <- lines %>%
			filter(
				source %in% node_counts$node[node_counts$count == 1] |
					target %in% node_counts$node[node_counts$count == 1]
			) %>%
			as.data.table()
		
		
		outer_edges <- outer_edges %>%
			mutate(
				unattached_at_source = !(source %in% lines$target | 
																 	source %in% lines$source[-match(source, lines$source)]),
				unattached_at_target = !(target %in% lines$source | 
																 	target %in% lines$target[-match(target, lines$target)])
			)
		
		rows <- which(points$id_edge %in% outer_edges$id)
		outer_points <- points[rows,]
		
		# ggplot()+
		# 	geom_sf(data=st_as_sf(outer_edges))+
		# 	geom_sf(data = st_as_sf(points))
		
		outer_points <- outer_points %>%
			left_join(outer_edges %>% select(id, unattached_at_source, unattached_at_target),
								by = c("id_edge" = "id"))
		
		most_outer_points <- outer_points %>%
			group_by(id_edge) %>%
			filter(
				(unattached_at_source & dist_to_start == min(dist_to_start)) | 
					(unattached_at_target & dist_to_end == min(dist_to_end))
			) %>%
			ungroup() %>%
			as.data.frame() %>%
			st_as_sf()
		
		most_outer_points
	})
	sf_outer_points <- rbindlist(list_of_outer_points) %>%
		st_as_sf()
	return(sf_outer_points)
}




# Documentation: get_outer_points_per_cluster
# Usage: get_outer_points_per_cluster(sf_cluster_points, sf_cluster_networks)
# Description: Get most outer points of each cluster
# Args/Options: sf_cluster_points, sf_cluster_networks
# Returns: sf-dataframe
# Output: ...
# Action: ...
reduce_networks_to_op <- function(sf_cluster_networks, 
																	sf_cluster_outerpoints) {
	total_ids <- 1:max(sf_cluster_networks$cluster_pred)
	
	list_sf_networks <- list()
	
	
	for (i in total_ids){
		cat("Cluster: ", i, "\n")
		# Get outer points per cluster
		op1 <- sf_cluster_outerpoints %>%
			filter(cluster_pred==i) %>%
			as.data.frame() %>%
			st_as_sf()
		
		# Get street network involved in cluster
		npc1 <- sf_cluster_networks %>%
			filter(cluster_pred==i) %>%
			as.data.frame() %>%
			st_as_sf()
		
		# ggplot()+
		#  	geom_sf(data=op1)+
		#  	geom_sf(data=npc1)
		
		# Transform cluster-network to sf-network
		network_cl <- as_sfnetwork(npc1, directed = FALSE)
		
		network_cl_nodes <- network_cl %>%
			activate("nodes") %>%
			as.data.frame() %>%
			st_as_sf()
		
		network_cl_edges <- network_cl %>%
			activate("edges") %>%
			as.data.frame() %>%
			st_as_sf() 
		
		
		# If cluster consists of only one point (station) draw a linestring to a almost
		# identical point to get the same sfc-format for all clusters
		if(nrow(op1)==1 & nrow(npc1)==1){
			coords <- st_coordinates(op1)
			near_coords <- coords + c(0.0001, -0.0001)
			new_linestring <- st_linestring(rbind(coords, near_coords))
			st_geometry(network_cl_edges) <- st_sfc(new_linestring,
																							crs = st_crs(network_cl_edges))
			
			list_sf_networks[[i]] <- network_cl_edges
			next
		}
		# ggplot()+
		#  	geom_sf(data=network_cl_edges)+
		#  	geom_sf(data=network_cl_nodes)+
		# 	geom_sf(data=op1)
		
		
		# Find outerpoints that are also nodes in the sf-network...
		duplicates <- st_intersects(op1, network_cl_nodes, sparse = FALSE)
		idx_duplicates <- which(duplicates, arr.ind = TRUE)
		idx_duplicates <- idx_duplicates[,1]
		
		# when 1 line, 2 outerpoints und 1 outerpoint==network node, dann
		if(nrow(op1)==2 & nrow(npc1)==1 & length(idx_duplicates)==1){
			network_cl_ext <- st_network_blend(network_cl, op1)
			# 1. Set index for row number
			network_cl_ext <- network_cl_ext %>%
				activate("nodes") %>%
				mutate(node_index = row_number()) # Numeriere Knoten explizit
			
			# 2. Get the row with NA-entry as this got added by blending but we don't want it
			na_nodes <- network_cl_ext %>%
				activate("nodes") %>%
				filter(is.na(cluster_od) | is.na(id_edge)) %>%
				pull(node_index)
			
			network_cl_ext_edges <- network_cl_ext %>%
				activate("edges") %>%
				filter(!from %in% na_nodes & !to %in% na_nodes) %>%
				as.data.frame() %>%
				st_as_sf()
			
			# ggplot()+
			# 	geom_sf(data=network_cl_ext_edges)+
			# 	geom_sf(data=op1)
			
			list_sf_networks[[i]] <- network_cl_ext_edges
			
			next	
		}
		
		if(length(idx_duplicates)>0){
			op1 <- op1[-idx_duplicates,]
		}
		
		# ggplot()+
		# 	geom_sf(data=op1)+
		# 	geom_sf(data=npc1)
		
		# Turn outer points into nodes of the network
		network_cl_ext <- st_network_blend(network_cl, op1)
		
		network_cl_ext_nodes <- network_cl_ext %>%
			activate("nodes") %>%
			st_as_sf()
		
		network_cl_ext_edges <- network_cl_ext %>%
			activate("edges") %>%
			st_as_sf()
		
		network_cl_ext_edges <- network_cl_ext_edges %>%
			mutate(id_new = 1:nrow(network_cl_ext_edges))
		
		# ggplot()+
		#  	geom_sf(data=st_as_sf(network_cl_ext_edges))+
		#  	geom_sf(data=network_cl_ext_nodes)
		
		# Find nodes that appear only once and thus represent outer nodes
		node_counts <- data.table(node = c(network_cl_ext_edges$from, 
																			 network_cl_ext_edges$to)) %>%
			group_by(node) %>%
			summarise(count = n())
		
		# Find the corresponding edges
		all_outer_edges <- network_cl_ext_edges %>%
			filter(
				from %in% node_counts$node[node_counts$count == 1] |
					to %in% node_counts$node[node_counts$count == 1]
			) %>%
			as.data.table()
		
		
		
		# From all outer edges, find the ones that were newly created by the blending...
		new_outer_edges <- all_outer_edges %>%
			anti_join(network_cl_edges, by = c("from", "to"))
		
		
		# ... and remove them
		# This can be dangerous in the edge-case of cluster 62 where the 2 outerpoints
		# are on the one single linestring and it is also equal to one node of
		# the original linestring
		network_cl_ext_edges <- network_cl_ext_edges %>%
			filter(!id_new %in% new_outer_edges$id_new) %>%
			st_as_sf()
		
		
		p <- ggplot()+
			geom_sf(data=network_cl_ext_edges)+
			geom_sf(data=op1) 
	
		network_cl_ext_edges <- network_cl_ext_edges %>%
			select(-id_new)
		list_sf_networks[[i]] <- network_cl_ext_edges
	}
	
	sf_cluster_networks <- rbindlist(list_sf_networks) %>%
		st_as_sf()
	
	return(sf_cluster_networks)
}

################################################################################
# pacmap.R
################################################################################

# Documentation: load_pacmap_embedding
# Usage: load_pacmap_embedding(sdistance, np)
# Description: Load numpy file created by pacmap and turn it into a R-df
# Args/Options: distance, np
# Returns: df
# Output: ...
# Action: ...
load_pacmap_embedding <- function(path_pacmap, distance, np) {
	path <- here::here(path_pacmap, distance)
	embedding <- np$load(here::here(path, "embedding.npy")) %>%
		as.data.frame()
	
	
	df_embedding <- data.frame(
		x = embedding[, 1],
		y = embedding[, 2]
	)
	df_embedding$id <- 1:nrow(df_embedding)
	df_embedding$dist_measure <- distance
	return(df_embedding)
}



pacmap_density_plot <- function(df_embedding) {
	dist <- df_embedding[1,4] %>% 
					as.character()
  p <- ggplot(df_embedding, aes(x = x, y = y)) +
  	geom_point(alpha = 0.1) +  
  	stat_density2d(aes(fill =after_stat(level)),
  								 geom = "polygon",
  								 alpha = 1,
  								 adjust = 0.1,
  								 n = 150) +
  	scale_fill_viridis_c(option = "plasma") +  
  	theme_minimal() +
  	labs(
  		title = paste0("PACMAP for ", dist),
  		x = "x",
  		y = "y",
  		fill = "Density"
  	)
  
  print(p)
}


# Documentation: create_pacmap_plotly_with_clusters
# Usage: create_pacmap_plotly_with_clusters(df_embedding, df_cluster)
# Description: Creates a plotyly with clsuters assigned to the pacmap
# Args/Options: df_embedding, df_cluster
# Returns: ...
# Output: plotly
# Action: ...
create_pacmap_plotly_with_clusters <- function(df_pacmap) {

	int_clusters <- sort(unique(df_pacmap$cluster))
	
	# valid_colors <- colors()[!grepl("black|gray|grey", colors(), ignore.case = TRUE)]
	# print(valid_colors)
	random_colors <- setNames(
		c("black", sample(colors(), length(int_clusters) - 1)),  # Verwende gefilterte Farben
		int_clusters
	)
	
	p <- ggplot(df_pacmap 
							%>% filter(cluster != 0)
							, aes(x = x, y = y, color = factor(cluster))) +
		geom_point(size = 1, alpha = 0.3) +
		scale_color_manual(values = random_colors) +  # Verwende scale_color_manual
		labs(
			title = "PaCMAP by Clusters",
			x = "x",
			y = "y"
		) +
		theme_minimal() +
		theme(
			legend.position = "none",
			plot.title = element_text(hjust = 0.5, size = 16),
			axis.title = element_text(size = 12)
		)
	
	
	# Interaktives Plotly-Plot
	ggplotly(p, tooltip = c("x", "y", "color"))
}



py_get_dbcv <- function(np, 
												df_euclid_embedding, 
												df_nd_embedding, 
												cluster_labels_euclid,
												cluster_labels_nd) {
	labels_euclid <- np$array(cluster_labels_euclid, dtype = "int32")
	labels_nd <- np$array(cluster_labels_nd, dtype = "int32")
	x_euclid <- np$array(df_euclid_embedding[, c(1:2)] %>% as.matrix())
	x_nd <- np$array(df_nd_embedding[, c(1:2)] %>% as.matrix())
	dbcv_euclid <- hdbscan$validity$validity_index(x_euclid, labels_euclid,
																								 per_cluster_scores = FALSE)
	
	dbcv_nd <- hdbscan$validity$validity_index(x_nd, labels_nd, 
																						 per_cluster_scores = FALSE)
	return(list("dbcv_euclid" = dbcv_euclid, "dbcv_nd" = dbcv_nd))
}


py_hdbscan_dbcv <- function(np, 
													  df_embedding, 
														minpts) {
	dist <- df_embedding[1,4] %>% 
		as.character()
	x <- np$array(df_embedding[, c(1:2)] %>% as.matrix())
	hdbscan_model <- hdbscan$HDBSCAN(min_cluster_size = as.integer(minpts))
	print("Cluster with HDBSCAN")
	hdbscan_res <- hdbscan_model$fit(x)
	np_labels <- np$array(hdbscan_res$labels_, dtype = "int32")
	number_cluster <- table(hdbscan_res$labels_) %>% length 
	numb_noise <- which(hdbscan_res$labels_==-1) %>% length
	print(number_cluster)
	if(number_cluster <= 3){
		res_list <- list("dist" = dist,
										 "n_cl" = 	number_cluster,
										 "n_noise" = numb_noise,
										 "dbcv" = NA, 
										 "cdbw" = NA,
										 "comp" = NA,
										 "coh" = NA,
										 "sep" = NA)
		return(invisible(res_list))
	}
	print("Calculate DBCV")
	dbcv <- hdbscan$validity$validity_index(x, np_labels)
	df_cluster <- cbind(df_embedding[, c(1:2)], hdbscan_res$labels_)
	print("Calculate CDbw")

	cdbw <- cdbw(x = df_cluster[, c(1:2)], clustering = df_cluster[,3])
	colnames(df_cluster) <- c("x", "y", "cluster")
	df_cluster$cluster <- df_cluster$cluster + 1

	res_list <- list("dist" = dist,
			 "n_cl" = 	number_cluster,
			 "n_noise" = numb_noise,
			 "dbcv" = dbcv, 
			 "cdbw" = cdbw$cdbw,
			 "comp" = cdbw$compactness,
			 "coh" = cdbw$cohesion,
			 "sep" = cdbw$sep
			 #,
			 #"df_cluster" = df_cluster
			 )
	
	return(invisible(res_list))
}



calc_nd_between_od_points <- function(char_schema, 
																			cores, 
																			n, 
																			char_network, 
																			char_dist_mat,
																			chunks) {
	query <- paste0("DROP TABLE IF EXISTS ", paste0(char_schema, ".origin_nd"))
	dbExecute(con, query)
	
	query <- paste0("DROP TABLE IF EXISTS ", paste0(char_schema, ".dest_nd"))
	dbExecute(con, query)
	
	
	query <- paste0("CREATE TABLE ", paste0(char_schema, ".origin_nd"),
									" (flow_id_i INTEGER,
  									flow_id_j INTEGER,
  									flow_manhattan_pts_network INTEGER);")
	cat(query)
	dbExecute(con, query)
	
	
	query <- paste0("CREATE TABLE ", paste0(char_schema, ".dest_nd"),
									" (flow_id_i INTEGER,
  									flow_id_j INTEGER,
  									flow_manhattan_pts_network INTEGER);")
	cat(query)
	dbExecute(con, query)
	
	### Caluclate ND between origin points ---------------------------------------
	t1 <- proc.time()

	print(chunks)
	parallel::mclapply(1:length(chunks), function(i) {
		
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
		
		
		t1 <- proc.time()
		print(chunks)
		query <- paste0(
			"INSERT INTO ", char_schema, ".origin_nd SELECT m1.flow_id as flow_id_i,
  																					 m2.flow_id as flow_id_j, ",
			"LEAST(",
			"CAST(m1.d_dist_to_start + m2.d_dist_to_start + COALESCE(pi_pk.m, 0) AS INT), ",
			"CAST(m1.d_dist_to_end + m2.d_dist_to_end + COALESCE(pj_pl.m, 0) AS INT), ",
			"CAST(m1.d_dist_to_start + m2.d_dist_to_end + COALESCE(pi_pl.m, 0) AS INT), ",
			"CAST(m1.d_dist_to_end + m2.d_dist_to_start + COALESCE(pj_pk.m, 0) AS INT)",
			") AS flow_manhattan_pts_network ",
			"FROM ", char_schema, ".data  m1 ",
			"CROSS JOIN ", char_schema, ".data m2 ",
			"INNER JOIN ", char_network, " e_ij ON m1.origin_id = e_ij.id ",
			"INNER JOIN ", char_network, " e_kl ON m2.origin_id = e_kl.id ",
			"INNER JOIN ", char_dist_mat, " pi_pk ON pi_pk.source = LEAST(e_ij.source, e_kl.source) AND
  				pi_pk.target = GREATEST(e_ij.source, e_kl.source) ",
			"INNER JOIN ", char_dist_mat, " pj_pl ON pj_pl.source = LEAST(e_ij.target, e_kl.target) AND
  				pj_pl.target = GREATEST(e_ij.target, e_kl.target) ",
			"INNER JOIN ", char_dist_mat, " pi_pl ON pi_pl.source = LEAST(e_ij.source, e_kl.target) AND
  				pi_pl.target = GREATEST(e_ij.source, e_kl.target) ",
			"INNER JOIN ", char_dist_mat, " pj_pk ON pj_pk.source = LEAST(e_ij.target, e_kl.source) AND
  				pj_pk.target = GREATEST(e_ij.target, e_kl.source) ",
			"WHERE m1.flow_id >= ", id_start,
			" AND m1.flow_id <= ", id_end,
			" AND m1.flow_id < m2.flow_id	AND m1.origin_id != m2.origin_id;")
		dbExecute(local_con, query)
		
	}, mc.cores = cores)
	
	t2 <- proc.time()
	diff_time <- t2-t1
	diff_time <- diff_time[3] %>% as.numeric
	cat("Time for calculating ND between origin points: ", diff_time, "\n")
	
	### Caluclate ND between dest points ---------------------------------------
	t1 <- proc.time()
	print(chunks)
	parallel::mclapply(1:length(chunks), function(i) {
		
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
		
		
		query <- paste0(
			"INSERT INTO ", char_schema, ".dest_nd SELECT m1.flow_id as flow_id_i,
  																				 m2.flow_id as flow_id_j, ",
			"LEAST(",
			"CAST(m1.d_dist_to_start + m2.d_dist_to_start + COALESCE(pi_pk.m, 0) AS INT), ",
			"CAST(m1.d_dist_to_end + m2.d_dist_to_end + COALESCE(pj_pl.m, 0) AS INT), ",
			"CAST(m1.d_dist_to_start + m2.d_dist_to_end + COALESCE(pi_pl.m, 0) AS INT), ",
			"CAST(m1.d_dist_to_end + m2.d_dist_to_start + COALESCE(pj_pk.m, 0) AS INT)",
			") AS flow_manhattan_pts_network ",
			"FROM ", char_schema, ".data  m1 ",
			"CROSS JOIN ", char_schema, ".data m2 ",
			"INNER JOIN ", char_network, " e_ij ON m1.dest_id = e_ij.id ",
			"INNER JOIN ", char_network, " e_kl ON m2.dest_id = e_kl.id ",
			"INNER JOIN ", char_dist_mat, " pi_pk ON pi_pk.source = LEAST(e_ij.source, e_kl.source) AND
  			pi_pk.target = GREATEST(e_ij.source, e_kl.source) ",
			"INNER JOIN ", char_dist_mat, " pj_pl ON pj_pl.source = LEAST(e_ij.target, e_kl.target) AND
  			pj_pl.target = GREATEST(e_ij.target, e_kl.target) ",
			"INNER JOIN ", char_dist_mat, " pi_pl ON pi_pl.source = LEAST(e_ij.source, e_kl.target) AND
  			pi_pl.target = GREATEST(e_ij.source, e_kl.target) ",
			"INNER JOIN ", char_dist_mat, " pj_pk ON pj_pk.source = LEAST(e_ij.target, e_kl.source) AND
  			pj_pk.target = GREATEST(e_ij.target, e_kl.source) ",
			"WHERE m1.flow_id >= ", id_start,
			" AND m1.flow_id <= ", id_end,
			" AND m1.flow_id < m2.flow_id	AND m1.dest_id != m2.dest_id;")
		dbExecute(local_con, query)
		
	}, mc.cores = cores)
	
	t2 <- proc.time()
	diff_time <- t2-t1
	diff_time <- diff_time[3] %>% as.numeric
	cat("Time for calculating ND between dest points: ", diff_time, "\n")
	
	### Calculate ND between origin points on the same road segment -------------- 
	t1 <- proc.time()
	query <- paste0(
		"INSERT INTO ", char_schema, ".origin_nd
  		 SELECT m1.flow_id as flow_id_i, m2.flow_id as flow_id_j, 
  	   ABS(m1.o_dist_to_start - m2.o_dist_to_start) as flow_manhattan_pts_network
  	   FROM ", char_schema, ".data m1
  	   CROSS JOIN ",  char_schema, ".data m2
  	   INNER JOIN ", char_network, " e_ij ON m1.origin_id = e_ij.id
  	   INNER JOIN ", char_network, " e_kl ON m2.origin_id  = e_kl.id
  	   where m1.origin_id = m2.origin_id and m1.flow_id != m2.flow_id;"
	)
	dbExecute(con, query)
	t2 <- proc.time()
	diff_time <- t2-t1
	diff_time <- diff_time[3] %>% as.numeric
	cat("Time for calculating ND between origin points on same road segment: ", diff_time, "\n")
	
	
	### Calculate ND between dest points on the same road segment -------------- 
	t1 <- proc.time()
	query <- paste0(
		"INSERT INTO ", char_schema, ".dest_nd
  		 SELECT m1.flow_id as flow_id_i, m2.flow_id as flow_id_j, 
  	   ABS(m1.d_dist_to_start - m2.d_dist_to_start) as flow_manhattan_pts_network
  	   FROM ", char_schema, ".data m1
  	   CROSS JOIN ",  char_schema, ".data m2
  	   INNER JOIN ", char_network, " e_ij ON m1.dest_id = e_ij.id
  	   INNER JOIN ", char_network, " e_kl ON m2.dest_id  = e_kl.id
  	   where m1.dest_id = m2.dest_id and m1.flow_id != m2.flow_id;"
	)
	dbExecute(con, query)
	t2 <- proc.time()
	diff_time <- t2-t1
	diff_time <- diff_time[3] %>% as.numeric
	cat("Time for calculating ND between dest points on same road segment: ", diff_time, "\n")
	
	query <- paste0("DROP INDEX IF EXISTS ", char_schema, 
									".origin_nd_flow_id_i_flow_id_j_idx;")
	cat(query, "\n")
	dbExecute(con, query)
	
	query <- paste0("CREATE INDEX ON ",
									char_schema, ".origin_nd (flow_id_i, flow_id_j);")
	cat(query, "\n")
	dbExecute(con, query)
	
	query <- paste0("DROP INDEX IF EXISTS ", char_schema, 
									".dest_nd_flow_id_i_flow_id_j_idx;")
	cat(query, "\n")
	dbExecute(con, query)
	
	query <- paste0("CREATE INDEX ON ",
									char_schema, ".dest_nd (flow_id_i, flow_id_j);")
	cat(query, "\n")
	dbExecute(con, query)
	

	dbExecute(con, query)
}



insert_flow_nd_in_distance_table <- function(char_schema, chunks, cores) {
	query <- paste0("DROP TABLE IF EXISTS ",
									char_schema, ".new_flow_distances")
	cat(query, "\n")
	dbExecute(con, query)
	
	query <- paste0("CREATE TABLE ",
									char_schema, ".new_flow_distances AS
    		SELECT 
    		f.*,
    		NULL::NUMERIC AS updated_flow_manhattan_pts_network
    		FROM ", char_schema, ".flow_distances f
    		WHERE false;")
	cat(query, "\n")
	dbExecute(con, query)
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
		
		
		query <- paste0(
			"INSERT INTO ", char_schema, ".new_flow_distances ",
			"SELECT ",
			"    f.*, ",
			"    COALESCE(f.flow_manhattan_pts_network, 0) + ",
			"    COALESCE(o.flow_manhattan_pts_network, 0) + ",
			"    COALESCE(d.flow_manhattan_pts_network, 0) AS updated_flow_manhattan_pts_network ",
			"FROM ", char_schema, ".flow_distances f ",
			"LEFT JOIN ", char_schema, ".origin_nd o ",
			"    ON f.flow_id_i = o.flow_id_i AND f.flow_id_j = o.flow_id_j ",
			"LEFT JOIN ", char_schema, ".dest_nd d ",
			"    ON f.flow_id_i = d.flow_id_i AND f.flow_id_j = d.flow_id_j ",
			"WHERE f.flow_id_i BETWEEN ", id_start, " AND ", id_end, ";"
		)
		cat(query)
		dbExecute(local_con, query)
		
	}, mc.cores = cores)
	
	
	
	
	
	query <- paste0("DROP TABLE ", char_schema, ".flow_distances;")
	cat(query, "\n")
	dbExecute(con, query)
	
	query <- paste0("ALTER TABLE ", char_schema, ".new_flow_distances
  									RENAME TO flow_distances;")
	cat(query, "\n")
	dbExecute(con, query)
	
	
	query <- paste0("ALTER TABLE ", char_schema, ".flow_distances
  									DROP COLUMN flow_manhattan_pts_network;")
	cat(query, "\n")
	dbExecute(con, query)
	
	
	query <- paste0("ALTER TABLE ", char_schema, ".flow_distances
  									RENAME COLUMN updated_flow_manhattan_pts_network TO 
  									flow_manhattan_pts_network;")
	cat(query, "\n")
	dbExecute(con, query)
	
	
	t2 <- proc.time()
	diff_time <- t2-t1
	diff_time <- diff_time[3] %>% as.numeric
	cat("Time for adding OD_pts distances ", diff_time, "\n")
}



