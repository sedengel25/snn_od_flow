source("./src/config.R")
source("./main_functions.R")




# Documentation: get_line_ids_of_mapped_points
# Usage: get_line_ids_of_mapped_points(sf_points, sf_roads)
# Description: Joins buffered points with 'sf_roads' and extracts id, source and target
# Args/Options: sf_points, sf_roads
# Returns: sf-dataframe
# Output: ...
# Action: ...
get_line_ids_of_mapped_points <- function(sf_points, sf_roads) {
	# Create small buffers around the points...
	sf_points_buffered <- st_buffer(sf_points, dist = 1e-7)  # Eine sehr kleine Puffergröße, z.B. 0.1 Meter, je nach Ihren Einheiten
	#...to get the linestrings and its attributes...
	sf_points_with_ids <- st_join(sf_points_buffered, sf_roads, join = st_intersects)
	# ...and then get the centroids of the buffer's polygons

	sf_centroids <- st_centroid(sf_points_with_ids)
	sf_centroids <- sf_centroids %>%
		select(point_id, id, source, target, x) %>%
		rename("geom" = "x")
	


	sf_centroids <- sf_centroids %>%
		group_by(point_id) %>%  
		slice(1) %>%               
		ungroup()
	
	return(sf_centroids)
}


# Documentation: create_ellipse_with_points
# Usage: create_ellipse_with_points(sf_network, sf_center_points, width_m,
	# height_m, n)
# Description: 
	# 1. Gets a random road segment
	# 2. Draws an ellipse around the center-point of that road segment
	# 3. Gets all road segments within that ellipse
	# 4. Converts sf road segments into a sfnetwork to keep the ones that
			# form the largest connected network
	# 5. Samples n points on the corresponding road segments 
# Args/Options: sf_network, sf_center_points, width_m, height_m, n
# Returns: sf-dataframe
# Output: ...
# Action: ...
create_ellipse_with_points <- function(sf_network, 
																			 sf_center_point, 
																			 width_m, 
																			 height_m, 
																			 n) {


	# Draw the ellipse with 'num_points'
	num_points <- 100
	angles <- seq(0, 2 * pi, length.out = num_points)
	unit_circle <- cbind(cos(angles), sin(angles))
	ellipse_points <- unit_circle %*% diag(c(width_m / 2, height_m / 2))
	ellipse_points <- sweep(ellipse_points, 2, c(sf_center_point[1], 
																							 sf_center_point[2]), "+")
	
	# Get sf polygon representing the ellipse
	ellipse_polygon <- st_polygon(list(ellipse_points))
	sf_ellipse <- st_sfc(ellipse_polygon, crs = st_crs(sf_network))
	
	# Get road segments lying within the ellipse
	sf_roads <- st_intersection(sf_network, sf_ellipse)

	
	# After performing the intersection, geometries such as MULTILINESTRING are removed,...
	#...which is totally fine since there are only few cases where MULTILINESTRINGS...
	#...
	sf_roads <- sf_roads %>%
		filter(st_geometry_type(geom_way) == "LINESTRING")
	
	# Ensure that linestrings are connected so network distance between points is low
	sf_net_small <- as_sfnetwork(sf_roads, directed = FALSE)
	# Get the components of the created networf
	components <- tidygraph::to_components(sf_net_small)
	components_lengths <- lapply(components, function(i){length(i)})
	# Choose the largest component...
	largest_comp <- which.max(unlist(components_lengths))
	sf_net_small <- components[[largest_comp]]

	#...and use the corresponding road segments...
	sf_roads <- sf_net_small %>%
		activate(edges) %>%
		as.data.frame() %>%
		st_as_sf()
	st_crs(sf_roads) <- st_crs(sf_network)

	#...to sample points
	sfc_points <- st_sample(sf_roads, size = n, type = "random")
	sfc_points <- sfc_points[(!st_is_empty(sfc_points)) %>% which]
	sfc_points <- st_cast(sfc_points, "POINT") 
	st_crs(sfc_points) <- st_crs(sfc_points)
	sf_points <- sfc_points %>% st_as_sf()
	sf_points$point_id <- 1:nrow(sf_points)
	sf_centroids <- get_line_ids_of_mapped_points(sf_points, sf_roads)

	return(sf_centroids)
}


# Documentation: create_sorted_linestrings
# Usage: create_sorted_linestrings(start_points, end_points)
# Description: Combines origin- and destination points into sf linestrings
# Args/Options: start_points, end_points
# Returns: sf-dataframe
# Output: ...
# Action: ...
create_sorted_linestrings <- function(start_points, end_points) {
	linestrings <- mapply(function(start, end) {
		st_linestring(rbind(st_coordinates(start), st_coordinates(end)))
	}, st_geometry(start_points), st_geometry(end_points), SIMPLIFY = FALSE)
	
	st_sf(geometry = st_sfc(linestrings), crs = st_crs(start_points))
}

# Documentation: generate_noise_points
# Usage: generate_noise_points(n)
# Description: Generates n poitns within a bounding box and maps them onto the
 # corresponding road network
# Args/Options: n
# Returns: sf-dataframe
# Output: ...
# Action: ...
generate_noise_points <- function(n) {
	
	sf_points <- st_sample(sf_bbox, size = n, type ="random")
	int_nearest_features <- st_nearest_feature(sf_points, sf_network)
	sf_nearest_lines <- sf_network[int_nearest_features, ]
	
	list_nearest_points <- lapply(1:n, function(i){
		nearest_points <- st_nearest_points(sf_points[i,], 
																				sf_nearest_lines[i,])%>% st_cast("POINT")
		nearest_points <- nearest_points[2] %>% st_sfc()

	})
	list_nearest_points <- sapply(list_nearest_points, st_sfc)

	sf_nearest_points <- do.call(st_sfc, list_nearest_points) 
	
	return(sf_nearest_points)
}


# Documentation: generate_noise_lines
# Usage: generate_noise_lines(n)
# Description: Draws lines from the points, extracts the endpoints, maps the 
	# endpoints to the network
# pairwise to linestrings
# Args/Options: n
# Returns: sf-dataframe
# Output: ...
# Action: ...
generate_noise_lines <- function(sf_startpoints, min_length, max_length) {
	lines <- vector("list", nrow(sf_startpoints))
	
	# Draw lines
	for (i in 1:nrow(sf_startpoints)) {
		angle <- runif(1, 0, 2 * pi)  
		length <- runif(1, min_length, max_length)
		start_point <- st_coordinates(sf_startpoints[i,"origin_geom"])
		end_point <- c(start_point[1] + length * cos(angle), 
									 start_point[2] + length * sin(angle))
		
		lines[[i]] <- st_sfc(st_linestring(matrix(c(start_point, end_point), 
																							ncol = 2, byrow = TRUE)), 
												 crs = st_crs(sf_startpoints))
	}
	sfc_lines <- do.call(rbind, lines) %>% st_sfc
	sf_lines <- st_sf(geometry = sfc_lines, cluster_id = rep(0, length(lines)))
	sf_lines <- st_set_crs(sf_lines, st_crs(sf_startpoints))
	# Extract endpoints...
	sf_endpoints <- lwgeom::st_endpoint(sf_lines)
	st_crs(sf_endpoints) <- st_crs(sf_startpoints)
	int_nearest_features <- st_nearest_feature(sf_endpoints, sf_network)
	sf_nearest_lines <- sf_network[int_nearest_features, ]
	
	#...and map them onto the network
	list_nearest_points <- lapply(1:nrow(sf_startpoints), function(i){
		nearest_points <- st_nearest_points(sf_endpoints[i,], 
																				sf_nearest_lines[i,])%>% st_cast("POINT")
		nearest_points <- nearest_points[2] %>% st_sfc()
		
	})
	list_nearest_points <- sapply(list_nearest_points, st_sfc)
	
	sf_nearest_points <- do.call(st_sfc, list_nearest_points) 
	sf_nearest_points <- sf_nearest_points %>% st_as_sf()
	st_crs(sf_nearest_points) <- st_crs(sf_network)
	sf_nearest_points$point_id <- 1:nrow(sf_nearest_points)
	# Extract atrtibutes of the LINESTRINGS on which endpoitns where mapped
	sf_nearest_points <- get_line_ids_of_mapped_points(sf_nearest_points, sf_network) %>%
		st_as_sf()
	sf_points_dest_noise <- sf_nearest_points %>%
		rename("dest_point_id" = "point_id",
					 "dest_id" = "id",
					 "dest_source" = "source",
					 "dest_target"="target",
					 "dest_geom"="geom")

	# Combine mapped origin- and destination points into LINESTRINGS
	final_lines <- mapply(function(start, end) {
		end_coords <- st_coordinates(end)
		st_sfc(st_linestring(rbind(st_coordinates(start), end_coords)))
	}, start = sf_startpoints$origin_geom, end = sf_points_dest_noise$dest_geom, SIMPLIFY = FALSE)

	sf_final_lines <- do.call(rbind, final_lines) %>% st_sfc
	sf_final_noise <- cbind(sf_final_lines, 
													sf_startpoints, 
													sf_points_dest_noise)
	sf_final_noise <- st_sf(geometry = sf_final_noise)
	sf_final_noise <- st_set_crs(sf_final_noise, st_crs(sf_startpoints))
	
	return(sf_final_noise)
}


# Documentation: create_clusters
# Usage: create_clusters(n_clusters, n_noiseflows)
# Description: Main function that creates n clusters on a given road network
# Args/Options: n_clusters, n_noiseflows
# Returns: sf-dataframe
# Output: ...
# Action: ...
create_synth_dataset <- function(n_clusters, n_noiseflows) {
	

	
	all_lines <- list()
	all_origin_pts <- list()
	all_dest_pts <- list()
	all_cluster_ids <- list()
	for (i in 1:n_clusters) {

	  # Configuration for LINESTRINGS
	  n_points <- sample(20:50, 1)
	  base_length_flows <- sample(800:4000, 1)
	  variance_length_flows <- runif(1, 10, 50)
	  variance_angles <- runif(1, 0, 0.4)
	  base_angle <- runif(1, 0, 2 * pi)
	  length <- base_length_flows + runif(1,
	  																		min = -variance_length_flows,
	  																		max = variance_length_flows)
	  
	  # Configuration for sample of origin points 
	  width_m <- sample(500:4000, 1)
	  height_m <- sample(250:1000, 1)
	  
	  # Get random road within the network
	  sf_sampled_road <- sample_n(sf_network, 1)
	  sf_center_point <-  st_coordinates(st_sample(sf_sampled_road, 1)) %>% as.numeric
	  
	  # Sample origin points
	  sf_points_origin <- create_ellipse_with_points(sf_network = sf_network,
	  																				sf_center_point = sf_center_point,
	  																				width_m = width_m,
	  																				height_m = height_m,
	  																				n = n_points)
	  
	  sf_points_origin <- sf_points_origin %>%
	  	rename("origin_point_id" = "point_id",
	  				 "origin_id" = "id",
	  				 "origin_source" = "source",
	  				 "origin_target"="target",
	  				 "origin_geom"="geom")

	  # Get one of the origin points...
	  sf_startpoint <- sample(sf_points_origin$origin_geom, 1)
	  
	  # ...and use it as startpoint for the determination of destination points
	  sf_endpoint <- sf_startpoint + c(length * cos(base_angle), 
	  																 length * sin(base_angle))
	  st_crs(sf_endpoint) <- st_crs(sf_network)
	  
	  # Configuration for sample of destination points 
	  width_m <- sample(500:3000, 1)
	  height_m <- sample(250:1000, 1)
	  
	  # Map destination point on road network 
	  int_nearest_feature <- st_nearest_feature(sf_endpoint, sf_network)
	  sf_nearest_line <- sf_network[int_nearest_feature, ]
	  sf_nearest_point <- st_nearest_points(sf_endpoint, sf_nearest_line) %>% 
	  	st_cast("POINT") %>% 
	  	.[2]
	  
	  # Sample destination points
	  sf_points_dest <- create_ellipse_with_points(sf_network = sf_network,
	  																							 sf_center_point = st_coordinates(
	  																							 	sf_nearest_point),
	  																							 width_m = width_m,
	  																							 height_m = height_m,
	  																							 n = n_points)  
	  sf_points_dest <- sf_points_dest %>%
	  	rename("dest_point_id" = "point_id",
	  				 "dest_id" = "id",
	  				 "dest_source" = "source",
	  				 "dest_target"="target",
	  				 "dest_geom"="geom")
	  
	  # Draw LINESTRINGS with little intersection between each other
	  sf_points_origin <- sf_points_origin %>% st_as_sf()
	  sf_points_dest <- sf_points_dest %>% st_as_sf()
	  
	  center_origin <- st_centroid(st_union(sf_points_origin))
	  center_destination <- st_centroid(st_union(sf_points_dest))
	  
	  # Get angle between center-points of origin- and destination points
	  #	to get a proxy of the direction
	  angle <- atan2(st_coordinates(center_destination)[2] - st_coordinates(center_origin)[2],
	  							 st_coordinates(center_destination)[1] - st_coordinates(center_origin)[1])
	  
	  # Convert angle in degrees
	  angle_deg <- angle * 180 / pi
	  
	  # Sort x- or y-coordinates depending on the angle between their center-points
	  if (abs(angle_deg) <= 45 || abs(angle_deg) > 135) {
	  	# Sort x-coordinates
	  	sf_points_origin <- sf_points_origin[order(st_coordinates(sf_points_origin)[, 1]), ]
	  	sf_points_dest <- sf_points_dest[order(st_coordinates(sf_points_dest)[, 1]), ]
	  } else {
	  	# Sort y-coordinates
	  	sf_points_origin <- sf_points_origin[order(st_coordinates(sf_points_origin)[, 2]), ]
	  	sf_points_dest <- sf_points_dest[order(st_coordinates(sf_points_dest)[, 2]), ]
	  }

	  # Combine sorted OD-points into LINESTRINGS
	  sf_linestrings <- create_sorted_linestrings(sf_points_origin, sf_points_dest)
	  all_lines[[i]] <- sf_linestrings
	  all_origin_pts[[i]] <- sf_points_origin
	  all_dest_pts[[i]] <- sf_points_dest
	  all_cluster_ids[[i]] <- rep(i, n_points)
	  cat("Cluster", i, "created\n")
	}
	
	# Combine LINESTRINGS, OD-points and cluster-ids into one matrix
	combined_lines <- do.call(rbind, all_lines)
	combined_origin_pts <- do.call(rbind, all_origin_pts)
	combined_dest_pts <- do.call(rbind, all_dest_pts)
	cluster_id <- unlist(all_cluster_ids) 
	matrix_final <- cbind(combined_lines, 
												cluster_id, 
												combined_origin_pts, 
												combined_dest_pts)


	sf_clusters <- st_as_sf(data.frame(matrix_final), sf_column_name = "geometry")
	st_crs(sf_clusters) <- st_crs(sf_network)
	sf_clusters$cluster_id <- unlist(sf_clusters$cluster_id)
	

	# Generate noise points
	sf_startpoints <- generate_noise_points(n = n_noiseflows)
	sf_startpoints <- sf_startpoints %>% st_as_sf()
	sf_startpoints$point_id <- 1:nrow(sf_startpoints)
	st_crs(sf_startpoints) <- st_crs(sf_network)
	
	# Map noise points
	sf_startpoints <- get_line_ids_of_mapped_points(sf_startpoints, sf_network) %>%
		st_as_sf()

	sf_points_origin_noise <- sf_startpoints %>%
		rename("origin_point_id" = "point_id",
					 "origin_id" = "id",
					 "origin_source" = "source",
					 "origin_target"="target",
					 "origin_geom"="geom")

	# Create noise flows
	sf_noiseflows <- generate_noise_lines(sf_points_origin_noise,
																				min_length = 100,
																				max_length = 6000)
	

	sf_noiseflows <- sf_noiseflows %>%
		mutate(cluster_id = 0) %>%
		select(cluster_id, 
					 origin_point_id,
					 origin_id,
					 origin_source,
					 origin_target,
					 dest_point_id,
					 dest_id,
					 dest_source,
					 dest_target,
					 geometry,
					 origin_geom,
					 dest_geom)
	st_crs(sf_noiseflows) <- st_crs(sf_network)

	sf_final <- rbind(sf_noiseflows[,-c(11,12)], 
										sf_clusters[,-c(11,12)])
	sf_final$flow_id <- 1:nrow(sf_final)
	return(sf_final)
}


################################################################################
# Main
################################################################################
psql1_get_available_networks(con)
sf_network <- st_read(con, "dd_complete_2po_4pgr")
sf_bbox <- st_convex_hull(st_union(sf_network))


################################################################################
# n_clusters_vals <- seq(10,50,5)
# 
# param_grid <- data.frame()
# 
# for (n_clusters in n_clusters_vals) {
# 	n_noiseflows_vals <- seq(1000,5000,250)
# 	for (n_noiseflows in n_noiseflows_vals) {
# 		param_grid <- rbind(param_grid, data.frame(n_clusters = n_clusters, 
# 																							 n_noiseflows = n_noiseflows))
# 	}
# }
n_clusters <- 300
n_noiseflows <- 10000
sf_clusters <- create_synth_dataset(n_clusters = n_clusters, 
																		n_noiseflows = n_noiseflows)

write_rds(sf_clusters, paste0("./data/synthetic/network_distance/",
															n_clusters,
															"_",
															n_noiseflows,
															".rds"))

# for(i in 1:nrow(param_grid)){
# 	n_clusters <- param_grid[i, "n_clusters"]
# 	n_noiseflows <- param_grid[i, "n_noiseflows"]
# 	sf_clusters <- create_synth_dataset(n_clusters = 100, 
# 																			n_noiseflows = 50000)
# 	
# 	write_rds(sf_clusters, paste0("./data/experiment/",
# 																100,
# 																"_",
# 																50000,
# 																".rds"))
# }




# colors <- c(rgb(211/255, 211/255, 211/255, 0.5), rainbow(n_clusters))
# names(colors) <- c("0", as.character(1:n_clusters))
# 
# ggplot(data = sf_clusters) +
# 	geom_sf(data=sf_network) +
# 	geom_sf(aes(color = as.character(cluster_id)), size = 1) +
# 	scale_color_manual(values = colors) +
# 	theme_minimal() +
# 	labs(color = "Cluster ID")






