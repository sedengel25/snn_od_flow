source("./src/config.R")
#source("./src/functions.R")
library(sfnetworks)
library(osmdata)
library(tidygraph)


# Documentation: extract_linestrings
# Usage: extract_linestrings(geometry)
# Description: Is applied to each row of a sfc-object. If it contains a MLS,
	# each element of the MLS is extracted and converted into a LS
# Args/Options: geometry
# Returns: list
# Output: ...
# Action: ...
extract_linestrings <- function(geometry) {
	if (st_is_empty(geometry)) {
		return(NULL)  # Return NULL for NA geometries
	} else if (st_geometry_type(geometry) == "MULTILINESTRING") {
		# Extract each LINESTRING manually from MULTILINESTRING
		return(lapply(1:length(geometry), function(i) st_linestring(geometry[[i]])))
	} else {
		return(list(geometry))  # Return the geometry if it is not a MULTILINESTRING
	}
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
	# Apply 'extract_lienstrings()' since some segments are multilinestrings
	list_sf_linestrings <- do.call(c, lapply(sf_roads$geom_way, extract_linestrings))
	sf_roads_cast <- st_sfc(list_sf_linestrings, crs = st_crs(sf_roads)) %>% st_as_sf()
	# Ensure that linestrings are connected so network distance between points is low
	sf_net_small <- as_sfnetwork(sf_roads_cast, directed = FALSE)
	# Get the components of the created networf
	components <- tidygraph::to_components(sf_net_small)
	components_lengths <- lapply(components, function(i){length(i)})
	# Choose the largest component...
	largest_comp <- which.max(unlist(components_lengths))
	sf_net_small <- components[[largest_comp]]

	#...and use the corresponding road segments...
	sf_roads_cast <- sf_net_small %>%
		activate(edges) %>%
		as_tibble()%>%
		st_as_sf()
	
	print(sf_roads_cast)
	#...to sample points
	sf_points <- st_sample(sf_roads_cast, size = n, type = "random")
	print(sf_points)
	sf_points <- sf_points[(!st_is_empty(sf_points)) %>% which]
	sf_points <- st_cast(sf_points, "POINT")
	sf_points <- sf_points
	return(sf_points)
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

# Documentation: create_noise_flows
# Usage: create_noise_flows(n)
# Description: Maps n*2 points onto the road network and randomly combines the points
 # pairwise to linestrings
# Args/Options: n
# Returns: sf-dataframe
# Output: ...
# Action: ...
# create_noise_flows <- function(n) {
# 	sf_points_on_line <- generate_noise_points(n = n*2,
# 																						 sf_bbox = sf_bbox,
# 																						 sf_network = sf_network)
# 	
# 	index_pairs <- seq(1, length(sf_points_on_line) - 1, by = 2)
# 	sf_linestrings <- lapply(index_pairs, function(i) {
# 		st_sfc(st_linestring(st_coordinates(sf_points_on_line[c(i, i + 1), ])))
# 	})
# 	
# 	sf_linestrings_sfc <- do.call(c, sf_linestrings)
# 	
# 	
# 	sf_linestrings <- st_sf(geometry = sf_linestrings_sfc)
# 	
# 	st_crs(sf_linestrings) <- st_crs(sf_points_on_line)
# 	
# 	return(sf_linestrings)
# }

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
		start_point <- st_coordinates(sf_startpoints[i,])
		end_point <- c(start_point[1] + length * cos(angle), start_point[2] + length * sin(angle))
		
		lines[[i]] <- st_sfc(st_linestring(matrix(c(start_point, end_point), ncol = 2, byrow = TRUE)), crs = st_crs(sf_startpoints))
	}
	sf_lines <- do.call(rbind, lines) %>% st_sfc
	sf_lines <- st_sf(geometry = sf_lines, cluster_id = rep(0, length(lines)))
	sf_lines <- st_set_crs(sf_lines, st_crs(sf_startpoints))
	# Extract endpoints
	sf_endpoints <- lwgeom::st_endpoint(sf_lines)
	st_crs(sf_endpoints) <- st_crs(sf_startpoints)
	int_nearest_features <- st_nearest_feature(sf_endpoints, sf_network)
	sf_nearest_lines <- sf_network[int_nearest_features, ]
	

	list_nearest_points <- lapply(1:nrow(sf_startpoints), function(i){
		nearest_points <- st_nearest_points(sf_endpoints[i,], 
																				sf_nearest_lines[i,])%>% st_cast("POINT")
		nearest_points <- nearest_points[2] %>% st_sfc()
		
	})
	list_nearest_points <- sapply(list_nearest_points, st_sfc)
	
	sf_nearest_points <- do.call(st_sfc, list_nearest_points) 
	
	final_lines <- mapply(function(start, end) {
		end_coords <- st_coordinates(end)
		st_sfc(st_linestring(rbind(st_coordinates(start), end_coords)))
	}, start = sf_startpoints, end = sf_nearest_points, SIMPLIFY = FALSE)
	
	sf_final_lines <- do.call(rbind, final_lines) %>% st_sfc
	sf_final_lines <- st_sf(geometry = sf_final_lines)
	sf_final_lines <- st_set_crs(sf_final_lines, st_crs(sf_startpoints))
	
	
	return(sf_lines)
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
	all_cluster_ids <- list()
	
	for (i in 1:n_clusters) {
	  ### Configuration for linestrings ----------------------------------------------
	  n_points <- sample(3:50, 1)
	  base_length_flows <- sample(800:4000, 1)
	  variance_length_flows <- runif(1, 10, 50)
	  variance_angles <- runif(1, 0, 0.4)
	  base_angle <- runif(1, 0, 2 * pi)
	  length <- base_length_flows + runif(1,
	  																		min = -variance_length_flows,
	  																		max = variance_length_flows)
	  
	  ### Configuration for sample of origin points ----------------------------------
	  width_m <- sample(500:4000, 1)
	  height_m <- sample(250:1000, 1)
	  
	  # Get random road
	  sf_sampled_road <- sample_n(sf_network, 1)
	  sf_center_point <-  st_coordinates(st_sample(sf_sampled_road, 1)) %>% as.numeric
	  
	  # Sample origin points
	  sf_points_origin <- create_ellipse_with_points(sf_network = sf_network,
	  																				sf_center_point = sf_center_point,
	  																				width_m = width_m,
	  																				height_m = height_m,
	  																				n = n_points)
	  # Get one of the origin points...
	  sf_startpoint <- sample(sf_points_origin, 1)
	  
	  ### Configuration for sample of destination points -----------------------------
	  width_m <- sample(500:3000, 1)
	  height_m <- sample(250:1000, 1)
	  
	  # ...and use it as startpoint for the destination points
	  sf_endpoint <- sf_startpoint + c(length * cos(base_angle), 
	  																								length * sin(base_angle))
	  
	  
	  
	  st_crs(sf_endpoint) <- st_crs(sf_network)
	  
	  # Map destination point one road network
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
	  
	  
	  ### Draw linestrings with little intersection between linestrings---------------
	  sf_points_origin <- sf_points_origin %>% st_as_sf()
	  sf_points_dest <- sf_points_dest %>% st_as_sf()
	  
	  center_origin <- st_centroid(st_union(sf_points_origin))
	  center_destination <- st_centroid(st_union(sf_points_dest))
	  
	  # Get angle between center-points to get a proxy of the direction
	  angle <- atan2(st_coordinates(center_destination)[2] - st_coordinates(center_origin)[2],
	  							 st_coordinates(center_destination)[1] - st_coordinates(center_origin)[1])
	  
	  # Convert angle in degrees
	  angle_deg <- angle * 180 / pi
	  
	  if (abs(angle_deg) <= 45 || abs(angle_deg) > 135) {
	  	# Sort x-coordinates
	  	sf_points_origin <- sf_points_origin[order(st_coordinates(sf_points_origin)[, 1]), ]
	  	sf_points_dest <- sf_points_dest[order(st_coordinates(sf_points_dest)[, 1]), ]
	  } else {
	  	# Sort y-coordinates
	  	sf_points_origin <- sf_points_origin[order(st_coordinates(sf_points_origin)[, 2]), ]
	  	sf_points_dest <- sf_points_dest[order(st_coordinates(sf_points_dest)[, 2]), ]
	  }
	  
	  sf_linestrings <- create_sorted_linestrings(sf_points_origin, sf_points_dest)
	  all_lines[[i]] <- sf_linestrings
	  all_cluster_ids[[i]] <- rep(i, n_points)
	}
	
	combined_lines <- do.call(rbind, all_lines)
	cluster_id <- unlist(all_cluster_ids) 
	matrix_final <- cbind(combined_lines, cluster_id)


	sf_clusters <- st_as_sf(data.frame(matrix_final), sf_column_name = "geometry")
	st_crs(sf_clusters) <- st_crs(sf_network)
	sf_clusters$cluster_id <- unlist(sf_clusters$cluster_id)
	


	sf_startpoints <- generate_noise_points(n = n_noiseflows)
	sf_startpoints <- sf_startpoints %>% st_as_sf()
	st_crs(sf_startpoints) <- st_crs(sf_network)
	sf_noiseflows <- generate_noise_lines(sf_startpoints,
																				min_length = 100,
																				max_length = 6000)
	sf_noiseflows <- sf_noiseflows %>%
		mutate(cluster_id = 0) %>%
		select(cluster_id, geometry)
	st_crs(sf_noiseflows) <- st_crs(sf_network)
	sf_final <- rbind(sf_noiseflows, sf_clusters)
	sf_final$flow_id <- 1:nrow(sf_final)
	return(sf_final)
}

################ MAPPING NEEDS IDS OF ROAD SEGMENTS IN NETWORK!!!
### Basic Configuration --------------------------------------------------------
sf_network <- st_read(con, "col_2po_4pgr")
sf_bbox <- st_convex_hull(st_union(sf_network))


n_clusters <- 12
n_noiseflows <- 3000

sf_clusters <- create_synth_dataset(n_clusters = n_clusters, 
															 n_noiseflows = n_noiseflows)

sf_clusters


summary(sf_clusters)
colors <- c(rgb(211/255, 211/255, 211/255, 0.5), rainbow(n_clusters))
names(colors) <- c("0", as.character(1:n_clusters))

ggplot(data = sf_clusters) +
	geom_sf(aes(color = as.character(cluster_id)), size = 1) +
	scale_color_manual(values = colors) +
	theme_minimal() +
	labs(color = "Cluster ID")



write_rds(sf_clusters, paste0("./data/synthetic/network_distance/",
															n_clusters,
															"_",
															n_noiseflows,
															".rds"))

st_write(sf_clusters, con, paste0("col_synth_", 	n_clusters,
																	"_",
																	n_noiseflows))

sf_clusters %>%
	mutate(dist=st_length(sf_clusters$geometry) %>% as.numeric) %>% 
	filter(cluster_id==0) %>%
	summary
