source("./src/config.R")
source("./src/functions.R")
library(sfnetworks)
library(osmdata)
library(tidygraph)

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
	
	
	ellipse_polygon <- st_polygon(list(ellipse_points))
	sf_ellipse <- st_sfc(ellipse_polygon, crs = st_crs(sf_network))
	
	sf_roads <- st_intersection(sf_network, sf_ellipse)
	# Sometimes there are multilinestrings after 'st_intersection()'
	sf_roads <- st_cast(sf_roads, "LINESTRING")
	# Ensure that linestrings are connected so network distance between points is low
	sf_net_small <- as_sfnetwork(sf_roads, directed = FALSE)
	components <- tidygraph::to_components(sf_net_small)
	components_lengths <- lapply(components, function(i){length(i)})
	largest_comp <- which.max(unlist(components_lengths))
	sf_net_small <- components[[largest_comp]]

	sf_roads <- sf_net_small %>%
		activate(edges) %>%
		as_tibble()%>%
		st_as_sf()
	sf_points <- st_sample(sf_roads, size = n, type = "random")

	sf_points <- sf_points[(!st_is_empty(sf_points)) %>% which]
	sf_points <- st_cast(sf_points, "POINT")
	sf_points <- sf_points
	return(sf_points)
}



create_sorted_linestrings <- function(start_points, end_points) {
	linestrings <- mapply(function(start, end) {
		st_linestring(rbind(st_coordinates(start), st_coordinates(end)))
	}, st_geometry(start_points), st_geometry(end_points), SIMPLIFY = FALSE)
	
	st_sf(geometry = st_sfc(linestrings), crs = st_crs(start_points))
}


### Basic Configuration --------------------------------------------------------
sf_network <- st_read(con, "col_2po_4pgr")
sf_bbox <- st_convex_hull(st_union(sf_network))

create_clusters <- function(n_clusters, sf_network) {
	
	all_lines <- list()
	all_cluster_ids <- list()
	
	for (i in 1:n_clusters) {
	  ### Configuration for linestrings ----------------------------------------------
	  n_points <- sample(3:50, 1)
	  base_length_flows <- sample(500:2500, 1)
	  variance_length_flows <- runif(1, 10, 50)
	  variance_angles <- runif(1, 0, 0.4)
	  base_angle <- runif(1, 0, 2 * pi)
	  base_length_flows <- runif(1, 500, 5000)
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
	  
	  # Konvertierung des Winkels in Grad
	  angle_deg <- angle * 180 / pi
	  
	  # Sortierung nach X- oder Y-Koordinaten basierend auf dem Winkel
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
	print(nrow(combined_lines))
	print(length(cluster_id))
	matrix_final <- cbind(combined_lines, cluster_id)
	print(matrix_final)


	sf_final <- st_as_sf(data.frame(matrix_final), sf_column_name = "geometry")
	st_crs(sf_final) <- st_crs(sf_network)
	sf_final$cluster_id <- unlist(sf_final$cluster_id)
	return(sf_final)
}


sf_clusters <- create_clusters(10, sf_network)

ggplot()+
	geom_sf(data=sf_bbox)+
	geom_sf(data=sf_network)+
	# geom_sf(data=sf_points_origin, color="red")+
	# geom_sf(data=sf_points_dest, color ="blue")+
	# geom_sf(data=sf_linestrings, color="green")
	geom_sf(data=sf_clusters, color = "red")
