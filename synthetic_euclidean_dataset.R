library(sf)
library(ggplot2)
library(readr)

create_ellipse <- function(bbox, width_m, height_m, crs_proj) {

	sf_bbox <- st_as_sfc(bbox, crs = crs_proj)
	
	bbox_vals <- st_bbox(sf_bbox)
	xmin <- bbox_vals["xmin"]
	xmax <- bbox_vals["xmax"]
	ymin <- bbox_vals["ymin"]
	ymax <- bbox_vals["ymax"]
	
	# Get random point within bbox as center of ellipse
	center_x <- runif(1, xmin + width_m / 2, xmax - width_m / 2)
	center_y <- runif(1, ymin + height_m / 2, ymax - height_m / 2)
	
	# Selct number of points to describe the desired shape
	num_points <- 100
	# Create vector rep. 100 uniformly distributed angles between 0 and 2*pi (0 and 360 degrees)
	angles <- seq(0, 2 * pi, length.out = num_points)
	# Calc sin- and cos for each angle to get x- and y-coordinates of points on "Einheitskreis"
	unit_circle <- cbind(cos(angles), sin(angles))
	# Transform "Einheitskreis" into ellipse based on the given parameters
	ellipse_points <- unit_circle %*% diag(c(width_m / 2, height_m / 2))
	# Add random center to all points using sweep() which moves the ellipse
	ellipse_points <- sweep(ellipse_points, 2, c(center_x, center_y), "+")
	

	ellipse_polygon <- st_polygon(list(ellipse_points))
	ellipse_sf <- st_sfc(ellipse_polygon, crs = crs_proj)
	
	return(ellipse_sf)
}


sample_points_in_ellipse <- function(ellipse_sf, n) {
	points <- st_sample(ellipse_sf, size = n, type = "random")
	return(points)
}


generate_lines_from_points <- function(points, 
																			 base_length_flows, 
																			 variance_length_flows, 
																			 variance_angles) {
	lines <- vector("list", length(points))
	flow_ids <- vector("integer", length(points))
	
	# Select random base angle
	base_angle <- runif(1, 0, 2 * pi)
	
	
	# Create lines based on a...
	for (i in seq_along(points)) {
		# ...angle that slightly differs from the base angle based on "variance_angles"...
		angle <- base_angle + rnorm(1, sd = variance_angles)
		
		# and a length that slightly differs from the base length based on "variance_length_flows".
		length <- base_length_flows + runif(1, 
																				min = -variance_length_flows,
																				max = variance_length_flows)
		
		end_point <- st_coordinates(points[i]) + c(length * cos(angle), length * sin(angle))
		lines[[i]] <- st_sfc(st_linestring(matrix(c(st_coordinates(points[i]), end_point), ncol = 2, byrow = TRUE)), crs = st_crs(points))
		flow_ids[i] <- i
	}
	sf_lines <- do.call(rbind, lines) %>% st_sfc
	sf_lines <- st_set_crs(sf_lines, 32632)
	return(sf_lines)
}


generate_clusters_sf <- function(num_clusters, bbox, crs_proj) {
	all_ellipses <- list()
	all_points <- list()
	all_lines <- list()
	all_cluster_ids <- list()
	
	for (i in 1:num_clusters) {
		width_m <- sample(500:1500, 1)
		height_m <- sample(250:750, 1)
		n_points <- sample(3:50, 1)
		base_length_flows <- sample(500:2500, 1)
		variance_length_flows <- runif(1, 10, 50)
		variance_angles <- runif(1, 0, 0.4)
		
		sf_ellipse <- create_ellipse(bbox, width_m, height_m, crs_proj)
		sf_points <- sample_points_in_ellipse(sf_ellipse, n_points)
		sf_lines <- generate_lines_from_points(sf_points, base_length_flows, variance_length_flows, variance_angles)
	
		all_lines[[i]] <- sf_lines
		all_cluster_ids[[i]] <- rep(i, n_points)
	}
	
	combined_lines <- do.call(rbind, all_lines)
	combined_cluster_ids <- unlist(all_cluster_ids) 

	matrix_final <- cbind(combined_lines, combined_cluster_ids)

	colnames(matrix_final) <- c("geometry", "cluster_id")
	sf_final <- st_as_sf(data.frame(matrix_final), sf_column_name = "geometry")
	sf_final <- st_set_crs(sf_final, 32632)
	sf_final$cluster_id <- unlist(sf_final$cluster_id)
	return(sf_final)
}

generate_noise_points <- function(bbox, n) {

	x_coords <- runif(n, min = bbox["xmin"], max = bbox["xmax"])
	y_coords <- runif(n, min = bbox["ymin"], max = bbox["ymax"])
	points <- cbind(x_coords, y_coords)
	
	points_sf <- st_as_sf(data.frame(x = x_coords, y = y_coords), 
												coords = c("x", "y"), crs = st_crs(bbox))
	return(points_sf)
}


generate_noise_lines <- function(points, min_length, max_length) {
	lines <- vector("list", nrow(points))
	
	for (i in 1:nrow(points)) {
		angle <- runif(1, 0, 2 * pi)  
		length <- runif(1, min_length, max_length)
		start_point <- st_coordinates(points[i,])
		end_point <- c(start_point[1] + length * cos(angle), start_point[2] + length * sin(angle))
		
		lines[[i]] <- st_sfc(st_linestring(matrix(c(start_point, end_point), ncol = 2, byrow = TRUE)), crs = st_crs(points))
	}
	sf_lines <- do.call(rbind, lines) %>% st_sfc
	sf_lines <- st_sf(geometry = sf_lines, cluster_id = rep(0, length(lines)))
	sf_lines <- st_set_crs(sf_lines, st_crs(points))
	return(sf_lines)
}


crs_proj <- 32632

xmin <- 343914.7
xmax <- 370674.3
ymin <- 5632759
ymax <- 5661475

x_center <- (xmin + xmax) / 2
y_center <- (ymin + ymax) / 2

width_reduction <- (xmax - xmin) * 0.15
height_reduction <- (ymax - ymin) * 0.15

new_xmin <- x_center - width_reduction
new_xmax <- x_center + width_reduction
new_ymin <- y_center - height_reduction
new_ymax <- y_center + height_reduction

bbox <- st_bbox(c(xmin = new_xmin, 
									ymin = new_ymin, 
									xmax = new_xmax, 
									ymax = new_ymax), crs = crs_proj)
n_clusters <- 12
sf_synth_clusters <- generate_clusters_sf(n_clusters, bbox, crs_proj)

n_noise_points <- 2000
min_length <- 500    
max_length <- 2000


noise_points <- generate_noise_points(bbox, n_noise_points)

noise_lines <- generate_noise_lines(noise_points, min_length, max_length)


sf_synth_clusters <- rbind(sf_synth_clusters, noise_lines)
sf_synth_clusters$flow_id <- 1:nrow(sf_synth_clusters) 

colors <- c(rgb(211/255, 211/255, 211/255, 0.5), rainbow(n_clusters))
names(colors) <- c("0", as.character(1:n_clusters))


ggplot(data = sf_synth_clusters) +
	geom_sf(aes(color = as.character(cluster_id)), size = 1) +
	scale_color_manual(values = colors) +
	theme_minimal() +
	labs(color = "Cluster ID")

filename <- paste0(n_clusters, "_", n_noise_points, ".rds")
write_rds(sf_synth_clusters, here::here("./data/synthetic/", filename))
