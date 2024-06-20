library(sf)
library(ggplot2)


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

# Funktion zur Erzeugung von Punkten innerhalb der Ellipse
sample_points_in_ellipse <- function(ellipse_sf, n) {
	points <- st_sample(ellipse_sf, size = n, type = "random")
	return(points)
}

# Funktion zum Zeichnen von Linien von diesen Punkten
generate_lines_from_points <- function(points, base_length_flows, variance_length_flows, variance_angles) {
	lines <- vector("list", length(points))
	
	# Basiswinkel zufällig wählen, dann geringe Variation für jeden Punkt
	base_angle <- runif(1, 0, 2 * pi)
	
	for (i in seq_along(points)) {
		angle <- base_angle + rnorm(1, sd = variance_angles)  # Winkelvariation
		length <- base_length_flows + rnorm(1, sd = variance_length_flows)  # Längenvariation
		end_point <- st_coordinates(points[i]) + c(length * cos(angle), length * sin(angle))
		lines[[i]] <- st_sfc(st_linestring(matrix(c(st_coordinates(points[i]), end_point), ncol = 2, byrow = TRUE)), crs = st_crs(points))
	}
	sf_lines <- do.call(rbind, lines) %>% st_sfc
	sf_lines <- st_set_crs(sf_lines, 32632)
	return(sf_lines)
}

generate_clusters_sf <- function(num_clusters, bbox, crs_proj) {
	all_ellipses <- list()
	all_points <- list()
	all_lines <- list()
	
	for (i in 1:num_clusters) {
		# Zufällige Parameterwerte für jedes Cluster
		width_m <- sample(500:1500, 1)
		height_m <- sample(250:750, 1)
		n_points <- sample(4:45, 1)
		base_length_flows <- sample(1000:2000, 1)
		variance_length_flows <- runif(1, 10, 50)
		variance_angles <- runif(1, 0, 0.3)
		
		# Erzeuge die Ellipse und sample Punkte
		sf_ellipse <- create_ellipse(bbox, width_m, height_m, crs_proj)
		sf_points <- sample_points_in_ellipse(sf_ellipse, n_points)
		sf_lines <- generate_lines_from_points(sf_points, base_length_flows, variance_length_flows, variance_angles)
		
		# Sammle Geometrien in Listen
		all_ellipses[[i]] <- sf_ellipse
		all_points[[i]] <- sf_points
		all_lines[[i]] <- sf_lines
	}
	
	# Kombiniere alle gesammelten Geometrien
	combined_ellipses <- do.call(rbind, all_ellipses)
	combined_points <- do.call(rbind, all_points)
	combined_lines <- do.call(rbind, all_lines)
	
	# Rückgabe als Liste von sf-Objekten
	return(list(ellipses = combined_ellipses, points = combined_points, lines = combined_lines))
}

crs_proj <- 32632
bbox <- st_bbox(c(xmin = 343914.7, ymin = 5632759, xmax = 370674.3, ymax = 5661475), crs = crs_proj)
cluster_plots <- generate_clusters_sf(30, bbox, crs_proj)

sf_clusters <- cluster_plots$lines %>% st_as_sfc()

plot(sf_clusters)

# sf_ellipse <- create_ellipse(bbox, 1000, 500, crs_proj)
# 
# sf_points <- sample_points_in_ellipse(sf_ellipse, 50) 
# sf_lines <- generate_lines_from_points(sf_points, 
# 																		base_length_flows = 1500, 
# 																		variance_length_flows = 0, 
# 																		variance_angles = 0.0)
# 
# ggplot() +
# 	geom_sf(data = st_as_sfc(bbox, crs = crs_proj), fill = "transparent", color = "black") +
# 	geom_sf(data = sf_ellipse, fill = "blue", alpha = 0.5) +
# 	geom_sf(data = sf_points, size = 0.1) +
# 	geom_sf(data = sf_lines)


# Variables:
# - ellipse shape (width_m, height_m)
# - number of sampled points in shape (n)
# - base length of flows (base_length_flows)
# - variance of flow length (variance_length_flows)
# - variance of angles (variance_angles)