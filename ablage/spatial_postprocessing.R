Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

char_city <- "dd"
char_prefix_data <- "sr"
char_data <- paste0(char_prefix_data, "_", char_city)

sf_cluster_nd_pred <- st_read(con, paste0(char_data, "_cluster"))
# sf_cluster_nd_pred <- st_transform(sf_cluster_nd_pred, crs = 4326)
st_geometry(sf_cluster_nd_pred)

sf_cluster_nd_pred <- sf_cluster_nd_pred %>%
	filter(cluster_pred != 0)

sf_cluster_nd_pred <- sf_cluster_nd_pred %>%
	mutate(
		start_point = st_coordinates(lwgeom::st_startpoint(line_geom)),
		end_point = st_coordinates(lwgeom::st_endpoint(line_geom)),
		start_lat = start_point[, "Y"],
		start_lon = start_point[, "X"],
		end_lat = end_point[, "Y"],
		end_lon = end_point[, "X"]
	) %>%
	mutate(start_point = NULL, end_point = NULL)

# Custom bandwidth function
get_bandwidth <- function(x) {
	h <- bw.nrd0(x)
	if (h <= 0) {
		h <- 1e-6  # Set a very small positive bandwidth if it's non-positive
	}
	return(h)
}

# Function to generate the representative line for each cluster
get_rep_line <- function(df) {
	# Calculate custom bandwidths
	bw_x_start <- get_bandwidth(df$start_lat)
	bw_y_start <- get_bandwidth(df$start_lon)
	bw_x_end <- get_bandwidth(df$end_lat)
	bw_y_end <- get_bandwidth(df$end_lon)
	
	# KDE for start points
	kde_res_start <- kde2d(df$start_lat, df$start_lon, n = 50, h = c(bw_x_start, bw_y_start))
	kde_max_idx_start <- which(kde_res_start$z == max(kde_res_start$z), arr.ind = TRUE)
	kde_max_x_start <- kde_res_start$x[kde_max_idx_start[1]]
	kde_max_y_start <- kde_res_start$y[kde_max_idx_start[2]]
	
	# KDE for end points
	kde_res_end <- kde2d(df$end_lat, df$end_lon, n = 50, h = c(bw_x_end, bw_y_end))
	kde_max_idx_end <- which(kde_res_end$z == max(kde_res_end$z), arr.ind = TRUE)
	kde_max_x_end <- kde_res_end$x[kde_max_idx_end[1]]
	kde_max_y_end <- kde_res_end$y[kde_max_idx_end[2]]
	
	# Create line geometry
	coords <- matrix(c(kde_max_y_start, kde_max_x_start, 
										 kde_max_y_end, kde_max_x_end), 
									 ncol = 2, byrow = TRUE)
	
	line_geom <- st_linestring(coords)
	sf_line <- st_sfc(line_geom, crs = 32632)
	sf_line <- st_sf(geometry = sf_line)
	

	sf_line$count <- nrow(df)
	
	return(sf_line)
}

# Apply to all clusters and combine the results
final_sf <- sf_cluster_nd_pred %>%
	group_by(cluster_pred) %>%
	do(get_rep_line(.)) %>%
	ungroup() %>%
	st_as_sf()


ggplot() +
	geom_sf(data = 97) 
st_write(final_sf, con, paste0(char_data, "_rep_ls"))

