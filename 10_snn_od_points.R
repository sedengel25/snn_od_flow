Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")
################################################################################
# Input files
################################################################################
availabe_cluster_tables <- psql1_get_cluster_tables(con)
print(availabe_cluster_tables)
char_data <- availabe_cluster_tables[1, "table_name"]


available_networks <- psql1_get_available_networks(con)
print(available_networks)
char_network <- available_networks[1, "table_name"]
dt_network <- st_read(con, char_network) %>%
	as.data.table()
sf_network <- st_as_sf(dt_network) %>%
	mutate(m = 1000*km)



char_path_dt_dist_mat <- here::here("data", "input", "dt_dist_mat")
char_av_dt_dist_mat_files <- list.files(char_path_dt_dist_mat)
print(char_av_dt_dist_mat_files)
# stop("Have you chosen the right dist mat?")
char_dt_dist_mat <-  char_av_dt_dist_mat_files[13]
char_buffer <- "2000"
dt_dist_mat <- read_rds(here::here(
	char_path_dt_dist_mat,
	char_dt_dist_mat))

################################################################################
# Clean points of OD-flow clusters
################################################################################
sf_old_cluster_points <- st_read(con, char_data)
sf_old_cluster_points <- sf_old_cluster_points %>%
	filter(cluster_pred!=0)
sf_old_cluster_points_origin <- sf_old_cluster_points %>%
	mutate(origin = lwgeom::st_startpoint(line_geom))
st_geometry(sf_old_cluster_points_origin) <- "origin"
sf_old_cluster_points_origin <- sf_old_cluster_points_origin %>%
	select(cluster_pred, origin, origin_id, o_dist_to_start, o_dist_to_end)


sf_old_cluster_points_dest <- sf_old_cluster_points %>%
	mutate(dest = lwgeom::st_endpoint(line_geom))
st_geometry(sf_old_cluster_points_dest) <- "dest"
sf_old_cluster_points_dest <- sf_old_cluster_points_dest %>%
	select(cluster_pred, dest, dest_id, d_dist_to_start, d_dist_to_end)


sf_old_cluster_points_origin_or <- clean_point_clusters_lof(sf_cluster = 
																															sf_old_cluster_points_origin)
sf_old_cluster_points_origin_or <- sf_old_cluster_points_origin_or %>%
	rename(id_edge = origin_id,
				 dist_to_start = o_dist_to_start,
				 dist_to_end = o_dist_to_end) %>%
	as.data.frame() %>%
	rename(points = origin) %>%
	st_as_sf()


sf_old_cluster_points_dest_or <- clean_point_clusters_lof(sf_cluster = 
																														sf_old_cluster_points_dest)
sf_old_cluster_points_dest_or <- sf_old_cluster_points_dest_or %>%
	rename(id_edge = dest_id,
				 dist_to_start = d_dist_to_start,
				 dist_to_end = d_dist_to_end) %>%
	as.data.frame() %>%
	rename(points = dest) %>%
	st_as_sf()


sf_old_cluster_points_cleaned <- rbind(sf_old_cluster_points_origin_or, 
																			 sf_old_cluster_points_dest_or)
sf_old_cluster_points_cleaned$id <- 1:nrow(sf_old_cluster_points_cleaned)
sf_old_cluster_points_cleaned <- sf_old_cluster_points_cleaned %>%
	rename(cluster_od = cluster_pred)


################################################################################
# Map points to network
################################################################################
# int_crs <- 32632
# char_data <- paste0(char_data_cluster, "_mapped")
# st_write(sf_points, con, char_data, delete_layer = TRUE)Rcpp::sourceCpp("./src/helper_functions.cpp")
# 
# 
# psql1_create_spatial_index(con, char_data)
# psql1_create_spatial_index(con, char_network)
# 
# psql1_map_points_onto_network(con,
# 																 char_network,
# 																 char_data,
# 																 crs = int_crs)
# 
# available_mapped_trip_data <- psql1_get_mapped_trip_data(con)
# print(available_mapped_trip_data)
# char_point_data <- available_mapped_trip_data[17, "table_name"]
# sf_points <- st_read(con, char_point_data)

################################################################################
# Algo
################################################################################
dt_pts_nd <- parallel_process_networks(sf_old_cluster_points_cleaned,
																			 dt_network,
																			 dt_dist_mat,
																			 int_cores) %>%
	as.data.table()


gc()
dt_sym <- copy(dt_pts_nd)
dt_sym <- dt_sym[, .(from = to, to = from, distance)]
dt_pts_nd_sym <- rbind(dt_pts_nd, dt_sym)
rm(dt_pts_nd)
rm(dt_sym)
gc()

int_k <- 40
int_eps <- 20
int_minpts <- 22

dt_snn_pred_nd <- snn_flow(ids = sf_old_cluster_points_cleaned$id,
													 k = int_k,
													 eps = int_eps,
													 minpts = int_minpts,
													 dt_flow_distance = dt_pts_nd_sym)


table(dt_snn_pred_nd$cluster_pred)


sf_new_cluster_points <- sf_old_cluster_points_cleaned %>%
	left_join(dt_snn_pred_nd, by = c("id" = "id"))
char_data
char_new_cluster_points <- paste0(
	char_data, "_p2_", int_k, "_", int_eps, "_", int_minpts, "_points")
st_write(sf_new_cluster_points, con, char_new_cluster_points, delete_layer = TRUE)
