Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")
################################################################################
# Trip data
################################################################################
availabe_cluster_tables <- psql1_get_cluster_tables(con)
print(availabe_cluster_tables)
char_data_cluster <- availabe_cluster_tables[1, "table_name"]

available_networks <- psql1_get_available_networks(con)
print(available_networks)
char_network <- available_networks[1, "table_name"]


char_path_dt_dist_mat <- here::here("data", "input", "dt_dist_mat")
char_av_dt_dist_mat_files <- list.files(char_path_dt_dist_mat)
print(char_av_dt_dist_mat_files)
# stop("Have you chosen the right dist mat?")
char_dt_dist_mat <-  char_av_dt_dist_mat_files[13]



sf_cluster <- st_read(con, char_data_cluster)
sf_cluster <- sf_cluster %>%
	filter(cluster_pred!=0)
sf_cluster_origin <- sf_cluster %>%
	mutate(origin = lwgeom::st_startpoint(line_geom))
st_geometry(sf_cluster_origin) <- "origin"
sf_cluster_origin <- sf_cluster_origin %>%
	select(cluster_pred, origin, origin_id, o_dist_to_start, o_dist_to_end)
st_write(sf_cluster_origin, con, "origin_points_from_flows", delete_layer = TRUE)


sf_cluster_dest <- sf_cluster %>%
	mutate(dest = lwgeom::st_endpoint(line_geom))
st_geometry(sf_cluster_dest) <- "dest"
sf_cluster_dest <- sf_cluster_dest %>%
	select(cluster_pred, dest, dest_id, d_dist_to_start, d_dist_to_end)

st_write(sf_cluster_dest, con, "dest_points_from_flows", delete_layer = TRUE)


sf_network <- st_read(con, char_network) 



sf_cluster_origin_or <- clean_point_clusters_lof(sf_cluster = sf_cluster_origin)
sf_cluster_origin_or <- sf_cluster_origin_or %>%
	rename(id_edge = origin_id,
				 dist_to_start = o_dist_to_start,
				 dist_to_end = o_dist_to_end) %>%
	as.data.frame() %>%
	rename(points = origin) %>%
	st_as_sf()


sf_cluster_dest_or <- clean_point_clusters_lof(sf_cluster = sf_cluster_dest)
sf_cluster_dest_or <- sf_cluster_dest_or %>%
	rename(id_edge = dest_id,
				 dist_to_start = d_dist_to_start,
				 dist_to_end = d_dist_to_end) %>%
	as.data.frame() %>%
	rename(points = dest) %>%
	st_as_sf()


sf_points <- rbind(sf_cluster_origin_or, sf_cluster_dest_or)
sf_points$id <- 1:nrow(sf_points)
sf_points <- sf_points %>%
	rename(cluster_od = cluster_pred) #%>%
	#select(cluster_od, id, points)


################################################################################
# Network data
################################################################################
# available_networks <- psql1_get_available_networks(con)
# print(available_networks)
# char_network <- available_networks[1, "table_name"]
dt_network <- st_read(con, char_network) %>%
	as.data.table()
sf_network <- st_as_sf(dt_network)
ggplot() +
	geom_sf(data=sf_network)


char_buffer <- "2000"
dt_dist_mat <- read_rds(here::here(
	char_path_dt_dist_mat,
	char_dt_dist_mat))

dt_dist_mat
################################################################################
# Map points to network
################################################################################
# int_crs <- 32632
# char_data <- paste0(char_data_cluster, "_mapped")
# st_write(sf_points, con, char_data, delete_layer = TRUE)
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
dt_pts_nd <- parallel_process_networks(sf_points, 
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

dt_snn_pred_nd <- snn_flow(ids = sf_points$id,
													 k = int_k,
													 eps = int_eps,
													 minpts = int_minpts,
													 dt_flow_distance = dt_pts_nd_sym)


table(dt_snn_pred_nd$cluster_pred)


sf_cluster_pred_points <- sf_points %>%
	left_join(dt_snn_pred_nd, by = c("id" = "id"))

char_data_2 <- paste0(
	char_data_cluster, "_p2_", int_k, "_", int_eps, "_", int_minpts)
st_write(sf_cluster_pred_points, con, char_data_2, delete_layer = TRUE)
################################################################################
# Turn point-clusters into street segments
################################################################################
# sf_cluster_pred_points <- st_read(con, "testtt2")
# setindex(dt_pts_nd_sym, from)
# 
# 
# cluster <- sf_cluster_pred_points %>%
# 	filter(cluster_pred == 26) %>%
# 	pull(id)
# 
# lapply(cluster, function(i){
# 	print(i)
# })
# dt_pts_nd_sym[from == 1728][which.min(distance)]


# sf_cluster_pred_pols <- sf_cluster_pred_points %>%
# 	group_by(cluster_pred) %>%
# 	summarize(do_union = FALSE) %>%
# 	as.data.frame() %>%
# 	st_as_sf() %>%
# 	mutate(pol = st_convex_hull(points)) %>%
# 	filter(cluster_pred!=0)
# 
# st_geometry(sf_cluster_pred_pols) <- "pol"
# sf_cluster_pred_points
# sf_network
# 
# cluster_15 <- sf_cluster_pred_points %>%
# 	filter(cluster_pred == 15)

# hull_1 <- concaveman(cluster_15, concavity = 1)
# hull_1_network <- st_intersection(sf_network, hull_1)
# hull_3 <- concaveman(cluster_15, concavity = 3)
# hull_3_network <- st_intersection(sf_network, hull_3)
# plot(hull_1_network$geom_way)
# plot(hull_3_network$geom_way)
