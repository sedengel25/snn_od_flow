Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Network data
################################################################################
available_networks <- psql1_get_available_networks(con)
print(available_networks)
char_network <- available_networks[12, "table_name"]
dt_network <- st_read(con, char_network) %>% as.data.table
sf_network <- st_as_sf(dt_network)
ggplot() +
	geom_sf(data=sf_network) 

char_path_dt_dist_mat <- here::here("data", "input", "dt_dist_mat")
char_av_dt_dist_mat_files <- list.files(char_path_dt_dist_mat)
print(char_av_dt_dist_mat_files)
# stop("Have you chosen the right dist mat?")
char_dt_dist_mat <-  char_av_dt_dist_mat_files[15]
char_buffer <- "2000"
dt_dist_mat <- read_rds(here::here(
	char_path_dt_dist_mat,
	char_dt_dist_mat))


# m-Spalte runden und in Integer umwandeln
dt_dist_mat <- dt_dist_mat %>%
	mutate(m = round(m, 0) %>% as.integer())



available_mapped_trip_data <- psql1_get_mapped_trip_data(con)
print(available_mapped_trip_data)
char_data <- available_mapped_trip_data[6, "table_name"]
sf_trips <- st_read(con, char_data) %>%
	rename("o_id" = "id_edge_origin",
				 "d_id" = "id_edge_dest")


sf_trips$month <- lubridate::month(sf_trips$start_datetime)
sf_trips$week <- lubridate::week(sf_trips$start_datetime)
sf_trips <- sf_trips %>%
	arrange(start_datetime)


dist_filter <- 500
int_kw <- c(13)
sf_trips <- sf_trips %>%
	filter(week %in% int_kw,
				 trip_distance >= dist_filter)



sf_trips <- sf_trips %>% mutate(o_id = as.integer(o_id),
																d_id = as.integer(d_id))


sf_trips
sf_points <- sf_trips %>%
	pivot_longer(
		cols = starts_with("o_") | starts_with("d_"),
		names_to = c("type", ".value"),
		names_pattern = "(o|d)_(.*)"
	) %>%
	mutate(type = ifelse(type == "o", "origin", "dest")) %>%
	as.data.frame() %>%
	rename("id_edge" = "id") %>%
	mutate(id = 1:(2*nrow(sf_trips))) %>%
	st_as_sf()

t_start <- proc.time()
################################################################################
# 3. Calculate network distances between OD flows and put it into a matrix
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

t_end<- proc.time()
print(t_end - t_start)
dt_cluster_overview <- table(dt_snn_pred_nd$cluster_pred) %>% 
	as.data.table() %>%
	arrange(desc(N))
head(dt_cluster_overview,20)
gc()

################################################################################
# 4. Postprocess cluster results and write them into PSQL-DB
################################################################################
sf_cluster_nd_pred_points <- sf_points %>%
	left_join(dt_snn_pred_nd, by = c("id" = "id"))
st_drop_geometry(sf_cluster_nd_pred_points)
st_geometry(sf_cluster_nd_pred_points) <- "closest_point"
sf_cluster_nd_pred_points <- sf_cluster_nd_pred_points %>%
	select(-origin_geom, -dest_geom, -line_geom)
st_geometry(sf_cluster_nd_pred_points)

cluster_convex_hulls <- sf_cluster_nd_pred_points %>%
	group_by(cluster_pred) %>%
	summarise(segment = st_combine(closest_point)) %>%
	st_convex_hull() %>%
	as.data.frame() %>%
	mutate(cluster_pred = as.numeric(cluster_pred)) %>%
	st_as_sf()


cluster_convex_hulls <- cluster_convex_hulls %>%
	filter(st_geometry_type(segment) == "POLYGON")

st_write(cluster_convex_hulls, con, "dd_segments")

ggplot() +
	#geom_sf(data = sf_cluster_nd_pred[sf_cluster_nd_pred$cluster_pred==19,], 
	#				aes(color = cluster_pred), size = 0.5) +
	geom_sf(data = cluster_convex_hulls, fill = NA, color = "black", size = 1) +
	theme_minimal() +
	theme(legend.position = "none")

str(sf_cluster_nd_pred)
ggplot(data = sf_cluster_nd_pred[sf_cluster_nd_pred$cluster_pred==19,]) +
	geom_sf(data=sf_network) +
	geom_sf(aes(color = as.character(cluster_pred)), size = 1) +
	theme_minimal()+
	theme(legend.position = "none")

st_write(sf_cluster_nd_pred, con, paste0(char_data, 
																				 "_filter", 
																				 dist_filter, 
																				 "_parameter",
																				 int_k,
																				 "_",
																				 int_eps,
																				 "_",
																				 int_minpts,
																				 "_cluster"))


################################################################################
# 5. Assign flows to clusters
################################################################################
sf_cluster_nd_pred_flows <- sf_points %>%
	left_join(dt_snn_pred_nd, by = c("id" = "id"))















sf_cluster_nd_pred_flows <- sf_cluster_nd_pred_flows %>%
	filter(cluster_pred != 0) %>%
  mutate(
    # Kombination von Start- und Ziel-Cluster (immer gleiche Reihenfolge für bidirektionale Paare)
    cluster_pred_flow = pmap_chr(
      list(cluster_pred, lead(cluster_pred, default = NA)),
      ~ paste0(sort(c(..1, ..2)), collapse = "_")
    )
  )

st_drop_geometry(sf_cluster_nd_pred_flows)
st_geometry(sf_cluster_nd_pred_flows) <- "closest_point"
sf_cluster_nd_pred_flows <- sf_cluster_nd_pred_flows %>%
	select(-origin_geom, -dest_geom, -line_geom)
st_geometry(sf_cluster_nd_pred_flows)

ggplot() +
	geom_sf(data = sf_cluster_nd_pred_flows[
		sf_cluster_nd_pred_flows$cluster_pred %in% c(19,96),],
				aes(color = cluster_pred), size = 0.5) +
	geom_sf(data = cluster_convex_hulls, fill = NA, color = "black", size = 1) +
	theme_minimal() +
	theme(legend.position = "none")
# head(sf_cluster_nd_pred_flows$cluster_pred_flow,50)
# 
# st_drop_geometry(sf_cluster_nd_pred_flows)
# st_geometry(sf_cluster_nd_pred_flows) <- "line_geom"
# sf_cluster_nd_pred_flows <- sf_cluster_nd_pred_flows %>%
# 	select(-origin_geom, -dest_geom, -closest_point)
# st_geometry(sf_cluster_nd_pred_flows)


top_n <- 40
table(sf_cluster_nd_pred_flows$cluster_pred_flow) %>%
	as.data.table() %>%
	arrange(desc(N)) %>%
	head(top_n) %>%
	pull(N) %>%
	sum()

table(sf_cluster_nd_pred_flows$cluster_pred_flow) %>%
	as.data.table() %>%
	arrange(desc(N)) %>%
	head(top_n)

top_segments <- table(sf_cluster_nd_pred_flows$cluster_pred_flow) %>%
	as.data.table() %>%
	arrange(desc(N)) %>%
	head(top_n) %>%
	pull(V1) 
# %>%
# 	strsplit("_") %>%
# 	unlist() %>%
# 	as.numeric() %>%
# 	unique() %>%
# 	sort()

sf_cluster_nd_pred_flows <- sf_cluster_nd_pred_flows %>%
	filter(sf_cluster_nd_pred_flows$cluster_pred_flow %in% top_segments)
st_write(sf_cluster_nd_pred_flows, con, "dd_segment_flows")


