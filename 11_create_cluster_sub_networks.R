Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")
################################################################################
# Input
################################################################################
available_data <- psql1_get_schemas(con)
available_data
char_schema <- available_data[2, "schema_name"]
availabe_cluster_tables <- psql1_get_tables_in_schema(con, char_schema)
availabe_cluster_tables
char_data <- availabe_cluster_tables[1, "table_name"]


sf_cluster_points <- st_read(
	con,
	query = paste0("SELECT * FROM ", 
								 char_schema, 
								 ".", 
								 char_data)
)

sf_cluster_points <- sf_cluster_points %>%
	as.data.table() %>%
	filter(cluster_pred!=0)

available_networks <- psql1_get_available_networks(con)
print(available_networks)
char_network <- available_networks[2, "table_name"]
dt_network <- st_read(con, char_network) %>%
	as.data.table()
sf_network <- st_as_sf(dt_network) %>%
	mutate(m = 1000*km)
int_dist <- 15




char_path_dt_dist_mat <- here::here("data", "input", "dt_dist_mat")
char_av_dt_dist_mat_files <- list.files(char_path_dt_dist_mat)
print(char_av_dt_dist_mat_files)
# stop("Have you chosen the right dist mat?")
char_dt_dist_mat <-  char_av_dt_dist_mat_files[18]
char_buffer <- "500"
dt_dist_mat <- read_rds(here::here(
	char_path_dt_dist_mat,
	char_dt_dist_mat))


# m-Spalte runden und in Integer umwandeln
dt_dist_mat <- dt_dist_mat %>%
	mutate(m = round(m, 0) %>% as.integer())


################################################################################
# sample points on network
################################################################################
char_network_sampled_pts <- paste0(char_network, 
																	 "_sampled_pts_", 
																	 int_dist, 
																	 "m_dist")

psql1_sample_points_on_network(con = con,
															 char_network = char_network,
															 char_network_sampled_pts = char_network_sampled_pts,
															 int_dist = int_dist)

sf_network_sampled_pts <- st_read(con, char_network_sampled_pts) %>%
	mutate(id = as.integer(id))
################################################################################
# Calculate distances from sample dpoints to cluster-points
################################################################################
dt_pts_nd <- parallel_process_networks_2(dt_od_pts_org = sf_cluster_points,
																				 dt_od_pts_sampled = sf_network_sampled_pts,
																				 dt_network,
																				 dt_dist_mat,
																				 int_cores) %>%
	as.data.table()


df_knn <- cpp_find_knn(dt_pts_nd, 1, sf_network_sampled_pts$id)


df_cluster_points <- sf_cluster_points %>% as.data.frame()
df_network_sampled <- sf_network_sampled_pts %>% as.data.frame()
df_knn <- df_knn %>%
	left_join(df_cluster_points %>%
							select(id, cluster_pred), by = c("NN1" = "id"))

sf_knn <- df_knn %>%
	left_join(df_network_sampled %>%
							select(id, geometry), by = c("flow_ref" = "id")) %>%
	st_as_sf()


sfn_network <- sf_network %>% as_sfnetwork()

sfn_network_ext <- st_network_blend(sfn_network, sf_knn)

tibble_nodes <- sfn_network_ext %>%
	activate(nodes) %>%
	mutate(id = row_number()) %>%
	as.data.frame() %>%
	as_tibble()

sf_edges <- sfn_network_ext %>%
	activate(edges) %>%
	as.data.frame() %>%
	as_tibble() %>%
	left_join(tibble_nodes %>% select(id, cluster_pred), by = c("from" = "id")) %>%
	rename(cluster_from = cluster_pred) %>%
	left_join(tibble_nodes %>% select(id, cluster_pred), by = c("to" = "id")) %>%
	rename(cluster_to = cluster_pred) %>%
	as.data.frame()%>%
	st_as_sf()

df_edges <- data.frame(from = sf_edges$source,
																	 to = sf_edges$target,
																	 weight = sf_edges$km*1000,
																	 edge_id = sf_edges$id)


sf_edges <- sf_edges %>%
	rowwise() %>%
	mutate(
		cluster_pred = if_else(
			cluster_from == cluster_to,
			cluster_from,
			sample(c(cluster_from, cluster_to), size = 1)
		)
	) %>%
	ungroup() %>%
	as.data.frame() %>%
	st_as_sf()



st_write(sf_edges, con, Id(schema=char_schema, 
																				table = "cluster_sub_networks"))


# g <- graph_from_data_frame(d = df_edges,
# 													 directed = FALSE)
# comp <- components(g)
# comp$no
# 
# 
# Sys.time()
# timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
print(timestamp)

