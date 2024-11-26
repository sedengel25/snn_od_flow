Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")
################################################################################
# Input
################################################################################
available_tables <- psql1_get_point_cluster_tables(con)
char_data <- available_tables[2, "table_name"]
sf_cluster_points <- st_read(con, char_data)
sf_cluster_points <- sf_cluster_points %>%
	as.data.table()

available_networks <- psql1_get_available_networks(con)
print(available_networks)
char_network <- available_networks[1, "table_name"]
dt_network <- st_read(con, char_network) %>%
	as.data.table()
sf_network <- st_as_sf(dt_network) %>%
	mutate(m = 1000*km)
################################################################################
# Turn point-clusters into street segments
################################################################################
list_cluster_edges <- sf_cluster_points %>%
	group_by(cluster_pred) %>%
	summarise(id_edges = list(unique(id_edge)), .groups = "drop") %>%
	pull(id_edges)

df_edges <- data.frame(from = sf_network$source,
										to = sf_network$target,
										weight = sf_network$m,
										edge_id = sf_network$id)

g <- graph_from_data_frame(df_edges, directed = FALSE)
E(g)$edge_id <- df_edges$edge_id


sf_cluster_networks <- get_networks_per_cluster(sf_cluster_points = sf_cluster_points,
												 sf_network = sf_network,
												 g = g)




char_cluster_networks <- paste0(char_data, "_cl_network")
st_write(sf_cluster_networks, con, char_cluster_networks)
################################################################################
# Find outer points
################################################################################
sf_cluster_outerpoints <- get_outer_points_per_cluster(sf_cluster_points = sf_cluster_points,
																								sf_cluster_networks = sf_cluster_networks)

sf_cluster_outerpoints <- sf_cluster_outerpoints %>%
	group_by(cluster_pred) %>% 
	distinct(points, .keep_all = TRUE) %>% #
	ungroup() %>%
	as.data.frame() %>%
	st_as_sf()

char_cluster_outerpoints <- paste0(char_data, "_cl_op")
st_write(sf_cluster_outerpoints, con, char_cluster_outerpoints)


################################################################################
# Reduce clsuter networks to outerpoints
################################################################################
sf_cluster_networks_red <- reduce_networks_to_op(sf_cluster_networks = sf_cluster_networks,
											sf_cluster_outerpoints = sf_cluster_outerpoints)


char_cluster_networks_red <- paste0(char_data, "_cl_network_red")
st_write(sf_cluster_networks_red, con, char_cluster_networks_red)

