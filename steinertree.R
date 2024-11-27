Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")
################################################################################
# Input
################################################################################
available_networks <- psql1_get_available_networks(con)
char_network <- available_network_clusters[1, "table_name"]
sf_network <- st_read(con, char_network)


available_tables <- psql1_get_point_cluster_tables(con)
char_data <- available_tables[2, "table_name"]
sf_cluster_points <- st_read(con, char_data)
sf_cluster_points <- sf_cluster_points %>%
	as.data.table()


list_cluster_edges <- sf_cluster_points %>%
	group_by(cluster_pred) %>%
	summarise(id_edges = list(unique(id_edge)), .groups = "drop") %>%
	pull(id_edges)


terminal_nodes <- sf_network %>%
	filter(id %in% 	list_cluster_edges[[25]]) %>%
	as.data.frame()%>%
	select(source, target) %>%
	pull(source, target) %>%
	unique() 
	
sort(terminal_nodes)

terminal_nodes <- terminal_nodes %>% as.character()
################################################################################
# SteinerTree
################################################################################
#devtools::install_github("krashkov/SteinerNet")
library(SteinerNet)


df_edges <- data.frame(from = sf_network$source,
											 to = sf_network$target,
											 weight = sf_network$m)

g <- graph_from_data_frame(df_edges, directed = FALSE)


list_neighborhood <- neighborhood(g, 
						 order = 3, 
						 nodes = terminal_nodes)


neighborhood_nodes <- unlist(list_neighborhood) %>% names()
setdiff(neighborhood_nodes, V(g)$name)
subgraph_nodes <- c(terminal_nodes, neighborhood_nodes) %>% unique()


subgraph <- induced_subgraph(g, vids = subgraph_nodes)
components(subgraph)$no
plot(subgraph)
t1 <- proc.time()
steiner_tree <- SteinerNet::steinertree(type = "SPM",
												terminals = terminal_nodes,
												graph = subgraph)
t2 <- proc.time()
print(t2-t1)
