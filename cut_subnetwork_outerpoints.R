Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

char_data <- "nb_dd_mapped_f2000_p20_10_12_cluster_p2_40_20_22"
sf_outerpoints <- st_read(con, paste0(char_data,"_outerpoints")) %>%
	as.data.frame()
sf_network <- st_read(con, "dd_2po_4pgr") %>%
	as.data.frame()
sf_outerpoints <- sf_outerpoints %>%
	left_join(sf_network %>% select(id, source, target), by = c("id_edge" = "id"))
sf_network_per_cluster <- st_read(con, paste0(char_data,"_road_segments")) %>%
	as.data.frame()

total_ids <- 1:max(sf_network_per_cluster$cluster_pred)

list_sf_networks <- list()

for (i in total_ids){
	print(i)
	#i = 211
	op1 <- sf_outerpoints %>%
		filter(cluster_pred==i) %>%
		st_as_sf()
	npc1 <- sf_network_per_cluster %>%
		filter(cluster_pred==i) %>%
		st_as_sf()
	
	if(nrow(npc1) <= 1){
		next
	}
	# ggplot()+
	# 	geom_sf(data=op1)+
	# 	geom_sf(data=npc1)
	# 
	network_cl <- as_sfnetwork(npc1, directed = FALSE)
	network_cl_ext <- st_network_blend(network_cl, op1)
	network_cl_ext <- network_cl_ext %>%
		activate("edges") %>%
		as.data.table()
	
	network_cl_ext <- network_cl_ext %>%
		mutate(id_new = 1:nrow(network_cl_ext))
	
	node_counts <- data.table(node = c(network_cl_ext$from, 
																		 network_cl_ext$to)) %>%
		group_by(node) %>%
		summarise(count = n())
	
	
	outer_edges <- network_cl_ext %>%
		filter(
			from %in% node_counts$node[node_counts$count == 1] |
				to %in% node_counts$node[node_counts$count == 1]
		) %>%
		as.data.table()
	
	network_cl_ext <- network_cl_ext %>%
		filter(!id_new %in% outer_edges$id_new) %>%
		st_as_sf()
	
	# ggplot()+
	# 	geom_sf(data=op1)+
	# 	geom_sf(data=network_cl_ext)
	
	list_sf_networks[[i]] <- network_cl_ext
}



