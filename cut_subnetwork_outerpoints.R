Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

char_data <- "nb_dd_mapped_f2000_p20_10_12_cluster_p2_40_20_22"
sf_outerpoints <- st_read(con, paste0(char_data,"_outerpoints")) %>%
	as.data.frame()
sf_network <- st_read(con, "dd_2po_4pgr") %>%
	as.data.frame()
sf_outerpoints <- sf_outerpoints %>%
	left_join(sf_network %>% select(id, source, target), by = c("id_edge" = "id")) %>%
	st_as_sf()
sf_network_per_cluster <- st_read(con, paste0(char_data,"_road_segments")) %>%
	as.data.frame()

total_ids <- 1:max(sf_network_per_cluster$cluster_pred)

list_sf_networks <- list()

# instantiate_py()
# g <- nx$from_pandas_edgelist(df = sf_network,
# 														 source = "source",
# 														 target = "target",
# 														 edge_attr = c("m", "id"))

for (i in total_ids){
	print(i)
	# Get outer points per cluster
	op1 <- sf_outerpoints %>%
		filter(cluster_pred==i) %>%
		as.data.frame() %>%
		st_as_sf()

	# Get street network involved in cluster
	npc1 <- sf_network_per_cluster %>%
		filter(cluster_pred==i) %>%
		as.data.frame() %>%
		st_as_sf()
 
	# ggplot()+
	#  	geom_sf(data=op1)+
	#  	geom_sf(data=npc1)

	# Transform cluster-network to sf-network
	network_cl <- as_sfnetwork(npc1, directed = FALSE)
	
	network_cl_nodes <- network_cl %>%
		activate("nodes") %>%
		as.data.frame() %>%
		st_as_sf()
	
	network_cl_edges <- network_cl %>%
		activate("edges") %>%
		as.data.frame() %>%
		st_as_sf() 
	

	# If cluster consists of only one point (station) draw a linestring to a almost
	# identical point to get the same sfc-format for all clusters
	if(nrow(op1)==1 & nrow(npc1)==1){
		coords <- st_coordinates(op1)
		near_coords <- coords + c(0.0001, -0.0001)
		new_linestring <- st_linestring(rbind(coords, near_coords))
		st_geometry(network_cl_edges) <- st_sfc(new_linestring,
																						crs = st_crs(network_cl_edges))

		list_sf_networks[[i]] <- network_cl_edges
		next
	}
	# ggplot()+
	#  	geom_sf(data=network_cl_edges)+
	#  	geom_sf(data=network_cl_nodes)+
	# 	geom_sf(data=op1)
	

	# Find outerpoints that are also nodes in the sf-network...
	duplicates <- st_intersects(op1, network_cl_nodes, sparse = FALSE)
	idx_duplicates <- which(duplicates, arr.ind = TRUE)
	idx_duplicates <- idx_duplicates[,1]
	

	# ... and remove them, as this creates an error when trying to "blend"
	#op1_org <- op1
	
	# when 1 line, 2 outerpoints und 1 outerpoint==network node, dann
	if(nrow(op1)==2 & nrow(npc1)==1 & length(idx_duplicates)==1){
		cat("Cluster: ", i, "\n")
		network_cl_ext <- st_network_blend(network_cl, op1)
		# 1. Set index for row number
		network_cl_ext <- network_cl_ext %>%
			activate("nodes") %>%
			mutate(node_index = row_number()) # Numeriere Knoten explizit
		
		# 2. Get the row with NA-entry as this got added by blending but we don't want it
		na_nodes <- network_cl_ext %>%
			activate("nodes") %>%
			filter(is.na(cluster_od) | is.na(id_edge)) %>%
			pull(node_index)
		
		network_cl_ext_edges <- network_cl_ext %>%
			activate("edges") %>%
			filter(!from %in% na_nodes & !to %in% na_nodes) %>%
			as.data.frame() %>%
			st_as_sf()
		
		# ggplot()+
		# 	geom_sf(data=network_cl_ext_edges)+
		# 	geom_sf(data=op1)
		
		list_sf_networks[[i]] <- network_cl_ext_edges
	
		next	
	}
	
	if(length(idx_duplicates)>0){
		op1 <- op1[-idx_duplicates,]
	}

	# ggplot()+
	# 	geom_sf(data=op1)+
	# 	geom_sf(data=npc1)

	# Turn outer points into nodes of the network
	network_cl_ext <- st_network_blend(network_cl, op1)
	
	network_cl_ext_nodes <- network_cl_ext %>%
		activate("nodes") %>%
		st_as_sf()

	network_cl_ext_edges <- network_cl_ext %>%
		activate("edges") %>%
		st_as_sf()
	
	network_cl_ext_edges <- network_cl_ext_edges %>%
		mutate(id_new = 1:nrow(network_cl_ext_edges))
	
	# ggplot()+
	#  	geom_sf(data=st_as_sf(network_cl_ext_edges))+
	#  	geom_sf(data=network_cl_ext_nodes)
	
	# Find nodes that appear only once and thus represent outer nodes
	node_counts <- data.table(node = c(network_cl_ext_edges$from, 
																		 network_cl_ext_edges$to)) %>%
		group_by(node) %>%
		summarise(count = n())
	
	# Find the corresponding edges
	all_outer_edges <- network_cl_ext_edges %>%
		filter(
			from %in% node_counts$node[node_counts$count == 1] |
				to %in% node_counts$node[node_counts$count == 1]
		) %>%
		as.data.table()
	


	# From all outer edges, find the ones that were newly created by the blending...
	new_outer_edges <- all_outer_edges %>%
		dplyr::anti_join(network_cl_edges, by = c("from", "to"))
	
	
	# ... and remove them
	# This can be dangerous in the edge-case of cluster 62 where the 2 outerpoints
	# are on the one single linestring and it is also equal to one node of
	# the original linestring
	network_cl_ext_edges <- network_cl_ext_edges %>%
		filter(!id_new %in% new_outer_edges$id_new) %>%
		st_as_sf()
	
	
	p <- ggplot()+
		geom_sf(data=network_cl_ext_edges)+
		geom_sf(data=op1) 
	
	network_cl_ext_edges <- network_cl_ext_edges %>%
		select(-id_new)
	list_sf_networks[[i]] <- network_cl_ext_edges
}


str(list_sf_networks[[1]])
str(list_sf_networks[[62]])
sf_cluster_networks <- rbindlist(list_sf_networks) %>%
	st_as_sf()


sf_outerpoints <- st_write(sf_cluster_networks, 
													 con,
													 paste0(char_data,"_cl_networks"))
