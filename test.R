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



op1 <- sf_outerpoints %>%
	filter(cluster_pred==1) %>%
	st_as_sf()
npc1 <- sf_network_per_cluster %>%
	filter(cluster_pred==1) %>%
	st_as_sf()




network_cl <- as_sfnetwork(npc1, directed = FALSE)
network_cl_ext <- st_network_blend(network_cl, op1)
network_cl <- network_cl %>%
	activate("edges") %>%
	as.data.frame() %>%
	st_as_sf()
blended_edges <- network_cl_ext %>%
	activate("edges") %>%
	as_tibble() %>%
	rowwise() %>%
	mutate(
		source = ifelse(
			is.na(source),
			# Finde den nächsten Knoten basierend auf der Geometrie
			which.min(st_distance(geometry, st_geometry(network_cl_ext %>% activate("nodes")))),
			source
		),
		target = ifelse(
			is.na(target),
			# Finde den nächsten Knoten für das Ziel
			which.min(st_distance(geometry, st_geometry(network_cl_ext %>% activate("nodes")))),
			target
		)
	)
blended_edges <- blended_edges %>%
	as.data.frame() %>%
	st_as_sf()


node_counts <- data.table(node = c(lines$source, lines$target)) %>%
	group_by(node) %>%
	summarise(count = n())


outer_edges <- lines %>%
	filter(
		source %in% node_counts$node[node_counts$count == 1] |
			target %in% node_counts$node[node_counts$count == 1]
	) %>%
	as.data.table()


ggplot()+
	geom_sf(data=op1)+
	#geom_sf(data=npc1)+
	geom_sf(data=blended_edges$geom_way)

#use "from" and "to" to identify the outer edges 

