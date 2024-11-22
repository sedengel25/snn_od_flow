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
sf_network <- sf_network %>%
	mutate(m = km*1000)


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
# st_write(sf_points, con, char_data, delete_layer = TRUE)Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
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

char_data_2_points <- paste0(
	char_data_cluster, "_p2_", int_k, "_", int_eps, "_", int_minpts, "_points")
st_write(sf_cluster_pred_points, con, char_data_2, delete_layer = TRUE)
################################################################################
# Turn point-clusters into street segments
################################################################################
sf_cluster_pred_points <- st_read(con, 
																	"nb_dd_mapped_f2000_p20_10_12_cluster_p2_40_20_22_no_dup")

sf_cluster_pred_points <- sf_cluster_pred_points %>%
	as.data.table()

list_cluster_edges <- sf_cluster_pred_points %>%
	group_by(cluster_pred) %>%
	summarise(id_edges = list(unique(id_edge)), .groups = "drop") %>%
	pull(id_edges)

dt_network <- st_read(con, char_network) %>%
	as.data.table()

setkey(dt_network, id)


r1_get_edge_ids_from_sp <- function(unconnected_source_nodes,
																		connected_source_nodes																		) {
	edges <- lapply(unconnected_source_nodes, function(unconnected_node) {
		#print("--------------------------------------------")
		#cat("source: ", unconnected_node, "\n")
		sapply(connected_source_nodes, function(connected_node) {
			#cat("target: ", connected_node, "\n")
			sp <- nx$shortest_path(g,
														 source = unconnected_node,
														 target = connected_node, 
														 weight = "m")
			
			
			pairs <- as.matrix(embed(sp, 2)[, 2:1])
			edge_ids <- apply(pairs, 1, function(row) {
				u <- row[1]
				v <- row[2]
				edge_id <- g$get_edge_data(u = u, v = v)$id
				return(edge_id)
			})
			#cat("edge_ids: ", edge_ids, "\n")
			edge_ids
		})
	})
}
instantiate_py()
g <- nx$from_pandas_edgelist(df = sf_network,
														 source = "source",
														 target = "target",
														 edge_attr = c("m", "id"))


int_cl <- 1
missing_edges <- lapply(list_cluster_edges, function(edges){
	#print("--------------------------------------------")
	cat("Cluster: ", int_cl, "\n")
	x <- dt_network[id %in% edges, .(id, source, target)]
	node_counts <- data.table(node = c(x$source, 
																		 x$target))[, .N, by = node]
	single_nodes <- node_counts[N == 1]$node
	unconnected_edges <- x[source %in% single_nodes & target %in% single_nodes]
	unconnected_source_nodes <- unconnected_edges$source
	connected_source_nodes <- x$source[!(x$source %in% unconnected_source_nodes)]
	# print(x)
	# cat("connected_source_nodes: ", connected_source_nodes, "\n")
	# cat("unconnected_source_nodes: ", unconnected_source_nodes, "\n")
	missing_edges <- r1_get_edge_ids_from_sp(unconnected_source_nodes,
																					 connected_source_nodes) %>% 
		unlist() %>% 
		unique()
	#cat("missing edges for cluster ", int_cl, ": ", missing_edges, "\n")
	int_cl <<- int_cl + 1
	missing_edges
})


contained_edges <- lapply(list_cluster_edges, function(edges){
	#print("--------------------------------------------")
	cat("Cluster: ", int_cl, "\n")
	x <- dt_network[id %in% edges, .(id, source, target)]
	x$id
})






full_edges <- mapply(function(x, y) 
	unique(c(x, y)), contained_edges, missing_edges, SIMPLIFY = FALSE)

sf_cluster <- bind_rows(
	lapply(seq_along(full_edges), function(cluster_id) {
		# IDs für den aktuellen Cluster
		cluster_ids <- full_edges[[cluster_id]]
		
		# Filtere die entsprechenden Geometrien und füge Cluster-ID hinzu
		sf_network %>%
			filter(id %in% cluster_ids) %>%
			mutate(cluster_pred = cluster_id,
						 edge_ids = cluster_ids)
	})
)


char_data_2_lines <- paste0(
	char_data_cluster, 
	"_p2_", 
	int_k, "_", 
	int_eps, "_", 
	int_minpts, "_road_segments")

st_write(sf_cluster, con, char_data_2_lines)
################################################################################
# Find outer points
################################################################################
r1_get_outer_points_per_cluster <- function(sf_points_clustered, sf_lines) {
	
	cluster_ids <- 1:max(sf_cluster_pred_points$cluster_pred)
	list_of_outer_points <- lapply(cluster_ids, function(i){
		points <- sf_points_clustered %>%
			filter(cluster_pred == i)
		
		lines <- sf_lines %>%
			filter(cluster_pred==i)
		
		node_counts <- data.table(node = c(lines$source, lines$target)) %>%
			group_by(node) %>%
			summarise(count = n())
		
		
		outer_edges <- lines %>%
			filter(
				source %in% node_counts$node[node_counts$count == 1] |
					target %in% node_counts$node[node_counts$count == 1]
			) %>%
			as.data.table()
		
		outer_edges <- outer_edges %>%
			mutate(
				unattached_at_source = !(source %in% lines$target | 
																 	source %in% lines$source[-match(source, lines$source)]),
				unattached_at_target = !(target %in% lines$source | 
																 	target %in% lines$target[-match(target, lines$target)])
			)
		outer_points <- points[which(points$id_edge %in% outer_edges$id),] %>%
			as.data.table()
		outer_points <- outer_points %>%
			left_join(outer_edges %>% select(id, unattached_at_source, unattached_at_target),
								by = c("id_edge" = "id"))
		
		most_outer_points <- outer_points %>%
			group_by(id_edge) %>%
			filter(
				(unattached_at_source & dist_to_start == min(dist_to_start)) | 
					(unattached_at_target & dist_to_end == min(dist_to_end))
			) %>%
			ungroup() %>%
			as.data.frame() %>%
			st_as_sf()

		most_outer_points
	})
	outer_points <- rbindlist(list_of_outer_points) %>%
		st_as_sf()
	return(outer_points)
}


outer_points <- r1_get_outer_points_per_cluster(sf_points_clustered = sf_cluster_pred_points,
																sf_lines = sf_cluster)
char_data_2_outerpoints <- paste0(
	char_data_cluster, "_p2_", int_k, "_", int_eps, "_", int_minpts, "_outerpoints")
st_write(outer_points, con, char_data_2_outerpoints)
