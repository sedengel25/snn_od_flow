################################################################################
# Source
################################################################################
source("./src/config.R")
source("./src/functions.R")
sourceCpp("./src/helper_functions.cpp")


################################################################################
# Configuration
################################################################################
list.files(path = path_bbox_coordinates, pattern = "*.poly")

char_region_abb <- "dd_standard"

char_polygon_filename <- paste0(char_region_abb, ending_polygon)   

char_polygon_file <- here::here(path_bbox_coordinates,
																char_polygon_filename) 



char_pbf_filename <- paste0(
	char_region_abb,
	".osm.pbf")

char_pbf_file <- here::here(path_osm_pbf, 
															 char_pbf_filename)

char_network <-  paste0(char_region_abb, "_2po_4pgr")

char_sql_filename <- paste0(char_network, ".sql")


################################################################################
# 1. Create routable network from given region
################################################################################
osmconvert_create_sub_osm_pbf(file_pbf = file_ger_osm_pbf)

osm2po_create_routable_network(int_crs = 32632)

################################################################################
# 2. Create local node distance matrix
################################################################################
sf_network <- st_read(con, char_network) %>%
	mutate(m = km*1000)

plot(sf_network$geom_way)

sfn_network <- as_sfnetwork(sf_network)

sfn_network <- sfn_network %>%
	activate(nodes) %>%
	mutate(node_id = row_number(), component = group_components()) 

sfn_network_biggest_comp <- sfn_network %>%
	activate(nodes) %>%
	filter(component == 1) 

igraph_network <- as.igraph(sfn_network_biggest_comp)
components(igraph_network)

sf_network <- sfn_network_biggest_comp %>%
	activate(edges) %>%
	as.data.frame() %>%
	st_as_sf()

sf_network <- sf_network %>%
	select(-c(from, to))
st_write(sf_network, con, char_network)

ncol(sf_network)


int_buffer <- 50000

dt_dist_mat <- calc_local_node_dist_mat(buffer = int_buffer)

write_rds(x = dt_dist_mat, file = paste0("./data/input/dt_dist_mat/",
																				 char_region_abb,
																				 "_",
																				 int_buffer,
																				 "_dist_mat.rds"))

# dbWriteTable(con, paste0(char_region_abb,
# 						 "_",
# 						 int_buffer,
# 						 "_dist_mat"), value = dt_dist_mat)

igraph_dist_mat <- dt_dist_mat %>%
	as.data.frame() %>%
	rename("from" = "source",
				 "to" = "target") %>%
	select(from, to) %>%
	igraph::graph_from_data_frame()

igraph_dist_mat_components <- igraph::components(igraph_dist_mat)

cat("Total components: ",
		igraph_dist_mat_components$csize %>% sum, "\n")

cat("Biggest number of cohrerent components: ",
		max(igraph_dist_mat_components$csize), "\n")


