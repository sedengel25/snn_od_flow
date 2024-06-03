################################################################################
# Source
################################################################################
source("./src/config.R")
source("./src/functions.R")



################################################################################
# Configuration
################################################################################
list.files(path = path_bbox_coordinates, pattern = "*.poly")

char_region_abb <- "dd"

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
#osmconvert_create_sub_osm_pbf()

#osm2po_create_routable_network()

################################################################################
# 2. Create local node distance matrix
################################################################################
sf_network <- st_read(con, char_network) %>%
	mutate(m = km*1000) 

int_buffer <- 500

dt_dist_mat <- calc_local_node_dist_mat()

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

################################################################################
# 3. 
################################################################################
