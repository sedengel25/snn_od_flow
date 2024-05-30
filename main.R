################################################################################
# Source
################################################################################
source("./src/config.R")
source("./src/functions.R")



################################################################################
# Configuration
################################################################################



list.files(path = path_bbox_coordinates, pattern = "txt")


char_region_abb <- "dd"

char_polygon_filename <- paste0(char_region_abb, ending_polygon)   

char_polygon_file <- here::here(path_bbox_coordinates,
																char_polygon_filename) 



chat_output_filename <- paste0(
	char_region_abb,
	".osm.pbf")

char_output_file <- here::here(path_osm_pbf, 
															 chat_output_filename)

char_network <-  paste0(char_region_abb, "_2po_4pgr")

char_sql_filename <- paste0(char_network, ".sql")

################################################################################
# 1. Create routable network from given region
################################################################################
osmconvert_create_sub_osm_pbf()

osm2po_create_routable_network()
