################################################################################
# Source
################################################################################
source("./src/config.R")
source("./src/functions.R")



################################################################################
# Configuration
################################################################################
list.files(path = path_bbox_coordinates, pattern = "*.poly")

char_region_abb <- "col"

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
# osmconvert_create_sub_osm_pbf()
# 
# osm2po_create_routable_network()

################################################################################
# 2. Create local node distance matrix
################################################################################
sf_network <- st_read(con, char_network) %>%
	mutate(m = km*1000)

# 
# int_buffer <- 500
# 
# dt_dist_mat <- calc_local_node_dist_mat()
# 
# igraph_dist_mat <- dt_dist_mat %>%
# 	as.data.frame() %>%
# 	rename("from" = "source",
# 				 "to" = "target") %>%
# 	select(from, to) %>% 
# 	igraph::graph_from_data_frame() 
# 
# igraph_dist_mat_components <- igraph::components(igraph_dist_mat)
# 
# cat("Total components: ", 
# 		igraph_dist_mat_components$csize %>% sum, "\n")
# 
# cat("Biggest number of cohrerent components: ",
# 		max(igraph_dist_mat_components$csize), "\n")

################################################################################
# 3. 
################################################################################
dt_trips <- read_rds(file = "./data/dt_voi_cologne_06_05.rds")
head(dt_trips)
char_min_date <- dt_trips$start_time %>% min %>% date %>% as.character
char_max_date <- dt_trips$start_time %>% max %>% date %>% as.character

char_name_trips <- tolower(paste0(dt_trips[1, "provider"] %>% as.character,
													"_",
													dt_trips[1, "city"] %>% as.character,
													"_from_", 
													gsub("-", "_", char_min_date)))


RPostgres::dbWriteTable(conn = con,
												name = char_name_trips,
												value = dt_trips,
												overwrite = TRUE)

query <- "DROP TABLE IF EXISTS mapped_points"
dbExecute(con, query)

query <- paste0("CREATE TABLE mapped_points AS
									SELECT
									*,
									ST_Transform(ST_SetSRID(ST_MakePoint(start_loc_lon, start_loc_lat), 4326), 32632) AS origin_geom,
									ST_Transform(ST_SetSRID(ST_MakePoint(dest_loc_lon, dest_loc_lat), 4326), 32632) AS dest_geom
									FROM
									", char_name_trips, ";")

dbExecute(con, query)

psql1_create_spatial_index(con, "mapped_points")
psql1_create_index(con, "mapped_points", col = "id_new")

query <- paste0("
UPDATE mapped_points
SET id = subquery.id
FROM(SELECT id-10000 FROM mapped_points) AS subquery;")
cat(query)
dbGetQuery(con, query)



query <- paste0("
UPDATE mapped_points mp
SET origin_geom = sub.origin_geom,
		dest_geom = sub.dest_geom,
		dist_to_start = sub.dist_to_start,
		dist_to_end = sub.dist_to_end
FROM(
		SELECT
			id_new,
		  ST_ClosestPoint(n.geom_way, p.origin_geom) AS origin_geom,
		  ST_Distance(n.geom_way, p.origin_geom) AS dist_to_start,
		  ST_ClosestPoint(n.geom_way, p.dest_geom) AS dest_geom,
		  ST_Distance(n.geom_way, p.dest_geom) AS dist_to_end
		FROM
		  mapped_points p
		CROSS JOIN LATERAL
		  (SELECT id, geom_way
		   FROM ", char_network, "
		   ORDER BY geom_way <-> p.origin_geom
		   LIMIT 1
		  ) AS n
) sub
WHERE mp.id_new = sub.id_new;")
cat(query)
dbExecute(con, query)

# query <- paste0("SELECT
#     point.id as id,
#     line.id AS line_id,
#     ST_ClosestPoint(line.geom_way, point.geometry) AS closest_point_on_line,
#     ST_Distance(line.", name_geom_col,", point.geometry) AS distance_to_line,
#     ST_Distance(ST_StartPoint(line.line.geom_way), ST_ClosestPoint(line.geom_way, point.geometry)) AS distance_to_start,
#     ST_Distance(ST_EndPoint(line.geom_way), ST_ClosestPoint(line.geom_way, point.geometry)) AS distance_to_end
#   FROM ", char_name_trips, " AS point 
#   CROSS JOIN LATERAL
#     (SELECT id, geom_way FROM ",  char_network, "
#      ORDER BY geom_way <-> point.geometry
#      LIMIT 1) AS line;
#   ")
# dbExecute(con, query)