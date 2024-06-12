################################################################################
# Source
################################################################################
source("./src/config.R")
source("./src/functions.R")
sourceCpp("./src/helper_functions.cpp")


################################################################################
# 3. Initiate OD points
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

char_city <- tolower(dt_trips[1, "city"] %>% as.character) %>% substr(1,3)

char_network <- paste0(char_city, "_2po_4pgr")

dt_dist_mat <- read_rds(paste0("./data/input/", char_city, "_dist_mat.rds"))

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

psql1_map_od_points_onto_network(con, char_network)


################################################################################
# 3. Calculate network distance between OD points
################################################################################
dt_origin <- dbReadTable(con, "mapped_points") %>%
	select(id_new, id_edge, o_dist_to_start, o_dist_to_end) %>%
	rename("dist_to_start" = "o_dist_to_start",
				 "dist_to_end" = "o_dist_to_end"
	) %>%
	as.data.table


dt_dest <- dbReadTable(con, "mapped_points") %>%
	select(id_new, id_edge, d_dist_to_start, d_dist_to_end) %>%
	rename("dist_to_start" = "d_dist_to_start",
				 "dist_to_end" = "d_dist_to_end"
	) %>%
	as.data.table


dt_network <- st_read(con, char_network) %>% 
	select(source, target, id) %>%
	as.data.table

dt_origin <- dt_origin[1:10000,]
int_n_od_pts <- nrow(dt_origin)

int_chunks <- r1_create_chunks(cores = int_cores, n = int_n_od_pts)

p1 <- proc.time()
dt_o_pts_nd <- parallel::mclapply(1:length(int_chunks), function(i) {
	id_start <- ifelse(i == 1, 1, int_chunks[i - 1] + 1)
	id_end <- int_chunks[i]
	
	cat("From ", id_start, " to ", id_end, "\n")
	
	process_networks(dt_od_pts_sub = dt_origin[id_start:id_end,],
									 dt_od_pts_full = dt_origin,
									 dt_network = dt_network,
									 dt_dist_mat = dt_dist_mat)
	
}, mc.cores = int_cores)
