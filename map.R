Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Network data
################################################################################
char_city <- "dd"
dt_network <- st_read(con, paste0(char_city,
																	"_2po_4pgr")) %>% as.data.table
sf_network <- st_as_sf(dt_network)
ggplot() +
	geom_sf(data=sf_network) 

trip_file <- read_delim("./data/SR_OD_Daten_Dresden.csv") %>% as.data.frame
head(trip_file)
sf_origin <- st_as_sf(trip_file, coords = c("start_lon", "start_lat"), crs = 4326, remove = FALSE) %>%
	rename(origin_geom = geometry)

# Convert end coordinates to sf object
sf_dest <- st_as_sf(trip_file, coords = c("end_lon", "end_lat"), crs = 4326, remove = FALSE) %>%
	rename(dest_geom = geometry)



# Combine the origin and destination geometries into the original dataframe
sf_trips <- trip_file %>%
	bind_cols(sf_origin %>% select(origin_geom), sf_dest %>% select(dest_geom)) 

sf_trips$id_new <- 1:nrow(sf_trips)
sf_trips$start_time <- as.character(sf_trips$start_time)
sf_trips$end_time <- as.character(sf_trips$end_time)

sf_trips <- sf_trips %>%
	mutate(origin_geom = st_transform(st_geometry(sf_trips$origin_geom), 32632),
				 dest_geom = st_transform(st_geometry(sf_trips$dest_geom), 32632))


sf_trips <- sf_trips %>%
	st_as_sf()


sf_bbox <- st_convex_hull(st_union(sf_network))


sf_trips <- sf_trips %>%
	filter(
		st_intersects(origin_geom, sf_bbox, sparse = FALSE) &
			st_intersects(dest_geom, sf_bbox, sparse = FALSE)
	)

plot(sf_trips$origin_geom)
plot(sf_trips$dest_geom)

char_prefix_data <- "sr"
char_trip_data <- paste0(char_prefix_data, "_", char_city)


st_write(sf_trips, con, char_trip_data, delete_layer = TRUE)

psql1_create_spatial_index(con, char_trip_data)
psql1_create_spatial_index(con, paste0(char_city, "_2po_4pgr"))
psql1_map_od_points_onto_network(con, paste0(char_city, "_2po_4pgr"), char_trip_data)


query <- paste0("ALTER TABLE ",  char_trip_data,
								" ADD COLUMN line_geom geometry(LineString, 32632);")
dbExecute(con, query)
query <- paste0("UPDATE ", char_trip_data, " SET line_geom = ST_MakeLine(o_closest_point,
			 d_closest_point);")
dbExecute(con, query)