Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Network data
################################################################################
available_networks <- psql1_get_available_networks(con)
print(available_networks)
char_network <- available_networks[6, "table_name"]
dt_network <- st_read(con, char_network) %>% as.data.table
sf_network <- st_as_sf(dt_network)
ggplot() +
	geom_sf(data=sf_network) 


available_raw_trip_data <- psql1_get_raw_trip_data(con)
print(available_raw_trip_data)
char_data <- available_raw_trip_data[8, "table_name"]
df_trips <- st_read(con, char_data) 
sf_trips <- df_trips 

sf_origin <- st_as_sf(df_trips, 
											coords = c("start_lng", "start_lat"), 
											crs = 4326, 
											remove = FALSE) %>%
	rename(origin_geom = geometry)

# Convert end coordinates to sf object
sf_dest <- st_as_sf(df_trips, 
										coords = c("end_lng", "end_lat"), 
										crs = 4326, 
										remove = FALSE) %>%
	rename(dest_geom = geometry)



# Combine the origin and destination geometries into the original dataframe
sf_trips <- df_trips %>%
	bind_cols(sf_origin %>% select(origin_geom), sf_dest %>% select(dest_geom)) 

sf_trips$id_new <- 1:nrow(sf_trips)
# sf_trips$start_time <- as.character(sf_trips$start_time)
# sf_trips$end_time <- as.character(sf_trips$end_time)

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



char_area <- strsplit(char_network, "_")[[1]][2]

if(char_area != "complete"){
	char_data <- strsplit(char_data, "_")[[1]][1:2]
	char_data <- paste0(paste0(char_data, collapse = "_"),
											"_",
											char_area,
											"_mapped")
} else {
	char_data <- paste0(char_data, "_mapped")
}

#char_data <- "nb_dd_+berelbe_mapped"
st_write(sf_trips, con, char_data, delete_layer = TRUE)


psql1_create_spatial_index(con, char_data, "public")
psql1_create_spatial_index(con, char_network, "public")

int_crs <- 32632

psql1_map_od_points_onto_network(con, 
																 char_network, 
																 char_data,
																 crs = int_crs)



query <- paste0("ALTER TABLE ",  char_data,
								" ADD COLUMN line_geom geometry(LineString, ", int_crs, ");")
dbExecute(con, query)
query <- paste0("UPDATE ", char_data, " SET line_geom = ST_MakeLine(o_closest_point,
			 d_closest_point);")
dbExecute(con, query)

