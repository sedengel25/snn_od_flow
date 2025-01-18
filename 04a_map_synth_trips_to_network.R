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


available_schemas <- psql1_get_schemas(con)
available_schemas
char_schema <- available_schemas[15, "schema_name"]



psql1_create_spatial_index(con, "data", char_schema)
psql1_create_spatial_index(con, char_network, "public")

int_crs <- 32632
char_data <- paste0(char_schema, ".data")
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





