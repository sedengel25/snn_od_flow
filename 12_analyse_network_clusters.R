Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Input data
################################################################################
available_data <- psql1_get_schemas(con)
available_data
char_schema <- available_data[2, "schema_name"]
available_network_cluster_tables <- psql1_get_tables_in_schema(con, char_schema)
available_network_cluster_tables
char_network_clusters <- available_network_cluster_tables[1, "table_name"]
sf_network_clusters <- st_read(
	con,
	query = paste0("SELECT * FROM ", 
								 char_schema, 
								 ".", 
								 char_network_clusters)
)



available_network_tables <- psql1_get_available_networks(con)
print(available_network_tables)
char_network <- available_network_tables[2, "table_name"]
sf_network <- st_read(con, char_network)




path <- "/home/sebastiandengel/dx/Dresden/"
folders <- list.dirs(path, recursive = FALSE)
files <- list.files(folders[1])
dt_nb <- read_rds(here::here(folders[1], files[1])) %>% as.data.table


sf_nb <- st_as_sf(
	dt_nb,
	coords = c("lng", "lat"), 
	crs = 4326,               
	remove = FALSE
)

sf_nb <- st_transform(sf_nb, crs = 32632)




# Write rds-file data as psql-table to database
char_nb_timestamp_data <- "nb_timestamp_data"
st_write(sf_nb, con, char_nb_timestamp_data, delete_layer = TRUE)

# Create convex hull for network...
query <- paste0("DROP TABLE IF EXISTS ",  char_schema, ".", char_network,"_convex_hull")
dbExecute(con, query)
print(char_network_clusters)
query <- paste0("CREATE TABLE ", char_schema, ".", char_network, "_convex_hull AS
SELECT ST_Transform(ST_ConvexHull(ST_Union(geom_way)), 32632) AS geom_convex_hull
FROM ", char_schema, ".", char_network_clusters,";")
dbExecute(con, query)

# ...update the geometry...
query <- paste0("UPDATE ",  char_schema, ".", char_network,"_convex_hull
SET geom_convex_hull = ST_SetSRID(geom_convex_hull, 32632)
WHERE ST_SRID(geom_convex_hull) = 0;")
cat(query)
dbExecute(con, query)


#... and filter which points are within the convex_hull
query <- paste0("DROP TABLE IF EXISTS ",  char_nb_timestamp_data, "_filtered;")
dbExecute(con, query)

query <- paste0("CREATE TABLE ", char_nb_timestamp_data, "_filtered AS
    SELECT * FROM ", char_nb_timestamp_data, " WHERE ST_Within(
        geometry,
        (SELECT geom_convex_hull FROM ",  char_schema, ".", 
        char_network, "_convex_hull)
    );
")
dbExecute(con, query)


psql1_create_spatial_index(con, paste0(char_nb_timestamp_data, "_filtered"))
psql1_create_spatial_index(con, paste0(char_schema, ".", char_network_clusters))

### Find a way to make much shorter names such that 'psql1_create_spatial_index' works every time!!!
query <- paste0("SELECT ",
								char_nb_timestamp_data, "_filtered.*,
								n.id AS id_edge, 
								n.cluster_pred,
								ST_ClosestPoint(n.geom_way, ",  char_nb_timestamp_data, 
								"_filtered.geometry) AS snapped_geom 
								FROM ", char_nb_timestamp_data, "_filtered
								CROSS JOIN LATERAL (
								SELECT
								id,
								cluster_pred,
								geom_way
								FROM ", char_network_clusters, " AS n
								ORDER BY
								geom_way <-> ", char_nb_timestamp_data, "_filtered.geometry
								LIMIT 1) AS n;")


