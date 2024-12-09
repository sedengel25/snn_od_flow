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
char_network_clusters <- "cluster_sub_networks" # available_network_cluster_tables[1, "table_name"]
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
char_network_clusters <- "cluster_sub_networks" # available_network_cluster_tables[1, "table_name"]
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

folders <- folders[1:14]
# Create convex hull for network...
query <- paste0("DROP TABLE IF EXISTS ",  char_schema, ".", char_network,"_convex_hull")
dbExecute(con, query)
query <- paste0("CREATE TABLE ", char_schema, ".", char_network, "_convex_hull AS
		SELECT ST_Transform(ST_ConvexHull(ST_Union(geom_way)), 32632) AS geom_convex_hull
		FROM ", char_schema, ".", char_network_clusters,";")
cat(query)
dbExecute(con, query)

# ...update the geometry...
query <- paste0("UPDATE ",  char_schema, ".", char_network,"_convex_hull
		SET geom_convex_hull = ST_SetSRID(geom_convex_hull, 32632)
		WHERE ST_SRID(geom_convex_hull) = 0;")
dbExecute(con, query)


psql1_create_spatial_index(con, 
													 char_network_clusters,
													 char_schema)

query <- "DROP TABLE IF EXISTS temp_nb_timestamp_data;"
dbExecute(con, query)
query <- paste0("
  CREATE TABLE temp_nb_timestamp_data (
  	time_stamp TIMESTAMP,
    lng DOUBLE PRECISION,
    lat DOUBLE PRECISION,
    geometry GEOMETRY(POINT, 32632)
  );
")
dbExecute(con, query)

query <- "DROP TABLE IF EXISTS temp_nb_timestamp_data_filtered;"
dbExecute(con, query)
query <- paste0("
  CREATE TABLE temp_nb_timestamp_data_filtered (
    time_stamp TIMESTAMP,
    lng DOUBLE PRECISION,
    lat DOUBLE PRECISION,
    geometry GEOMETRY(POINT, 32632)
  );
")
dbExecute(con, query)

query <- paste0("DROP TABLE IF EXISTS ", 
								char_schema, ".time_series;")
dbExecute(con, query)

query <- paste0("
  CREATE TABLE ", char_schema, ".time_series (
    timestamp TIMESTAMP,
    cluster INTEGER,
    count INTEGER
  );
")
dbExecute(con, query)


int_skip <- 5


process_files <- function(folder) {
	
	con <- dbConnect(RPostgres::Postgres(),
									 dbname = dbname,
									 host = host,
									 user = user,
									 password = pw,
									 port = port,
									 options="-c client_min_messages=warning")
	
	
	files <- list.files(folder)
	for(i in seq(1, length(files), int_skip)){
		

		
		
		
		file <- files[i]
		dt_nb <- read_rds(here::here(folder, file)) %>% as.data.table
		posixct_ts <- dt_nb[1, "time_stamp"]
		dt_nb <- dt_nb[, .(time_stamp, lat, lng)]
		
		#print(dt_nb[1,1])
		sf_nb <- st_as_sf(
			dt_nb,
			coords = c("lng", "lat"), 
			crs = 4326,               
			remove = FALSE
		)
		
		sf_nb <- st_transform(sf_nb, crs = 32632) 
		
		
		# Write rds-file data as psql-table to database
		dbExecute(con, "TRUNCATE TABLE temp_nb_timestamp_data;")
		dbWriteTable(
			con, 
			"temp_nb_timestamp_data", 
			sf_nb, 
			append = TRUE, 
			row.names = FALSE
		)
		
		
		#... and filter which points are within the convex_hull
		dbExecute(con, "TRUNCATE TABLE temp_nb_timestamp_data_filtered;")
		
		query <- paste0("
        INSERT INTO temp_nb_timestamp_data_filtered
        SELECT * FROM temp_nb_timestamp_data
        WHERE ST_Within(
          geometry,
          (SELECT geom_convex_hull FROM ", char_schema, ".", char_network, "_convex_hull)
        );
      ")
		dbExecute(con, query)
		
		
		# Create spatial index so that the following operation is faster
		dbExecute(con, 
							"CREATE INDEX ON temp_nb_timestamp_data_filtered USING GIST (geometry);")
		
		
		# Map points from temp-table (for each timestamp) on to the cluster networks
		# count the appearances for each clsuter and insert the values into 
		# the time series table
		query <- paste0("INSERT INTO ", char_schema, ".time_series (timestamp, cluster, count)
  		  SELECT 
  		    time_stamp AS timestamp, 
  		    n.cluster_pred AS cluster,
  		    COUNT(*) AS count
  		  FROM temp_nb_timestamp_data_filtered
  		  CROSS JOIN LATERAL (
  		    SELECT
  		      id,
  		      cluster_pred,
  		      geom_way
  		    FROM ", char_schema, ".", char_network_clusters, " AS n
  		    ORDER BY
  		      geom_way <-> temp_nb_timestamp_data_filtered.geometry
  		    LIMIT 1
  		  ) AS n
  		  GROUP BY time_stamp, n.cluster_pred;")
		dbExecute(con, query)
	}
}


t1 <- proc.time()
pbmclapply(folders, process_files, mc.cores = 14)
t2 <- proc.time()

print(t2-t1)


