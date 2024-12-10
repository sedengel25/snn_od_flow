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
folders <- list.dirs(path, recursive = FALSE)

################################################################################
# 2. Prepare PSQl tables
################################################################################
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
stop("Sure you want to remove the time series table?")
dbExecute(con, query)



int_skip <- 5


################################################################################
# 3. Main
################################################################################

all_selected_files <- lapply(folders, function(folder) {
	files <- list.files(folder, full.names = TRUE)  
	files <- sort(files)                        
	files[seq(1, length(files), int_skip)]        
})


all_selected_files <- unlist(all_selected_files)

batch_size <- ceiling(length(all_selected_files) / int_cores)  
file_batches <- split(all_selected_files, 
											rep(1:int_cores, 
													each = batch_size, 
													length.out = length(all_selected_files)))

t1 <- proc.time()

mclapply(file_batches, 
				 main_map_od_points_on_cluster_networks, 
				 mc.cores = int_cores)

t2 <- proc.time()

print(t2 - t1)




