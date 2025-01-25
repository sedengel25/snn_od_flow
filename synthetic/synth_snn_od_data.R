Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

create_clustered_sf <- function(char_cluster) {
  dt_cluster <- rbindlist(lapply(seq_along(char_cluster), function(i) {
    data.table(cluster_id = i, 
               id = as.integer(unlist(strsplit(char_cluster[[i]], ","))))
  }))
  dt_origin <- read.table(here::here(char_path, "merge_match_pointOs.txt"),
                          header = FALSE, sep = ",") %>%
    select(V4, V5) %>%
    rename("origin_X" = "V4", "origin_y" = "V5")
  dt_dest <- read.table(here::here(char_path, "merge_match_pointDs.txt"),
                          header = FALSE, sep = ",") %>%
    select(V4, V5) %>%
    rename("dest_X" = "V4", "dest_y" = "V5")
  
  
  dt_flows <- cbind(dt_origin, dt_dest)
  dt_flows$id <- 1:nrow(dt_flows)
  
  
  
  dt_flows_cluster <- dt_flows %>%
    left_join(dt_cluster, by = ("id" = "id")) %>%
    mutate(cluster_id = ifelse(is.na(cluster_id), 0, cluster_id))
  print("dt_cluster:")
  print(dt_cluster)
  print("dt_flows:")
  print(dt_flows)
  print("dt_flows_cluster:")
  print(dt_flows_cluster)
  
  origin_points <- st_as_sf(dt_flows_cluster, 
                            coords = c("origin_X", "origin_y"), 
                            crs = 32633) 
  dest_points <- st_as_sf(dt_flows_cluster, 
                          coords = c("dest_X", "dest_y"), 
                          crs = 32633)
  
  sf_cluster <- st_sf(
    id = origin_points$id,
    cluster_id = origin_points$cluster_id,
    geometry = mapply(function(orig, dest) st_cast(st_combine(c(orig, dest)), "LINESTRING"), 
                      origin_points$geometry, dest_points$geometry)
  )
  
  return(sf_cluster)
}

char_schema <- "beijing_synthetic_data"
query <- paste0("CREATE SCHEMA IF NOT EXISTS ", char_schema)
cat(query)
dbExecute(con, query)


char_path <- "/home/sebastiandengel/snn_flow_py/IJGIS-code&data(2020)/Synthetic data"
files <- list.files(here::here(char_path, "result"), pattern = "Clusters")
for(file in files){
  char_cluster <- readLines(here::here(char_path, "result", file))
  if(length(char_cluster) == 0){
    next()
  }
  sf_cluster <- create_clustered_sf(char_cluster)
  filename <- substr(file, 1, nchar(file) - 4)
  st_write(sf_cluster, con, Id(schema=char_schema, 
  												 table = filename))
  break
}



















char_schema <- "beijing_real_world_data"
query <- paste0("CREATE SCHEMA IF NOT EXISTS ", char_schema)
cat(query)
dbExecute(con, query)
char_path <- "/home/sebastiandengel/snn_flow_py/IJGIS-code&data(2020)/Beijing taxi data"
char_methods <- list.files(here::here(char_path, "Cluster results"))
char_datasets_origin <- list.files(char_path, pattern = "O.txt")
char_datasets_dest <- list.files(char_path, pattern = "D.txt")

for(method in char_methods){
	char_cluster <- list.files(here::here(char_path,  "Cluster results", method),
														 pattern = "Cluster")
	
	for(i in 1:length(char_cluster)){
		data_cluster <- fread(here::here(char_path,  
																 "Cluster results", 
																 method,
																 char_cluster[i]), 
											header = FALSE, 
											sep = "\n", 
											quote = "")
		
		dt_cluster <- rbindlist(
			lapply(seq_along(data_cluster$V1), function(i) {
				flow_ids <- as.integer(unlist(strsplit(data_cluster$V1[i], ","))) # Zahlen extrahieren
				data.table(cluster_id = i, flow_id = flow_ids) # Cluster-ID zuordnen
			})
		)

		print(paste0(method, "_",  char_cluster[i]))
		char_time <- substr(char_cluster[i], 1, nchar(char_cluster[i]) - 4)
		char_time <- strsplit(char_time, "_")[[1]][2]

		dt_origin <- read.table(here::here(char_path, 
																			 paste0(char_time, "_O.txt")),
														header = FALSE, sep = ",") %>%
			as.data.table()

		dt_dest <- read.table(here::here(char_path, 
																			 paste0(char_time, "_D.txt")),
														header = FALSE, sep = ",") %>%
			as.data.table()
		colnames(dt_origin) <- c("flow_id", "ox", "oy")
		colnames(dt_dest) <- c("flow_id", "dx", "dy")

		dt_dest <- dt_dest %>%
			select(-flow_id)
		dt_flows <- cbind(dt_origin, dt_dest)

		dt_flows_cluster <- dt_flows %>%
			left_join(dt_cluster, by = "flow_id") %>%
			mutate(cluster_id = ifelse(is.na(cluster_id), 0, cluster_id))
		print("dt_cluster:")
		print(dt_cluster)
		print("dt_flows:")
		print(dt_flows)
		print("dt_flows_cluster:")
		print(dt_flows_cluster)

		
		origin_points <- st_as_sf(dt_flows_cluster,
															coords = c("ox", "oy"),
															crs = 32633)

		dest_points <- st_as_sf(dt_flows_cluster,
														coords = c("dx", "dy"),
														crs = 32633)


		sf_cluster <- st_sf(
			id = origin_points$flow_id,
			cluster_id = origin_points$cluster_id,
			geometry = mapply(function(orig, dest) st_cast(st_combine(c(orig, dest)), "LINESTRING"),
												origin_points$geometry, dest_points$geometry))

		st_write(sf_cluster, con, Id(schema=char_schema,
																 table = paste0(method, "_", char_time)))

	}
}


n_clusters <- length(unique(sf_cluster$cluster_id))
colors <- c(rgb(211/255, 211/255, 211/255, 0.5), rainbow(n_clusters))
names(colors) <- c("0", as.character(1:n_clusters))

ggplot(data = sf_cluster) +
	# geom_sf(data=sf_network) +
	geom_sf(aes(color = as.character(cluster_id)), size = 1) +
	scale_color_manual(values = colors) +
	theme_minimal() +
	labs(color = "Cluster ID")
