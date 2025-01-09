Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")


reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")
hdbscan <- reticulate::import("hdbscan")
#################################################################################
# 1. Network data
################################################################################
available_networks <- psql1_get_available_networks(con)
print(available_networks)
char_network <- available_networks[1, "table_name"]
sf_network <- st_read(con, char_network) 
#################################################################################
# 2. Get original OD flow data based on subsets
################################################################################
subset <- c(15001, 20001)
char_schema <- paste0("data_", paste0(subset, collapse = "_"))
available_mapped_trip_data <- psql1_get_mapped_trip_data(con)
print(available_mapped_trip_data)
char_data <- available_mapped_trip_data[1, "table_name"]
sf_trips <- st_read(con, char_data) %>%
	rename("origin_id" = "id_edge_origin",
				 "dest_id" = "id_edge_dest") 

sf_trips <- sf_trips %>%
	arrange(start_datetime) %>%
	filter(trip_distance >= 100)

sf_trips_sub <- sf_trips %>%
	mutate(flow_id = 1:nrow(sf_trips)) %>%
	filter(flow_id >= min(subset) & flow_id <= max(subset))

matrix_pacmap <- np$load(here::here(path_python, 
																char_schema,
																"flow_manhattan_pts_euclid",
																"embedding_3d_1.npy"))
	
df_cluster_euclid <- py_hdbscan(np, hdbscan, matrix_pacmap, 30)
df_cluster_euclid$cluster %>% table()
matrix_pacmap <- np$load(here::here(path_python, 
																		char_schema,
																		"flow_manhattan_pts_network",
																		"embedding_3d_5.npy"))

df_cluster_network <- py_hdbscan(np, hdbscan, matrix_pacmap, 30)
df_cluster_network$cluster %>% unique
merged_df <- merge(df_cluster_euclid, 
									 df_cluster_network, 
									 by = "flow_id", 
									 suffixes = c("_euclid", "_network"))
cluster_matrix <- table(merged_df$cluster_euclid, merged_df$cluster_network)
mapping <- apply(cluster_matrix, 1, function(row) colnames(cluster_matrix)[which.max(row)])
mapping <- setNames(as.numeric(names(mapping)), mapping)
df_cluster_network$cluster_mapped <- ifelse(
	as.character(df_cluster_network$cluster) %in% names(mapping), # Wenn im Mapping vorhanden
	mapping[as.character(df_cluster_network$cluster)],           # Neue Zuordnung aus Mapping
	df_cluster_network$cluster                                   # Sonst Original-Label beibehalten
)
df_cluster_network$cluster %>% table()
df_cluster_network$cluster_mapped %>% table()
merged_df <- merge(df_cluster_euclid, 
									 df_cluster_network, 
									 by = "flow_id", 
									 suffixes = c("_euclid", "_network"))
cluster_matrix <- table(merged_df$cluster_euclid, merged_df$cluster_mapped)
cluster_matrix
result <- merged_df %>%
	mutate(movement = paste(cluster_euclid, "->", cluster_mapped)) %>%
	filter(cluster_euclid != cluster_mapped) %>%
	group_by(movement) %>%
	summarise(count = n(), flows = list(flow_id), .groups = "drop") 
print(result, n = 100)
int_row <- c(3, 5, 6)

list_plots <- show_moved_vs_border_plot(result = result,
													int_row = int_row,
													df_cluster_euclid = df_cluster_euclid,
													sf_trips_sub = sf_trips_sub,
													sf_network = sf_network,
													k = 100)

list_plots$pacmap_plotly
list_plots$od_pts_on_rd_network









euclid_cl <- strsplit(result[int_row,"movement"] %>% pull, " ")[[1]][1] %>% as.integer()
network_cl <- strsplit(result[int_row,"movement"] %>% pull, " ")[[1]][3] %>% as.integer()




sf_trips_sub$flow_id_temp <- 1:nrow(sf_trips_sub)
sf_trips_sub$cluster_euclid <- df_cluster_euclid$cluster
sf_trips_sub$cluster_net <- df_cluster_network$cluster_mapped


filtered_euclid <- sf_trips_sub %>% filter(cluster_euclid == euclid_cl)
filtered_network <- sf_trips_sub %>% filter(cluster_net == network_cl)

# ggplot erstellen
ggplot() +
	geom_sf(data = filtered_euclid, aes(geometry = o_closest_point, color = "Euclid Origin"), size = 1) +
	geom_sf(data = filtered_euclid, aes(geometry = d_closest_point, color = "Euclid Destination"), size = 1) +
	#geom_sf(data = filtered_euclid, aes(geometry = line_geom, color = "Euclid Destination"), size = .1) +
	geom_sf(data = filtered_network, aes(geometry = o_closest_point, color = "Network Origin"), size = 1) +
	geom_sf(data = filtered_network, aes(geometry = d_closest_point, color = "Network Destination"), size = 1) +
	#geom_sf(data = filtered_network, aes(geometry = line_geom, color = "Network OD flows"), size = 1) +
	geom_sf(data = sf_network, aes(geometry = geom_way), color = "gray", alpha = 0.5, size = 0.5) +
	# Farben definieren
	scale_color_manual(values = c(
		"Euclid Origin" = "darkseagreen4",
		"Euclid Destination" = "darkseagreen1",
		"Network Origin" = "red",
		"Network Destination" = "darkred"
	)) +
	# Plot-Labels und Titel
	labs(title = "Cluster Points for Euclid and Network",
			 color = "Point Type") +
	theme_minimal() 
	










query <- paste0("CREATE SCHEMA IF NOT EXISTS ", char_schema)
cat(query)
dbExecute(con, query)
# sf_trips_sub$cluster <- df_pacmap$cluster
st_write(sf_trips_sub, con, Id(schema=char_schema,
															 table = "data"))
