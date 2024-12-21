Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Network data
################################################################################
available_networks <- psql1_get_available_networks(con)
print(available_networks)
char_network <- available_networks[2, "table_name"]
# dt_network <- st_read(con, char_network) %>% as.data.table
# sf_network <- st_as_sf(dt_network)
# ggplot() +
# 	geom_sf(data=sf_network)

char_dist_mat <- "dd_center_50000_dist_mat"


#################################################################################
# 2. OD flow data
################################################################################
available_mapped_trip_data <- psql1_get_mapped_trip_data(con)
print(available_mapped_trip_data)
char_data <- available_mapped_trip_data[2, "table_name"]
sf_trips <- st_read(con, char_data) %>%
	rename("origin_id" = "id_edge_origin",
				 "dest_id" = "id_edge_dest")

char_data <- substr(char_data, 1, nchar(char_data) - 7)
sf_trips$month <- lubridate::month(sf_trips$start_datetime)
sf_trips$week <- lubridate::week(sf_trips$start_datetime)
sf_trips$hour <- lubridate::hour(sf_trips$start_datetime)
sf_trips$weekday <- lubridate::wday(sf_trips$start_datetime, week_start = 1)
sf_trips <- sf_trips %>%
	arrange(start_datetime)
dist_filter <- 1000
#int_kw <- c(9:11)
int_wday <- c(1:4)
int_hours <- c(5:9)
sf_trips_sub <- sf_trips %>%
	#filter(week %in% int_kw) %>%
	filter(trip_distance >= dist_filter) %>%
	filter(hour %in% int_hours) %>%
	filter(weekday %in% int_wday) %>% 
	mutate(origin_id = as.integer(origin_id),
				 dest_id = as.integer(dest_id))
sf_trips_sub$flow_id <- 1:nrow(sf_trips_sub)
rm(sf_trips)
gc()

char_schema <- paste0(char_data, 
											"_min", 
											dist_filter,
											"_hours",
											paste0(int_hours, collapse = "_"),
											"_wdays",
											paste0(int_wday, collapse = "_"))

psql1_create_schema(con, char_schema)


st_write(sf_trips_sub, con, Id(schema=char_schema, 
															 table = "data"))
path_pacmap <- here::here(path_python, char_schema)
################################################################################
# 2. Calculate different distance measures between all OD flows 
################################################################################
# Calculate the euclidean distance between all OD flows
main_calc_diff_flow_distances(char_schema = char_schema,
															char_trips = "data",
															n = nrow(sf_trips_sub),
															cores = int_cores)

# Create an index on flow_id_i
query <- paste0("DROP INDEX IF EXISTS ", char_schema, ".flow_distances_flow_id_i_flow_id_j_idx;")
dbExecute(con, query)

query <- paste0("CREATE INDEX ON ",
								char_schema, ".flow_distances (flow_id_i, flow_id_j);")
dbExecute(con, query)

main_nd_dist_mat_cpu(char_schema = char_schema,
										 char_network = char_network,
										 char_dist_mat = char_dist_mat,
										 n = nrow(sf_trips_sub),
										 cores = int_cores)

char_dist_measures <- c("flow_manhattan_pts_euclid",
												"flow_chebyshev_pts_euclid",
												"flow_euclid",
												"flow_euclid_norm",
												"flow_manhatten_pts_network")
#sudo chown -R postgres /home/sebastiandengel/snn_flow/python --> run in terminal

for(dist_measure in char_dist_measures[5]){
	folder <- here::here(path_pacmap, dist_measure)
	# Create folder for pacmap-file depending on the OD flow subset
	if (!dir.exists(folder)) {
		dir.create(folder, recursive = TRUE, mode = "0777") 
		message("Directory created with full permissions: ", folder)
	} else {
		message("Directory already exists: ", folder)
	}
	
	# To make this work you need to allow your user to execute
	# chownd-commands without a password
	system2("/usr/bin/sudo", c("chown", "-R", "postgres", folder))
	
	# Write from psql-table into json-files 
	main_psql_dist_mat_to_matrix(char_schema = char_schema,
															 dist_measure = dist_measure,
															 n = nrow(sf_trips_sub),
															 cores = int_cores)


	Sys.sleep(10)
	# Turn json-files into symmetric matrix as numpy array
	system2("python3", args = c(here::here(path_python,
																				 "read_json_to_npy.py"),
															"--directory", folder,
															"--distance", dist_measure),
					stdout = "", stderr = "")
	
	# Create PaCMAP from numpy array
	system2("python3", args = c(here::here(path_python,
																				 "pacmap_cpu.py"),
															"--directory ", folder,
															"--distance ", dist_measure, " --n ", nrow(sf_trips_sub)),
					stdout = "", stderr = "")

}



################################################################################
# 3. PaCMAP
################################################################################
reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")
hdbscan <- import("hdbscan")

df_1_embedding <- load_pacmap_embedding(path_pacmap, char_dist_measures[1], np)
df_2_embedding <- load_pacmap_embedding(path_pacmap, char_dist_measures[2], np)
df_3_embedding <- load_pacmap_embedding(path_pacmap, char_dist_measures[3], np)
df_4_embedding <- load_pacmap_embedding(path_pacmap, char_dist_measures[4], np)

pacmap_density_plot(df_1_embedding)
pacmap_density_plot(df_2_embedding)
pacmap_density_plot(df_3_embedding)
pacmap_density_plot(df_4_embedding)

int_minpts <- 10
list_embedding_1 <- py_hdbscan_dbcv(np = np, 
								df_embedding = df_1_embedding,
								minpts = int_minpts)
list_embedding_1
list_embedding_2 <- py_hdbscan_dbcv(np = np, 
																		df_embedding = df_2_embedding,
																		minpts = int_minpts)
list_embedding_2
list_embedding_3 <- py_hdbscan_dbcv(np = np, 
																		df_embedding = df_3_embedding,
																		minpts = int_minpts)
list_embedding_3
list_embedding_4 <- py_hdbscan_dbcv(np = np, 
																		df_embedding = df_4_embedding,
																		minpts = int_minpts)
list_embedding_4

# int_k <- 12
# int_eps <- 6
# int_minpts <- 10
# hdbcsan_res_euclid <- dbscan::hdbscan(df_euclid_embedding[,c(1:2)], minPts = int_minpts)
# hdbcsan_res_nd <- dbscan::hdbscan(df_nd_embedding[,c(1:2)], minPts = int_minpts)
# snn_res_euclid <- dbscan::sNNclust(df_euclid_embedding[,c(1:2)], 
# 																	 k = int_k,
# 																	 eps = int_eps,
# 																	 minPts = int_minpts)
# 
# snn_res_nd <- dbscan::sNNclust(df_nd_embedding[,c(1:2)], 
# 															 k = int_k,
# 															 eps = int_eps,
# 															 minPts = int_minpts)




list_py_hdbscan$dbcv_euclid
list_py_hdbscan$dbcv_nd

py_get_dbcv(np,
						df_euclid_embedding,
						df_nd_embedding,
						cluster_labels_euclid = hdbcsan_res_euclid$cluster,
						cluster_labels_nd = hdbcsan_res_nd$cluster)





create_pacmap_plotly_with_clusters(list_py_hdbscan$df_cluster_euclid)
create_pacmap_plotly_with_clusters(list_py_hdbscan$df_cluster_nd)
################################################################################
# 5. Add cluster to PaCMAP
################################################################################
av_schemas <- psql1_get_schemas(con)
char_schema <- av_schemas[4, "schema_name"]
av_schema_tables <- psql1_get_tables_in_schema(con, char_schema)
char_data <- av_schema_tables[3, "table_name"]

sf_snn <- st_read(
	con,
	query = paste0("SELECT * FROM ",
								 char_schema,
								 ".",
								 char_data)
) 
df_snn <- sf_snn %>% as.data.frame()

if (nrow(df_snn) != nrow(df_embedding)){
	stop("Embedding and clsuter dataset have different dimensions")
}



################################################################################
# 5. CDBW
################################################################################
# df_snn_cdbw <- st_coordinates(sf_snn) %>% as.data.frame()
# df_snn_cdbw <- df_snn_cdbw %>%
# 	mutate(row_id = row_number(), .by = L1) %>%   # Zeilen-ID pro Gruppe
# 	pivot_wider(
# 		names_from = row_id,
# 		values_from = c(X, Y),
# 		names_prefix = ""
# 	) %>%
# 	rename(ox = X_1, oy = Y_1, dx = X_2, dy = Y_2) %>%
# 	as.data.frame() %>%
# 	rename(flow_id = L1)
# 
# df_snn_cdbw$cluster_pred <- df_snn$cluster_pred
# 
# cdbv_idx_euclid <- cdbw(x = df_snn_cdbw[,c(2:5)],
# 												clustering = df_snn_cdbw[,6])
# cdbv_idx_euclid
# df_pacmap <- df_embedding %>%
# 	left_join(df_snn %>% select(flow_id, cluster_pred),
# 						by = c("id" = "flow_id"))
# 
# 
# cdbv_idx_pacmap <- cdbw(x = df_pacmap[,c(1,2)],
# 												clustering = df_pacmap[,4])
# 
# cdbv_idx_pacmap

################################################################################
# 6. Clustering of PaCMAP dimension reduced data
################################################################################
int_k <- 14
int_eps <- 7
int_minpts <- 9
df_pacmap_snn <- dbscan::sNNclust(df_embedding[,1:2], 
				 k = int_k,
				 eps = int_eps,
				 minPts = int_minpts)

df_pacmap_snn$cluster %>% unique %>% length

df_pacmap$cluster_pred_snn_pacmap <- df_pacmap_snn$cluster

p <- ggplot(df_pacmap 
						%>% filter(cluster_pred_snn_pacmap != 0)
						, aes(x = x, y = y, color = factor(cluster_pred_snn_pacmap))) +
	geom_point(size = 1, alpha = 0.3) +
	scale_color_manual(values = random_colors) +  # Verwende scale_color_manual
	labs(
		title = "PaCMAP by Clusters",
		x = "x",
		y = "y"
	) +
	theme_minimal() +
	theme(
		legend.position = "none",
		plot.title = element_text(hjust = 0.5, size = 16),
		axis.title = element_text(size = 12)
	)


# Interaktives Plotly-Plot
ggplotly(p, tooltip = c("x", "y", "color"))
cdbw(x = df_pacmap[,c(1,2)],
		 clustering = df_pacmap[,5])



sf_pacmap_snn <- df_snn %>%
	left_join(df_pacmap %>% select(id, cluster_pred_snn_pacmap), by = c("flow_id" = "id")) %>%
	st_as_sf()

char_table <- paste0("pacmap_snn_k",
										 int_k,
										 "_eps",
										 int_eps,
										 "_minpts",
										 int_minpts)


st_write(sf_pacmap_snn, con, Id(schema=char_schema, 
												 table = char_table))
