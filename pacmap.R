Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Network data
################################################################################
available_networks <- psql1_get_available_networks(con)
print(available_networks)
char_network <- available_networks[2, "table_name"]
dt_network <- st_read(con, char_network) %>% as.data.table
sf_network <- st_as_sf(dt_network)
ggplot() +
	geom_sf(data=sf_network) 

as_sfnetwork(sf_network)

char_path_dt_dist_mat <- here::here("data", "input", "dt_dist_mat")
char_av_dt_dist_mat_files <- list.files(char_path_dt_dist_mat)
print(char_av_dt_dist_mat_files)
# stop("Have you chosen the right dist mat?")
char_dt_dist_mat <-  char_av_dt_dist_mat_files[20]
char_buffer <- "50000"
dt_dist_mat <- read_rds(here::here(
	char_path_dt_dist_mat,
	char_dt_dist_mat))


# m-Spalte runden und in Integer umwandeln
dt_dist_mat <- dt_dist_mat %>%
	mutate(m = round(m, 0) %>% as.integer())


################################################################################
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
# 2. Calculate euclidean distances between all OD flows 
################################################################################



# Create folder for pacmap-file depending on the OD flow subset
if (!dir.exists(path_pacmap)) {
	dir.create(path_pacmap, recursive = TRUE, mode = "0777") 
	message("Directory created with full permissions: ", path_pacmap)
} else {
	message("Directory already exists: ", path_pacmap)
}

# Calculate the euclidean distance between all OD flows
main_euclid_dist_mat_cpu(char_schema = char_schema,
															 char_trips = "data",
															 n = nrow(sf_trips_sub),
															 cores = int_cores)

# Create an index on flow_id_i
query <- paste0("DROP INDEX IF EXISTS ",
								char_schema, ".idx_flow_id_i")

dbExecute(con, query)

query <- paste0("CREATE INDEX idx_flow_id_i ON ",
								char_schema, ".euclid_dist_sum (flow_id_i);")
dbExecute(con, query)

# Write from psql-table into json-files 
main_psql_dist_mat_to_matrix(char_schema = char_schema,
														 n = nrow(sf_trips_sub),
														 cores = int_cores)



# Turn json-files into symmetric matrix as numpy array
system2("python3", args = c(here::here(path_python,
																			 "read_json_to_npy.py"),
														"--directory", path_pacmap),
				stdout = "", stderr = "")

# Create PaCMAP from numpy array
system2("python3", args = c(here::here(path_python,
																			 "pacmap_cpu.py"),
														"--directory", path_pacmap,
														"--distance euclid --n", nrow(sf_trips_sub)),
				stdout = "", stderr = "")



################################################################################
# 3. Calculate network distances between OD flows and put it into a matrix
################################################################################
gc()
dt_pts_nd <- main_nd_dist_mat_ram(sf_trips_sub, dt_network, dt_dist_mat)
rm(dt_dist_mat)
gc()
dt_o_pts_nd <- dt_pts_nd$dt_o_pts_nd %>% as.data.table()
dt_d_pts_nd <- dt_pts_nd$dt_d_pts_nd %>% as.data.table()
char_buffer



rm(dt_pts_nd)
gc()

dt_flow_nd <- merge(dt_o_pts_nd, dt_d_pts_nd, by = c("from", "to"))
rm(dt_o_pts_nd)
rm(dt_d_pts_nd)
gc()
dt_flow_nd[, distance := distance.x + distance.y]
dt_flow_nd <- dt_flow_nd[, .(flow_m = from, flow_n = to, distance)]


dt_sym <- rbind(
	dt_flow_nd,
	dt_flow_nd[, .(flow_m = flow_n, flow_n = flow_m, distance = distance)]
)
rm(dt_flow_nd)
gc()

dt_flow_nd <- dt_sym
rm(dt_sym)
gc()

dt_flow_nd <- dt_flow_nd %>%
	rename(from = flow_m,
				 to = flow_n)



gc()
matrix_flow_nd <- matrix(99999, nrow = nrow(sf_trips_sub), ncol = nrow(sf_trips_sub))
matrix_flow_nd[cbind(dt_flow_nd$from, dt_flow_nd$to)] <- dt_flow_nd$distance
matrix_flow_nd[cbind(dt_flow_nd$to, dt_flow_nd$from)] <- dt_flow_nd$distance
gc()

reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")
np_matrix_flow_nd <- np$array(matrix_flow_nd)
np$save(here::here(path_pacmap, "nd_mat.npy"),
				np_matrix_flow_nd)

system2("python3", args = c(here::here(path_python,
																			 "pacmap_ram.py"),
														"--directory", path_pacmap,
														"--distance nd"),
				stdout = "", stderr = "")
################################################################################
# 4. PaCMAP
################################################################################
df_euclid_embedding <- load_pacmap_embedding(path_pacmap, "euclid", np)
df_nd_embedding <- load_pacmap_embedding(path_pacmap, "nd", np)

ggplot(df_euclid_embedding, aes(x = x, y = y)) +
	geom_point(alpha = 0.1) +  
	stat_density2d(aes(fill =after_stat(level)),
								 geom = "polygon",
								 alpha = 1,
								 adjust = 0.1,
								 n = 150) +
	scale_fill_viridis_c(option = "plasma") +  
	theme_minimal() +
	labs(
		title = "PACMAP for euclidean distance",
		x = "x",
		y = "y",
		fill = "Density"
	)

ggplot(df_nd_embedding, aes(x = x, y = y)) +
	geom_point(alpha = 0.1) +  
	stat_density2d(aes(fill =after_stat(level)),
								 geom = "polygon",
								 alpha = 1,
								 adjust = 0.1,
								 n = 150) +
	scale_fill_viridis_c(option = "plasma") +  
	theme_minimal() +
	labs(
		title = "PACMAP for network distance",
		x = "x",
		y = "y",
		fill = "Density"
	)

reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")
hdbscan <- import("hdbscan")



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



list_py_hdbscan <- py_hdbscan_dbcv(np = np,
								df_euclid_embedding = df_euclid_embedding,
								df_nd_embedding = df_nd_embedding,
								minpts = 10)
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
