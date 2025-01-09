Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Network data
################################################################################
available_networks <- psql1_get_available_networks(con)
print(available_networks)
char_network <- available_networks[3, "table_name"]
dt_network <- st_read(con, char_network) %>% as.data.table
sf_network <- st_as_sf(dt_network)
ggplot() +
	geom_sf(data=sf_network) 

as_sfnetwork(sf_network)

char_path_dt_dist_mat <- here::here("data", "input", "dt_dist_mat")
char_av_dt_dist_mat_files <- list.files(char_path_dt_dist_mat)
print(char_av_dt_dist_mat_files)
# stop("Have you chosen the right dist mat?")
char_dt_dist_mat <-  char_av_dt_dist_mat_files[16]
char_buffer <- "50000"
# dt_dist_mat <- read_rds(here::here(
# 	char_path_dt_dist_mat,
# 	char_dt_dist_mat))


# m-Spalte runden und in Integer umwandeln
# dt_dist_mat <- dt_dist_mat %>%
# 	mutate(m = round(m, 0) %>% as.integer())
char_dist_mat <- paste0("dd_",
												char_buffer,
												"_dist_mat")
# dbWriteTable(con, char_dist_mat, value = dt_dist_mat)
# rm(dt_dist_mat)
# gc()
# query <- paste0("CREATE INDEX idx_char_dist_mat_source_target ON ",
# 								char_dist_mat, " (source, target);")
# 
# dbExecute(con, query)
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
dist_filter <- 200
int_kw <- c(8:11)
int_wday <- c(1:3)
#int_hours <- c(16:18)
sf_trips_sub <- sf_trips %>%
	filter(week %in% int_kw) %>%
	filter(trip_distance >= dist_filter) %>%
	#filter(hour %in% int_hours) 
	#%>%
	filter(weekday %in% int_wday)
n_samples <- nrow(sf_trips_sub)
print(n_samples)
rm(sf_trips)
gc()




sf_trips_sub$flow_id <- 1:nrow(sf_trips_sub)

sf_trips_sub <- sf_trips_sub %>% mutate(origin_id = as.integer(origin_id),
																				dest_id = as.integer(dest_id))





t_start <- proc.time()

char_schema <- paste0(char_data, 
											"_min", 
											dist_filter,
											"m",
											"_kw",
											paste0(int_kw, collapse = "_"),
											# "_hours",
											# paste0(int_hours, collapse = "_"),
											"_wdays",
											paste0(int_wday, collapse = "_"),
											"m_buffer",
											char_buffer,
											"m")

query <- paste0("CREATE SCHEMA IF NOT EXISTS ", char_schema)
cat(query)
dbExecute(con, query)


st_write(sf_trips_sub, con, Id(schema=char_schema, 
															 table = "data"))





################################################################################
# 3. Calculation of distance matrices
################################################################################
reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")
hdbscan <- reticulate::import("hdbscan")




### Calculate euclidean based distances --------------------------------------
main_calc_diff_flow_distances(char_schema = char_schema,
															n = nrow(sf_trips_sub),
															cores = int_cores)

query <- paste0("DROP INDEX IF EXISTS ", 
								char_schema, 
								".flow_distances_flow_id_i_flow_id_j;")
dbExecute(con, query)

query <- paste0("CREATE INDEX ON ",
								char_schema, ".flow_distances (flow_id_i, flow_id_j);")
dbExecute(con, query)


### Calculate network based distances ----------------------------------------
main_nd_dist_mat_cpu(char_schema = char_schema,
										 char_network = char_network,
										 char_dist_mat = char_dist_mat,
										 n = nrow(sf_trips_sub),
										 cores = int_cores)

# matrix_flow_nd <- main_nd_dist_mat_ram(sf_trips_sub, dt_network, dt_dist_mat)
# rm(dt_dist_mat)
# gc()
# matrix_flow_nd[1:5,1:5]
path_pacmap <- here::here(path_python, char_schema)
# dist_measure <- "flow_manhattan_pts_network"
# folder <- here::here(path_pacmap, dist_measure)
# if (!dir.exists(folder)) {
# 	dir.create(folder, recursive = TRUE, mode = "0777")
# 	message("Directory created with full permissions: ", folder)
# } else {
# 	message("Directory already exists: ", folder)
# }
# npy_file <- here::here(path_pacmap, dist_measure, "dist_mat.npy")
# np$save(npy_file, np$array(matrix_flow_nd, dtype = "int32"))
# rm(matrix_flow_nd)
# gc()
char_dist_measures <- c("flow_manhattan_pts_euclid",
												"flow_manhattan_pts_network")


################################################################################
# 4. Converting of distance matrices into numpy arrays
################################################################################
for(dist_measure in char_dist_measures[2]){
	folder <- here::here(path_pacmap, dist_measure)
	if (!dir.exists(folder)) {
		dir.create(folder, recursive = TRUE, mode = "0777")
		message("Directory created with full permissions: ", folder)
	} else {
		message("Directory already exists: ", folder)
	}

	system2("/usr/bin/sudo", c("chown", "-R", "postgres", folder))
	main_psql_dist_mat_to_matrix(char_schema = char_schema,
															 dist_measure = dist_measure,
															 n = nrow(sf_trips_sub),
															 cores = int_cores)
	Sys.sleep(3)
	system2("python3", args = c(here::here(path_python,
																				 "read_json_to_npy.py"),
															"--directory", folder,
															"--distance", dist_measure),
					stdout = "", stderr = "")
	

}

################################################################################
# 5. Creating 23, 3d, 4d pacmap-embeddings
################################################################################

for(dist_measure in char_dist_measures){
	folder <- here::here(path_pacmap, dist_measure)
	for(i in 1:3){
		system2("python3", args = c(here::here(path_python,
																					 "pacmap_cpu.py"),
																"--directory ", folder,
																"--distance ", dist_measure,
																"--n ", n_samples,
																"--i ", i),
						stdout = "", stderr = "")
		
		# if(dist_measure != "flow_manhattan_pts_network"){
		# 	system2("python3", args = c(here::here(path_python,
		# 																				 "pacmap_cpu.py"),
		# 															"--directory ", folder,
		# 															"--distance ", dist_measure,
		# 															"--n ", n_samples,
		# 															"--i ", i),
		# 					stdout = "", stderr = "")
		# } else {
		# 	system2("python3", args = c(here::here(path_python,
		# 																				 "pacmap_ram.py"),
		# 															"--directory ", folder,
		# 															"--distance ", dist_measure,
		# 															"--i ", i),
		# 					stdout = "", stderr = "")
		# }
	}
}


################################################################################
# PaCMAP plots
################################################################################
psql1_get_schemas(con)
char_schema <- "nb_dd_min250m_kw9_wdays1_2_3_4m_buffer50000m"
path_pacmap <- here::here(path_python, char_schema)
reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")
hdbscan <- reticulate::import("hdbscan")

df_pacmap <- np$load(here::here(path_pacmap, 
																"flow_manhattan_pts_network", 
																"embedding_3d_1.npy")) %>%
	as.data.frame() %>%
	rename("x" = "V1", "y" = "V2", "z" = "V3")

# np_dist_mat <-  np$load(here::here(path_pacmap,
# 																	 "flow_manhattan_pts_network",
# 																	 "dist_mat.npy"))
# np_dist_mat[1:5, 1:5]
# class(np_dist_mat)
# k <- 25
# minpts <- 12
# eps <- 13
# snn_res <- sNNclust(np_dist_mat, k, eps, minpts)
# rm(np_dist_mat)
# gc()

df_pacmap <- py_hdbscan(np, hdbscan, df_pacmap, 40)
# kmeans_res <- kmeans(df_pacmap, centers = 4, nstart = 100)
# df_pacmap$cluster <- kmeans_res$cluster
table(df_pacmap$cluster)

plot_ly(
	data = df_pacmap %>% filter(cluster!=0), 
	x = ~x, 
	y = ~y, 
	z = ~z, 
	color = ~factor(cluster), # Cluster einfärben
	type = "scatter3d", 
	mode = "markers", 
	marker = list(
		size = 4, 
		opacity = 0.25 # Transparenz der Punkte
	),
	text = ~as.character(paste0("Flow -", flow_id)), # Text für Hover-Effekt (ID)
	hoverinfo = "text" # Nur den Text anzeigen (ID)
) %>%
	layout(
		scene = list(
			xaxis = list(title = "x"),
			yaxis = list(title = "y"),
			zaxis = list(title = "z")
		)
	)

sf_trips_sub$cluster_pred <- df_pacmap$cluster
st_write(sf_trips_sub, con, Id(schema=char_schema, 
															 table = "hdbscan_minpts35"))


df_pacmap_sub <- df_pacmap %>%
	filter(cluster==2)



df_pacmap_sub <- py_hdbscan(np, hdbscan, df_pacmap_sub, 30)


plot_ly(
	data = df_pacmap_sub %>% filter(cluster!=0), 
	x = ~x, 
	y = ~y, 
	z = ~z, 
	color = ~factor(cluster), # Cluster einfärben
	type = "scatter3d", 
	mode = "markers", 
	marker = list(
		size = 4, 
		opacity = 0.25 # Transparenz der Punkte
	),
	text = ~factor(cluster), # Text für Hover-Effekt (ID)
	hoverinfo = "text" # Nur den Text anzeigen (ID)
) %>%
	layout(
		scene = list(
			xaxis = list(title = "x"),
			yaxis = list(title = "y"),
			zaxis = list(title = "z")
		)
	)
