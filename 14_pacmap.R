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
dist_filter <- 100
int_kw <- c(8:10)
int_wday <- c(1:4)
int_hours <- c(5:9)
sf_trips_sub <- sf_trips %>%
	filter(week %in% int_kw) %>%
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
											"m_hours",
											paste0(int_hours, collapse = "_"),
											"_wdays",
											paste0(int_wday, collapse = "_")
											,
											"_kw",
											paste0(int_kw, collapse = "_")
											)

psql1_create_schema(con, char_schema)


st_write(sf_trips_sub, con, Id(schema=char_schema, 
															 table = "data"))



path_pacmap <- here::here(path_python, char_schema)
################################################################################
# 2. Calculate different distance measures between all OD flows 
################################################################################
# Calculate the euclidean distance between all OD flows
main_calc_diff_flow_distances(char_schema = char_schema,
															n = nrow(sf_trips_sub),
															cores = int_cores)

# Create an index on flow_id_i
query <- paste0("DROP INDEX IF EXISTS ", char_schema, ".flow_distances_flow_id_i_flow_id_j;")
dbExecute(con, query)

query <- paste0("CREATE INDEX ON ",
								char_schema, ".flow_distances (flow_id_i, flow_id_j);")
dbExecute(con, query)

# psql1_norm_col(con = con,
# 							 char_schema = char_schema,
# 							 char_col = "flow_euclid",
# 							 cores = int_cores,
# 							 int_min = 0,
# 							 int_max = 2,
# 							 n = nrow(sf_trips_sub))





main_nd_dist_mat_cpu(char_schema = char_schema,
										 char_network = char_network,
										 char_dist_mat = char_dist_mat,
										 n = nrow(sf_trips_sub),
										 cores = int_cores)

char_dist_measures <- c("flow_manhattan_pts_euclid",
												"flow_chebyshev_pts_euclid",
												"flow_euclid",
												"flow_manhattan_pts_network")
#sudo chown -R postgres /home/sebastiandengel/snn_flow/python --> run in terminal

for(dist_measure in char_dist_measures){
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


p1 <- pacmap_density_plot(df_1_embedding)
ggsave(here::here(path_pacmap, paste0(char_dist_measures[1],
																			"_pacmap.pdf")), plot = p1)
p2 <- pacmap_density_plot(df_2_embedding)
ggsave(here::here(path_pacmap, paste0(char_dist_measures[2],
																			"_pacmap.pdf")), plot = p2)
p3 <- pacmap_density_plot(df_3_embedding)
ggsave(here::here(path_pacmap, paste0(char_dist_measures[3],
																			"_pacmap.pdf")), plot = p3)
p4 <- pacmap_density_plot(df_4_embedding)
ggsave(here::here(path_pacmap, paste0(char_dist_measures[4],
																			"_pacmap.pdf")), plot = p4)
