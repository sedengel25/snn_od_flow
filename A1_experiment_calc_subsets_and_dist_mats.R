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
# char_dist_mat <- "dd_center_50000_dist_mat"
char_path_dt_dist_mat <- here::here("data", "input", "dt_dist_mat")
char_av_dt_dist_mat_files <- list.files(char_path_dt_dist_mat)
char_dt_dist_mat <-  char_av_dt_dist_mat_files[20]
char_buffer <- "50000"
dt_dist_mat <- read_rds(here::here(
	char_path_dt_dist_mat,
	char_dt_dist_mat))
dt_dist_mat <- dt_dist_mat %>%
	mutate(m = round(m, 0) %>% as.integer())

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
	arrange(start_datetime) %>%
	filter(trip_distance >= 100)

sf_trips$flow_id <- 1:nrow(sf_trips)




################################################################################
# Create distance matrices based on different distance measures for subsets
################################################################################
int_seq <- seq(1, nrow(sf_trips), 5000)
reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")
hdbscan <- import("hdbscan")



for(i in 1:length(int_seq)){
	i <- 12
	j <- int_seq[i]
	k <- int_seq[i+1]
	if(is.na(k)){
		break
	}
	### Create the subset -------------------------------------------------------
	sf_trips_sub <- sf_trips %>%
		filter(flow_id >= j & flow_id <= k)
	
	sf_trips_sub <- sf_trips_sub %>% mutate(origin_id = as.integer(origin_id),
																					dest_id = as.integer(dest_id))
	
	sf_trips_sub$org_id <- sf_trips_sub$flow_id
	sf_trips_sub$flow_id <- 1:nrow(sf_trips_sub)
	print(nrow(sf_trips_sub))
	char_schema <- paste0("data_", j, "_", k)
	psql1_create_schema(con, char_schema)
	path_pacmap <- here::here(path_python, char_schema)
	st_write(sf_trips_sub, con, Id(schema=char_schema,
																 table = "data"))
	
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
	# main_nd_dist_mat_cpu(char_schema = char_schema,
	# 										 char_network = char_network,
	# 										 char_dist_mat = char_dist_mat,
	# 										 n = nrow(sf_trips_sub),
	# 										 cores = int_cores)
	
	matrix_flow_nd <- main_nd_dist_mat_ram(sf_trips_sub, dt_network, dt_dist_mat)
	char_dist_measures <- c("flow_manhattan_pts_euclid",
													"flow_chebyshev_pts_euclid",
													"flow_euclid",
													"flow_manhattan_pts_network")
	### PaCMAP ----------------------------------------
	for(dist_measure in char_dist_measures[4]){
		folder <- here::here(path_pacmap, dist_measure)
		if (!dir.exists(folder)) {
			dir.create(folder, recursive = TRUE, mode = "0777") 
			message("Directory created with full permissions: ", folder)
		} else {
			message("Directory already exists: ", folder)
		}
		
		
		if(dist_measure != "flow_manhattan_pts_network"){
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
		} else {
			npy_file <- here::here(path_pacmap, dist_measure, "dist_mat.npy")
			np$save(npy_file, np$array(matrix_flow_nd, dtype = "int32"))
		}

		# system2("python3", args = c(here::here(path_python,
		# 																			 "pacmap_cpu.py"),
		# 														"--directory ", folder,
		# 														"--distance ", dist_measure, " --n ", nrow(sf_trips_sub)),
		# 				stdout = "", stderr = "")
		
	}
	break
}
np_mat[1:5, 1:5]
np_mat <-   np$array(matrix_flow_nd, dtype = "int32")
################################################################################
# Run PaCMAP for each subset and distance matrix 5x for robustness
################################################################################
