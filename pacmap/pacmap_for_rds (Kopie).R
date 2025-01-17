Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Network data
################################################################################
available_networks <- psql1_get_available_networks(con)
print(available_networks)
char_network <- available_networks[4, "table_name"]
dt_network <- st_read(con, char_network) %>% as.data.table
sf_network <- st_as_sf(dt_network)
ggplot() +
	geom_sf(data=sf_network) 

as_sfnetwork(sf_network)

char_path_dt_dist_mat <- here::here("data", "input", "dt_dist_mat")
char_av_dt_dist_mat_files <- list.files(char_path_dt_dist_mat)
print(char_av_dt_dist_mat_files)
# stop("Have you chosen the right dist mat?")
char_dt_dist_mat <-  char_av_dt_dist_mat_files[24]
char_buffer <- "50000"
dt_dist_mat <- read_rds(here::here(
	char_path_dt_dist_mat,
	char_dt_dist_mat))


# m-Spalte runden und in Integer umwandeln
dt_dist_mat <- dt_dist_mat %>%
	mutate(m = round(m, 0) %>% as.integer())
# char_dist_mat <- paste0("dd_",
# 												char_buffer,
# 												"_dist_mat")
# dbWriteTable(con, char_dist_mat, value = dt_dist_mat)
# rm(dt_dist_mat)
# gc()
# query <- paste0("CREATE INDEX idx_char_dist_mat_source_target ON ",
# 								char_dist_mat, " (source, target);")
# 
# dbExecute(con, query)
#################################################################################
# 2. OD flow data (RDS)
################################################################################
char_path <- here::here("synthetic", "data")
list.files(path = char_path, pattern = ".rds")

char_schema <- "synthetic_dd_middle_2po_4pgr_cl100_noise500"
query <- paste0("CREATE SCHEMA IF NOT EXISTS ", char_schema)
cat(query)
dbExecute(con, query)
sf_trips_sub <- read_rds(paste0(char_path, "/", char_schema, ".rds"))
sf_trips_sub$flow_id <- 1:nrow(sf_trips_sub)
st_write(sf_trips_sub, con, Id(schema=char_schema, 
															 table = "data"))


################################################################################
# 3. Calculation of distance matrices
################################################################################
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
rm(dt_dist_mat)
gc()
matrix_flow_nd[1:5,1:5]
path_pacmap <- here::here(path_python, char_schema)
dist_measure <- "flow_manhattan_pts_network"
folder <- here::here(path_pacmap, dist_measure)
if (!dir.exists(folder)) {
	dir.create(folder, recursive = TRUE, mode = "0777")
	message("Directory created with full permissions: ", folder)
} else {
	message("Directory already exists: ", folder)
}
npy_file <- here::here(path_pacmap, dist_measure, "dist_mat.npy")
np$save(npy_file, np$array(matrix_flow_nd, dtype = "int32"))
rm(matrix_flow_nd)
gc()
char_dist_measures <- c("flow_manhattan_pts_euclid",
												"flow_manhattan_pts_network")


################################################################################
# 4. Converting of distance matrices into numpy arrays
################################################################################
for(dist_measure in char_dist_measures[1]){
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
# 5. Creating 2d, 3d, 4d pacmap-embeddings
################################################################################
n_samples <- nrow(sf_trips_sub)
for(dist_measure in char_dist_measures){
	folder <- here::here(path_pacmap, dist_measure)
	for(i in 1:3){
		# system2("python3", args = c(here::here(path_python,
		# 																			 "pacmap_cpu.py"),
		# 														"--directory ", folder,
		# 														"--distance ", dist_measure,
		# 														"--n ", n_samples,
		# 														"--i ", i),
		# 				stdout = "", stderr = "")
		
		if(dist_measure != "flow_manhattan_pts_network"){
			system2("python3", args = c(here::here(path_python,
																						 "pacmap_cpu.py"),
																	"--directory ", folder,
																	"--distance ", dist_measure,
																	"--n ", n_samples,
																	"--i ", i),
							stdout = "", stderr = "")
		} else {
			system2("python3", args = c(here::here(path_python,
																						 "pacmap_ram.py"),
																	"--directory ", folder,
																	"--distance ", dist_measure,
																	"--i ", i),
							stdout = "", stderr = "")
		}
	}
}


