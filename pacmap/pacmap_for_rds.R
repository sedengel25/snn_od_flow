Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Network data
################################################################################
available_networks <- psql1_get_available_networks(con)
print(available_networks)
char_network <- available_networks[6, "table_name"]
dt_network <- st_read(con, char_network) %>% as.data.table
sf_network <- st_as_sf(dt_network)


char_path_dt_dist_mat <- here::here("data", "input", "dt_dist_mat")
char_av_dt_dist_mat_files <- list.files(char_path_dt_dist_mat)
print(char_av_dt_dist_mat_files)
char_dt_dist_mat <-  char_av_dt_dist_mat_files[27]
char_buffer <- "50000"
dt_dist_mat <- read_rds(here::here(
	char_path_dt_dist_mat,
	char_dt_dist_mat))
dt_dist_mat <- dt_dist_mat %>%
	mutate(m = round(m, 0) %>% as.integer())
#################################################################################
# 2a OD flow data (RDS)
################################################################################

# Get OD-trip data from rds-file -----------------------------------------------
#char_path <- here::here("synthetic", "data")
# list.files(pattern = ".rds", recursive = TRUE)
# sf_trips_sub <- read_rds(paste0(char_path, "/", char_schema, ".rds"))
# sf_trips_sub$flow_id <- 1:nrow(sf_trips_sub)
# st_write(sf_trips_sub, con, Id(schema=char_schema, 
# 															 table = "data"))


#################################################################################
# 2b OD flow data (PSQL)
################################################################################
psql1_get_schemas(con)
char_schema <- "synth_local_n_130"

#################################################################################
# 3. Create PaCMAP init-file
################################################################################
sf_trips_sub <- st_read(
	con,
	query = paste0("SELECT * FROM ", 
								 char_schema, 
								 ".data"))

sf_trips_sub <- sf_trips_sub %>% mutate(origin_id = as.integer(origin_id),
																				dest_id = as.integer(dest_id),
																				id = as.integer(id))

str(sf_trips_sub)
df_coordinates <- data.frame(
	o_x = st_coordinates(sf_trips_sub$origin_geom)[,1],
	o_y = st_coordinates(sf_trips_sub$origin_geom)[,2],
	d_x = st_coordinates(sf_trips_sub$dest_geom)[,1],
	d_y = st_coordinates(sf_trips_sub$dest_geom)[,2])

path_pacmap <- here::here(path_python, char_schema)
char_dist_measures <- c("flow_manhattan_pts_euclid",
												"flow_manhattan_pts_network")
lapply(char_dist_measures, function(x){
	folder <- here::here(path_pacmap, x)
	if (!dir.exists(folder)) {
		dir.create(folder, recursive = TRUE, mode = "0777")
		message("Directory created with full permissions: ", folder)
	} else {
		message("Directory already exists: ", folder)
	}
})


################################################################################
# 3. Calculation of distance matrices
################################################################################
### Calculate euclidean based distances --------------------------------------
main_calc_diff_flow_distances(char_schema = char_schema,
															n = nrow(sf_trips_sub),
															cores = 1)

query <- paste0("DROP INDEX IF EXISTS ", 
								char_schema, 
								".flow_distances_flow_id_i_flow_id_j;")
dbExecute(con, query)

query <- paste0("CREATE INDEX ON ",
								char_schema, ".flow_distances (flow_id_i, flow_id_j);")
dbExecute(con, query)


### Calculate network based distances ----------------------------------------
matrix_flow_nd <- main_nd_dist_mat_ram(sf_trips_sub, dt_network, dt_dist_mat)
rm(dt_dist_mat)
gc()
matrix_flow_nd[1:5,1:5]

################################################################################
# 4. Write network dist-mat to np-file
################################################################################
char_dist_measures <- c("flow_manhattan_pts_euclid",
												"flow_manhattan_pts_network")
dist_measure <- char_dist_measures[2]
folder <- here::here(path_pacmap, dist_measure)
if (!dir.exists(folder)) {
	dir.create(folder, recursive = TRUE, mode = "0777")
	message("Directory created with full permissions: ", folder)
} else {
	message("Directory already exists: ", folder)
}
write.csv(df_coordinates, here::here(folder,
																		 "df_coordinates.csv"), row.names = FALSE)
npy_file <- here::here(path_pacmap, dist_measure, "dist_mat.npy")
np$save(npy_file, np$array(matrix_flow_nd, dtype = "int32"))
rm(matrix_flow_nd)
gc()


################################################################################
# 5. Write euclidean dist-mat to np-file
################################################################################
dist_measure <- char_dist_measures[1]
folder <- here::here(path_pacmap, dist_measure)
if (!dir.exists(folder)) {
	dir.create(folder, recursive = TRUE, mode = "0777")
	message("Directory created with full permissions: ", folder)
} else {
	message("Directory already exists: ", folder)
}
write.csv(df_coordinates, here::here(folder,
																		 "df_coordinates.csv"), row.names = FALSE)
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

files_delete <- list.files(folder, "chunk", full.names = TRUE)
unlink(files_delete)
	
gc()


################################################################################
# 5. Creating 2d, 3d, 4d pacmap-embeddings
################################################################################
n_samples <- nrow(sf_trips_sub)
for(dist_measure in char_dist_measures){
	folder <- here::here(path_python, char_schema, dist_measure)

	for(i in 1:50){
		if(dist_measure != "flow_manhattan_pts_network"){
			system2("/home/sebastiandengel/anaconda3/bin/python3", args = c(here::here(path_python,
																						 "pacmap_init_cpu.py"),
																	"--directory ", folder,
																	"--distance ", dist_measure,
																	"--n ", n_samples,
																	"--i ", i),
							stdout = "", stderr = "")
		} else {

			system2("/home/sebastiandengel/anaconda3/bin/python3", args = c(here::here(path_python,
																																								 "pacmap_init_ram.py"),
																																			"--directory ", folder,
																																			"--distance ", dist_measure,
																																			"--i ", i),
							stdout = "", stderr = "")
		}
		gc()
	}
}


