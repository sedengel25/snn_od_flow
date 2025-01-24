Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Functions
################################################################################
save_npy_distmat_and_knn <- function(matrix_knn_val, 
																		 matrix_knn_idx, 
																		 matrix_distmat,  
																		 dist_measure) {
	# Create folder
	folder <- here::here(path_pacmap, dist_measure)
	if (!dir.exists(folder)) {
		dir.create(folder, recursive = TRUE, mode = "0777")
		message("Directory created with full permissions: ", folder)
	} else {
		message("Directory already exists: ", folder)
	}
	
	
	# Save initialization file
	write.csv(df_coordinates, here::here(folder,
																			 "df_coordinates.csv"), row.names = FALSE)
	
	
	# Save distmat
	npy_distmat <- here::here(folder, "dist_mat.npy")
	np$save(npy_distmat, np$array(matrix_distmat, dtype = "int32"))
	# Save knn containing values
	np_knn_val <- np$array(matrix_knn_val)
	file_np_knn_val <- here::here(folder, "knn_dists.npy")
	np$save(file_np_knn_val, np$array(np_knn_val, dtype = "float32"))
	
	# Save knn containing indices
	matrix_knn_idx <- matrix_knn_idx - 1 # (Für Python)
	np_knn_idx <- np$array(matrix_knn_idx)
	file_np_knn_idx <- here::here(folder, "knn_idx.npy")
	np$save(file_np_knn_idx, np$array(np_knn_idx, dtype = "int32"))
}

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
char_schema <- "synth_local_n_101"

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

int_n <- nrow(sf_trips_sub)
df_coordinates <- data.frame(
	o_x = st_coordinates(sf_trips_sub$origin_geom)[,1],
	o_y = st_coordinates(sf_trips_sub$origin_geom)[,2],
	d_x = st_coordinates(sf_trips_sub$dest_geom)[,1],
	d_y = st_coordinates(sf_trips_sub$dest_geom)[,2])

path_pacmap <- here::here(path_python, char_schema)
char_dist_measures <- c("flow_manhattan_pts_euclid",
												"flow_manhattan_pts_network")



################################################################################
# 3. Calculation of distance matrices and knn
################################################################################
### Calculate euclidean based distances --------------------------------------
num_o_dist <- as.matrix(dist(cbind(df_coordinates$o_x, df_coordinates$o_y))) 
num_d_dist <- as.matrix(dist(cbind(df_coordinates$d_x, df_coordinates$d_y))) 
matrix_distmat_euclid <- num_o_dist + num_d_dist
rm(num_o_dist)
rm(num_d_dist)
gc()

matrix_distmat_euclid[30:35, 30:35]
knn_res_euclid <- get.knnx(data = matrix_distmat_euclid, 
											 query = matrix_distmat_euclid, k = int_n)
matrix_knn_euclid_val <- knn_res_euclid$nn.dist
matrix_knn_euclid_val
matrix_knn_euclid_idx <- knn_res_euclid$nn.index

save_npy_distmat_and_knn(matrix_knn_val = matrix_knn_euclid_val,
												 matrix_knn_idx = matrix_knn_euclid_idx,
												 matrix_distmat = matrix_distmat_euclid,
												 dist_measure = char_dist_measures[1])
### Calculate network based distances ----------------------------------------
dt_distmat_network <- main_nd_dist_mat_ram(sf_trips_sub, dt_network, dt_dist_mat)
matrix_distmat_network <- convert_distmat_dt_to_matrix(dt_dist_mat = dt_distmat_network)
knn_res_network <- get.knnx(data = matrix_distmat_network, 
													 query = matrix_distmat_network, k = int_n)
matrix_knn_network_val <- knn_res_network$nn.dist
matrix_knn_network_val
matrix_knn_network_idx <- knn_res_network$nn.index
matrix_knn_network_idx
save_npy_distmat_and_knn(matrix_knn_val = matrix_knn_network_val,
												 matrix_knn_idx = matrix_knn_network_idx,
												 matrix_distmat = matrix_distmat_network,
												 dist_measure = char_dist_measures[2])


################################################################################
# 5. Creating 2d, 3d, 4d pacmap-embeddings
################################################################################
path <- "/home/sebastiandengel/deterministic_pacmap/pacmap_synth.py"
char_dist_measures <- c("flow_manhattan_pts_euclid",
												"flow_manhattan_pts_network")
for(dist_measure in char_dist_measures){
	for(dim in c(4)){
		cat("dimension: ", dim, "\n")
		folder <- here::here(path_pacmap, dist_measure)
		system2("/home/sebastiandengel/anaconda3/bin/python3", args = c(path,
																								"--directory ", folder,
																								"--dim ", dim,
																								"--quantile_start_MN", 0.3,
																								"--quantile_start_FP", 0.6,
																								"--n_neighbors", 20,
																								"--MN_ratio", 0.5,
																								"--FP_ratio", 2),
						stdout = "", stderr = "")
	}
}




