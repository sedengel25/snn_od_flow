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
char_network <- available_networks[12, "table_name"]
dt_network <- st_read(con, char_network) %>% 
	as.data.table  %>%
	rename(geom_way = geometry)

sf_network <- st_as_sf(dt_network)


char_path_dt_dist_mat <- here::here("data", "input", "dt_dist_mat")
char_av_dt_dist_mat_files <- list.files(char_path_dt_dist_mat)
print(char_av_dt_dist_mat_files)
char_dt_dist_mat <-  char_av_dt_dist_mat_files[29]
char_buffer <- "5000"
dt_dist_mat <- read_rds(here::here(
	char_path_dt_dist_mat,
	char_dt_dist_mat))
dt_dist_mat <- dt_dist_mat %>%
	mutate(m = round(m, 0) %>% as.integer())

#################################################################################
# 2b OD flow data (PSQL)
################################################################################
psql1_get_schemas(con)
char_schema <- "synthetic_rand_cl15_noise50_4"


#################################################################################
# Prepare OD flow data
################################################################################
sf_trips <- st_read(
	con,
	query = paste0("SELECT * FROM ", 
								 char_schema, 
								 ".data"))
str(sf_trips)

sf_trips <- sf_trips %>%
	select(origin_id, dest_id, flow_id, o_closest_point, d_closest_point, geometry)
#################################################################################
# 3. Create PaCMAP init-file
################################################################################

int_n <- nrow(sf_trips)
df_coordinates <- data.frame(
	o_x = st_coordinates(sf_trips$o_closest_point)[,1],
	o_y = st_coordinates(sf_trips$o_closest_point)[,2],
	d_x = st_coordinates(sf_trips$d_closest_point)[,1],
	d_y = st_coordinates(sf_trips$d_closest_point)[,2])

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

matrix_distmat_euclid[1:5, 1:5]
knn_res_euclid <- get.knnx(data = matrix_distmat_euclid, 
													 query = matrix_distmat_euclid, k = int_n)
matrix_knn_euclid_val <- knn_res_euclid$nn.dist
matrix_knn_euclid_idx <- knn_res_euclid$nn.index

save_npy_distmat_and_knn(matrix_knn_val = matrix_knn_euclid_val,
												 matrix_knn_idx = matrix_knn_euclid_idx,
												 matrix_distmat = matrix_distmat_euclid,
												 dist_measure = char_dist_measures[1])
### Calculate network based distances ----------------------------------------
dt_distmat_network <- main_nd_dist_mat_ram(sf_trips, dt_network, dt_dist_mat)
matrix_distmat_network <- convert_distmat_dt_to_matrix(dt_dist_mat = dt_distmat_network)
matrix_distmat_network[1:5, 1:5]
knn_res_network <- get.knnx(data = matrix_distmat_network, 
														query = matrix_distmat_network, k = int_n)
matrix_knn_network_val <- knn_res_network$nn.dist
matrix_knn_network_idx <- knn_res_network$nn.index
save_npy_distmat_and_knn(matrix_knn_val = matrix_knn_network_val,
												 matrix_knn_idx = matrix_knn_network_idx,
												 matrix_distmat = matrix_distmat_network,
												 dist_measure = char_dist_measures[2])
rm(matrix_distmat_network)
rm(matrix_distmat_euclid)
rm(knn_res_network)
rm(knn_res_euclid)
rm(matrix_knn_network_val)
rm(matrix_knn_network_idx)
rm(matrix_knn_euclid_val)
rm(matrix_knn_euclid_idx)
gc()
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
		system2("/usr/bin/python3", args = c(path,
																				"--directory ", folder,
																				"--dim ", dim,
																				"--quantile_start_MN", 0.4,
																				"--quantile_start_FP", 0.7,
																				"--n_neighbors", 3,
																				"--MN_ratio", 1,
																				"--FP_ratio", 2),
						stdout = "", stderr = "")
	}
}




