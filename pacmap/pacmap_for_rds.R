Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Network data
################################################################################
available_networks <- psql1_get_available_networks(con)
print(available_networks)
char_network <- available_networks[7, "table_name"]
dt_network <- st_read(con, char_network) %>% as.data.table
sf_network <- st_as_sf(dt_network)


char_path_dt_dist_mat <- here::here("data", "input", "dt_dist_mat")
char_av_dt_dist_mat_files <- list.files(char_path_dt_dist_mat)
print(char_av_dt_dist_mat_files)
char_dt_dist_mat <-  char_av_dt_dist_mat_files[28]
char_buffer <- "50000"
dt_dist_mat <- read_rds(here::here(
	char_path_dt_dist_mat,
	char_dt_dist_mat))
dt_dist_mat <- dt_dist_mat %>%
	mutate(m = round(m, 0) %>% as.integer())
#################################################################################
# 2a OD flow data (RDS)
################################################################################
# char_path <- here::here("synthetic", "data")
# list.files(char_path, pattern = ".rds", recursive = FALSE)
# char_schema <- "synthetic_dd_synth_2po_4pgr_collapsed_cl10_noise100"
# sf_trips_sub <- read_rds(paste0(char_path, "/", char_schema, ".rds"))
# sf_trips_sub$flow_id <- 1:nrow(sf_trips_sub)
# psql1_create_schema(con, char_schema)
# st_write(sf_trips_sub, con, Id(schema=char_schema,
# 															 table = "data"))


#################################################################################
# 2b OD flow data (PSQL)
################################################################################
psql1_get_schemas(con)
char_schema <- "synthetic_rand_cl10_noise100"

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
															cores = int_cores)

query <- paste0("DROP INDEX IF EXISTS ", 
								char_schema, 
								".flow_distances_flow_id_i_flow_id_j;")
dbExecute(con, query)

query <- paste0("CREATE INDEX ON ",
								char_schema, ".flow_distances (flow_id_i, flow_id_j);")
dbExecute(con, query)


### Calculate network based distances ----------------------------------------
dt_network_dist_mat <- main_nd_dist_mat_ram(sf_trips_sub, dt_network, dt_dist_mat)
rm(dt_dist_mat)
gc()

df_knn_network <- cpp_find_knn(dt_network_dist_mat,
															k = nrow(sf_trips_sub) - 1,
															flow_ids =  1:nrow(sf_trips_sub))
head(df_knn_network)

matrix_flow_nd <- convert_distmat_dt_to_matrix(dt_dist_mat = dt_network_dist_mat)
rm(dt_network_dist_mat)
gc()


df_knn_network_val <- replace_ids_with_values(df_knn = df_knn_network,
												matrix_distances = matrix_flow_nd)
df_knn_network_val$flow_ref <- 0

np_knn_network_val <- np$array(as.matrix(df_knn_network_val))
rm(df_knn_network_val)
gc()
df_knn_network <- df_knn_network - 1
np_knn_network_idx <- np$array(as.matrix(df_knn_network))
rm(df_knn_network)
gc()
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
npy_distmat <- here::here(path_pacmap, dist_measure, "dist_mat.npy")
npy_knn_idx <- here::here(path_pacmap, dist_measure, "knn_idx.npy")
npy_knn_dists <- here::here(path_pacmap, dist_measure, "knn_dists.npy")
np$save(npy_distmat, np$array(matrix_flow_nd, dtype = "int32"))
np$save(npy_knn_dists, np$array(np_knn_network_val, dtype = "int32"))
np$save(npy_knn_idx, np$array(np_knn_network_idx, dtype = "int32"))
head(np_knn_network_idx)
tail(np_knn_network_idx)
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

# Get dist-mat-datatable
dt_euclidean_dist_mat <- st_read(
	con,
	query = paste0("SELECT * FROM ", 
								 char_schema, 
								 ".flow_distances")) %>%
	rename(from = flow_id_i,
				 to = flow_id_j,
				 distance = flow_manhattan_pts_euclid) %>%
	select(-flow_manhattan_pts_network) %>%
	as.data.table()
# Make dist-mat datatable symmetric
dt_euclidean_dist_mat <- rbind(
	dt_euclidean_dist_mat,
	dt_euclidean_dist_mat[, .(from = to, to = from, distance = distance)]
)


df_knn_euclid <- cpp_find_knn(dt_euclidean_dist_mat,
						 k = nrow(sf_trips_sub) - 1,
						 flow_ids =  1:nrow(sf_trips_sub))
tail(df_knn_euclid)


################################################################################
# 5. Creating 2d, 3d, 4d pacmap-embeddings
################################################################################
n_samples <- nrow(sf_trips_sub)
for(dist_measure in char_dist_measures){
	folder <- here::here(path_python, char_schema, dist_measure)

	for(i in 1:100){
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
	break
}


