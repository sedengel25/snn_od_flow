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

################################################################################
# 1. Python data
################################################################################
all_dirs <- list.dirs(path = path_python, recursive = FALSE)
data_dirs <- all_dirs[grep("data", all_dirs)]
char_dist_measures <- c("flow_manhattan_pts_euclid",
												"flow_chebyshev_pts_euclid",
												"flow_euclid",
												"flow_manhattan_pts_network")

snn_k <- c(seq(5, 40, 2), 45, 50, 60, 80)

df_res <- data.frame(dataset = character(),
										 type = character(),
										 method = character(),
										 dist_measure = character(),
										 param = integer(),
										 numb_cluster = numeric(),
										 numb_noise = numeric(),
										 cvi = numeric(),
										 stringsAsFactors = FALSE)

for(dir in data_dirs){
		dataset <- strsplit(dir, "/")[[1]][6]
		cat("dataset: ", dataset, "\n")

		dt_dist_mat_full <- dbGetQuery(con, paste0("SELECT * FROM ", 
																							 dataset, 
																							 ".flow_distances"))
		for(dist_measure in char_dist_measures){

			cat("dist_measure: ", dist_measure, "\n")
			if(dist_measure != "flow_manhattan_pts_network"){
				dt_flow_nd <- dt_dist_mat_full[, c("flow_id_i",
																						"flow_id_j",
																						dist_measure)]
				
			} else {
				sf_trips <- st_read(
					con,
					query = paste0("SELECT * FROM ", 
												 dataset, 
												 ".data"))
				print(head(sf_trips))

				dt_flow_nd <- main_nd_dist_mat_ram(sf_trips = sf_trips,
																						dt_network = dt_network,
																						dt_dist_mat = dt_dist_mat)
			}

			for(i in 1:10){
				embedding <- here::here(dir, dist_measure, paste0(i, "_embedding.npy"))
				df_embedding <- np$load(embedding) %>%
					as.data.frame() %>%
					rename("x" = "V1",
								 "y" = "V2") 

	
				for(k in snn_k){	
					minpts <- floor(k*0.92)
					eps <- ceiling(minpts*0.185)
					cat("k: ", k, "\n")
					cat("minpts: ", minpts, "\n")
					cat("eps: ", eps, "\n")

					stop()
					cvnn_score <- fpc::cvnn(d = matrix_dist_mat_emb,
																	k = m)
					df_res <- rbind(df_res, data.frame(
						dataset = dataset,
						type = "embedding",
						method = "hdbscan",
						dist_measure = dist_measure,
						param = m,
						numb_cluster = numb_cluster,
						numb_noise = numb_noise,
						cvnn_score = mean(cvnn_score[, 3])
					))
				}
				
				
				#write_rds(df_res, here::here(path_python, "df_res.rds"))
				
		}
	}
}

unique(df_res$dataset)
