Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Python data
################################################################################
reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")
hdbscan <- import("hdbscan")



reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")

all_dirs <- list.dirs(path = path_python, recursive = FALSE)

filtered_dirs <- all_dirs[grep("data", all_dirs)]

char_dist_measures <- c("flow_manhattan_pts_euclid",
												"flow_chebyshev_pts_euclid",
												"flow_euclid",
												"flow_manhattan_pts_network")

hdbscan_minpts <- seq(4, 60, 2)
snn_k <- seq(6, 80, 2)

df_res <- data.frame(dataset = character(),
										 i = numeric(),
										 method = character(),
										 dist_measure = character(),
										 param = integer(),
										 numb_cluster = numeric(),
										 numb_noise = numeric(),
										 sil_score = numeric(),
										 stringsAsFactors = FALSE)

for(dir in filtered_dirs){
		dataset <- strsplit(dir, "/")[[1]][6]
		for(dist_measure in char_dist_measures){
			for(i in 1:10){
				embedding <- here::here(dir, dist_measure, paste0(i, "_embedding.npy"))
				df_embedding <- np$load(embedding) %>%
					as.data.frame() %>%
					rename("x" = "V1",
								 "y" = "V2") 
				matrix_dist_mat <- dist(df_embedding)
				cat("dir: ", dir, "\n")
				cat("dist_measure: ", dist_measure, "\n")
				for(m in hdbscan_minpts){	
	
					cat("minpts: ", m, "\n")
					df_cluster <- py_hdbscan(np, hdbscan, df_embedding, m)
					numb_cluster <- unique(df_cluster$cluster) %>% length
					numb_noise <- length(which(df_cluster$cluster == 0))
					sil_score <- silhouette(df_cluster$cluster, matrix_dist_mat)
					df_res <- rbind(df_res, data.frame(
						dataset = dataset,
						i = i,
						method = "hdbscan",
						dist_measure = dist_measure,
						param = m,
						numb_cluster = numb_cluster,
						numb_noise = numb_noise,
						sil_score = mean(sil_score[, 3])
					))
				}
				
				
				for(k in snn_k){
					print(k)
					snn_eps <- k/2
					snn_minpts <- k/2 + 1
					gc()
					snn_res <- sNNclust(matrix_dist_mat, k = k, eps = snn_eps, minPts = snn_minpts)
					gc()
					numb_cluster <- length(snn_res$cluster %>% unique)
					numb_noise <- length(which(snn_res$cluster == 0))
					print(numb_cluster)
					print(numb_noise)
					gc()
					sil_score <- silhouette(snn_res$cluster, matrix_dist_mat)
					gc()
					df_res <- rbind(df_res, data.frame(
						dataset = dataset,
						i = i,
						method = "snn",
						dist_measure = dist_measure,
						param = k,
						numb_cluster = numb_cluster,
						numb_noise = numb_noise,
						sil_score = mean(sil_score[, 3])
					))
				}
				write_rds(df_res, here::here(path_python, "df_res.rds"))
				
		}
	}
}

unique(df_res$dataset)
