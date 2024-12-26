Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Python data
################################################################################
reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")
hdbscan <- import("hdbscan")

chars_datasets <- list.dirs(path_python, recursive = FALSE)
char_base <- chars_datasets[6]
char_data <- strsplit(char_base, "/")[[1]][6]
char_dist_measures <- c("flow_manhattan_pts_euclid",
												"flow_chebyshev_pts_euclid",
												"flow_euclid",
												"flow_manhattan_pts_network")


reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")

df_hdbscan <- data.frame(method = character(),
												 dist_measure = character(),
												 minpts = integer(),
												 numb_cluster = numeric(),
												 numb_noise = numeric(),
												 sil_score = numeric(),
												 stringsAsFactors = FALSE)
hdbscan_minpts <- 5:60
for(dist_measure in char_dist_measures){
	char_dist_measure <- here::here(char_base, dist_measure)
	print(char_dist_measure)
	df_embedding <- np$load(here::here(char_dist_measure, "embedding.npy")) %>%
		as.data.frame() %>%
		rename("x" = "V1",
					 "y" = "V2") 
	dist(df_embedding)
	gc()
	n <- nrow(df_embedding)
	print(n)
	matrix_dist_mat <- dist(df_embedding)
	gc()
	for(m in hdbscan_minpts){	
		print(m)
		df_cluster <- py_hdbscan(np, hdbscan, df_embedding, m)
		numb_cluster <- unique(df_cluster$cluster) %>% length
		numb_noise <- length(which(df_cluster$cluster == 0))
		print(numb_cluster)
		print(numb_noise)
		sil_score <- silhouette(df_cluster$cluster, matrix_dist_mat)
		df_hdbscan <- rbind(df_hdbscan, data.frame(
			method = "hdbscan",
			dist_measure = dist_measure,
			minpts = m,
			numb_cluster = numb_cluster,
			numb_noise = numb_noise,
			sil_score = mean(sil_score[, 3])
		))
	}
}
gc()
p1 <- ggplot(df_hdbscan, aes(x = minpts, y = sil_score, color = dist_measure)) +
	geom_line(size = 1) +  
	geom_point(size = 2) + 
	labs(
		title = paste0("Cluster vs. minpts (",
									 char_data, ")"),		 
		x = "minpts",
		y = "Anzahl der Cluster",
		color = "Distanzmaß"
	) +
	theme_minimal() + 
	theme(
		text = element_text(size = 12), 
		legend.position = "bottom"    
	)
gc()
print(p1)
ggsave(here::here(char_base, "silscore_on_hdbscan_pacman.pdf"), plot = p1)
df_snn <- data.frame(method = character(),
												 dist_measure = character(),
												 k = integer(),
												 numb_cluster = numeric(),
												 numb_noise = numeric(),
												 sil_score = numeric(),
												 stringsAsFactors = FALSE)
snn_k <- seq(6, 70, 2)
for(dist_measure in char_dist_measures){
	
	char_dist_measure <- here::here(char_base, dist_measure)
	print(char_dist_measure)
	df_embedding <- np$load(here::here(char_dist_measure, "embedding.npy")) %>%
		as.data.frame() %>%
		rename("x" = "V1",
					 "y" = "V2") 
	gc()
	matrix_dist_mat <- dist(df_embedding)
	gc()
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
		df_snn <- rbind(df_snn, data.frame(
			method = "snn",
			dist_measure = dist_measure,
			k = k,
			numb_cluster = numb_cluster,
			numb_noise = numb_noise,
			sil_score = mean(sil_score[, 3])
		))
	}
}

p2 <- ggplot(df_snn, aes(x = k, y = sil_score, color = dist_measure)) +
	geom_line(size = 1) +  
	geom_point(size = 2) + 
	labs(
		title = paste0("Cluster vs. k (",
									 char_data, ")"),		 
		x = "minpts",
		y = "k der Cluster",
		color = "Distanzmaß"
	) +
	theme_minimal() + 
	theme(
		text = element_text(size = 12), 
		legend.position = "bottom"    
	)
print(p2)
ggsave(here::here(char_base, "silscore_on_snn_pacman.pdf"), plot = p2)
