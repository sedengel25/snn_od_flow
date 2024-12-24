Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Python data
################################################################################
reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")
hdbscan <- import("hdbscan")

paths_datasets <- list.dirs(path_python, recursive = FALSE)
path_base <- paths_datasets[3]

char_dist_measures <- c("flow_manhattan_pts_euclid",
												"flow_chebyshev_pts_euclid",
												"flow_euclid",
												"flow_manhattan_pts_network")


reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")

df_results <- data.frame(dist_measure = character(),
												 minpts = integer(),
												 numb_cluster = numeric(),
												 numb_noise = numeric(),
												 stringsAsFactors = FALSE)

for(dist_measure in char_dist_measures){
	path_dist_measure <- here::here(path_base, dist_measure)
	char_cluster_files <- list.files(path_dist_measure, "cluster")
	
	for (file in char_cluster_files) {
		file_part <- strsplit(file, "_")[[1]][4]
		minpts <- strsplit(file_part, "\\.")[[1]][1] %>% as.integer
		# c_cluster <-  np$load(here::here(path_dist_measure, file))
		# df_embedding <- np$load(here::here(path_dist_measure, "embedding.npy")) %>%
		# 	as.data.frame() %>%
		# 	rename("x" = "V1",
		# 				 "y" = "V2") %>%
		# 	mutate(cluster = c_cluster + 1)
		
		# p <- create_pacmap_ggplot_with_clusters(df_cluster = df_embedding,
		# 																	 char_dist_measure = dist_measure,
		# 																	 minpts = minpts)
		# 
		# ggsave(here::here(path_dist_measure, paste0("org_data_hdbscan_", minpts, ".pdf")), 
		# 			 plot = p)
		# 
		
		df_embedding <- np$load(here::here(path_dist_measure, "embedding.npy")) %>%
			as.data.frame() %>%
			rename("x" = "V1",
						 "y" = "V2") 
		
		df_cluster <- py_hdbscan(np, hdbscan, df_embedding, minpts)
		numb_cluster <- unique(df_cluster$cluster) %>% length
		numb_noise <- length(which(df_cluster$cluster == 0))
		df_results <- rbind(df_results, data.frame(
			dist_measure = dist_measure,
			minpts = minpts,
			numb_cluster = numb_cluster,
			numb_noise = numb_noise
		))
}


	
	# p <- create_pacmap_ggplot_with_clusters(df_cluster = df_cluster,
	# 																				char_dist_measure = dist_measure,
	# 																				minpts = minpts)
	# 
	# ggsave(here::here(path_dist_measure, paste0("pacman_data_hdbscan_", minpts, ".pdf")), 
	# 			 plot = p)

}




ggplot(df_results, aes(x = minpts, y = numb_cluster, color = dist_measure)) +
	geom_line(size = 1) +  # Linien zeichnen
	geom_point(size = 2) + # Optional: Punkte markieren
	labs(
		title = "Anzahl der Cluster in Abhängigkeit von minpts",
		x = "minpts",
		y = "Anzahl der Cluster",
		color = "Distanzmaß"
	) +
	theme_minimal() +  # Minimalistisches Theme
	theme(
		text = element_text(size = 12),  # Schriftgröße anpassen
		legend.position = "bottom"      # Legende unten platzieren
	)



