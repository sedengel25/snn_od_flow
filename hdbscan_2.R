Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Input
################################################################################
reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")
hdbscan <- import("hdbscan")


chars_datasets <- list.dirs(path_python, recursive = FALSE)
char_base <- chars_datasets[6]
char_schema <- strsplit(char_base, "/")[[1]][6]
char_dist_measures <- c("flow_manhattan_pts_euclid",
												"flow_chebyshev_pts_euclid",
												"flow_euclid",
												"flow_manhattan_pts_network")

df_data <- st_read(
	con,
	query = paste0("SELECT * FROM ", 
								 char_schema, 
								 ".data")) %>%
	as.data.frame()
################################################################################
# 2. Cluster PaCMAP data (HDBSCAN)
################################################################################
df_pacmap_data <- np$load(here::here(char_base, char_dist_measures[4], "embedding.npy")) %>%
	as.data.frame %>%
	rename("x"="V1", "y"="V2")



int_minpts <- 10
df_pacmap_cl <- py_hdbscan(np, hdbscan, df_pacmap_data, int_minpts)

################################################################################
# 3. Cluster PaCMAP data (SNN)
################################################################################
snn_k <- 14
snn_eps <- 7
snn_minpts <- 8

snn_pacmap_res <- sNNclust(df_pacmap_data,
													 k = snn_k,
													 eps = snn_eps,
													 minPts = snn_minpts)
snn_pacmap_res$cluster %>% table
df_pacmap_snn <- data.frame(
	flow_id = 1:n,
	cluster = snn_pacmap_res$cluster
)

sf_cluster_pacmap_snn <- df_data %>%
	left_join(df_pacmap_snn %>% select(flow_id, cluster), by = "flow_id") %>%
	st_as_sf()

st_write(sf_cluster_pacmap_snn, con, Id(schema=char_schema, 
																		 table = paste0("pacmap_snn_k",
																		 							 snn_k,
																		 							 "_eps",
																		 							 snn_eps,
																		 							 "_minpts",
																		 							 snn_minpts)))
################################################################################
# 4. Cluster original data (SNN)
################################################################################
n <- nrow(df_pacmap_cl)
matrix_dist_mat <- np$memmap(here::here(char_base, char_dist_measures[4], "dist_mat.npy"), 
														 dtype="float32", 
														 mode="r",
														 shape = tuple(n, n))
matrix_dist_mat[1:5, 1:5]
gc()

snn_org_res <- sNNclust(x = as.dist(matrix_dist_mat),
														k = snn_k,
														eps = snn_eps,
														minPts = snn_minpts)
df_org_snn <- data.frame(
	flow_id = 1:n,
	cluster = snn_res$cluster
)

sf_cluster_org_snn <- df_data %>%
	left_join(df_org_snn %>% select(flow_id, cluster), by = "flow_id") %>%
	st_as_sf()

st_write(sf_cluster_org_snn, con, Id(schema=char_schema, 
																			table = paste0("org_snn_k",
																										 snn_k,
																										 "_eps",
																										 snn_eps,
																										 "_minpts",
																										 snn_minpts)))

################################################################################
# 5. Cluster original data (HDBSCAN)
################################################################################
int_minpts <- 10
df_org_hdbscan <- np$load(here::here(char_base, 
																		 char_dist_measures[4], 
																		 paste0("cluster_labels_minpts_",
																		 			 int_minpts, ".npy"))) %>% 
	as.data.frame()  %>%
	rename("flow_id"="V1", "cluster"="V2") %>%
	mutate(cluster = cluster + 1)

adjustedRandIndex(df_org_hdbscan$cluster, df_org_snn$cluster)






















sf_cluster_pacmap <- df_data %>%
	left_join(df_pacmap_cl %>% select(flow_id, cluster), by = "flow_id") %>%
	st_as_sf()

st_write(sf_cluster_pacmap, con, Id(schema=char_schema, 
																			table = paste0("pacmap_hdbscan_", minpts)))
# ggplot(df_results, aes(x = minpts, y = numb_cluster, color = dist_measure)) +
# 	geom_line(size = 1) +  # Linien zeichnen
# 	geom_point(size = 2) + # Optional: Punkte markieren
# 	labs(
# 		title = "Anzahl der Cluster in Abhängigkeit von minpts",
# 		x = "minpts",
# 		y = "Anzahl der Cluster",
# 		color = "Distanzmaß"
# 	) +
# 	theme_minimal() +  # Minimalistisches Theme
# 	theme(
# 		text = element_text(size = 12),  # Schriftgröße anpassen
# 		legend.position = "bottom"      # Legende unten platzieren
# 	)



