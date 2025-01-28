Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")


ari_snn <- function(embedding, 
										k, 
										eps, 
										minpts, 
										dist_measure, 
										n_neighbors,
										n_mp,
										n_fp,
										mp_ratio,
										fp_ratio) {
	# Clusterberechnung mit sNNclust
	snn_res <- sNNclust(embedding, k, eps, minpts)
	# Cluster- und Rauschstatistiken
	n_cluster <- length(unique(snn_res$cluster)) - as.integer(any(snn_res$cluster == 0))
	n_noise <- sum(snn_res$cluster == 0)
	
	# ARI-Berechnung
	num_ari <- aricode::ARI(c1 = sf_data$cluster, c2 = snn_res$cluster)
	
	# Ergebnis als data.frame zurückgeben
	df_temp <- data.frame(
		dist_measure = dist_measure,
		ari =num_ari,
		k = k,
		minpts = minpts,
		eps = eps,
		n_cluster = n_cluster,
		n_noise = n_noise,
		n_neighbors = n_neighbors,
		n_mp = n_mp,
		n_fp = n_fp,
		mp_ratio = mp_ratio,
		fp_ratio = fp_ratio
	)
	
	return(df_temp)
}


#################################################################################
# 1. Create embeddings for parameter grid
################################################################################
df_pg_pacmap <- expand.grid(
	n_neighbors = c(1.0, 2.0, 3.0, 5.0, 7.0, 10.0),
	n_mp = c(0.2, 0.3, 0.4, 0.5, 0.6),
	n_fp = c(0.5, 0.6, 0.7, 0.8, 0.9),
	mp_ratio = c(0.5, 1.0, 2.0, 3.0),
	fp_ratio = c(1.0, 2.0, 3.0, 4.0)
)

df_pg_pacmap <- df_pg_pacmap %>%
	filter(n_mp < n_fp) %>%
	filter(mp_ratio*n_neighbors >= 1)

df_pg_pacmap <- df_pg_pacmap %>%
	mutate(across(-n_neighbors, ~ sprintf("%.1f", .)))

df_pg_pacmap <- head(df_pg_pacmap, 14)
# char_dist_measures <- c("flow_manhattan_pts_euclid",
# 												"flow_manhattan_pts_network")


# for(dist_measure in char_dist_measures){
# 	folder <- here::here(path_pacmap, dist_measure)
# 	for(i in 1:nrow(df_pg_pacmap)){
# 		cat("i: ", i, "\n")
# 		system2("/usr/bin/python3", args = c(path,
# 																				 "--directory ", folder,
# 																				 "--dim ", dim,
# 																				 "--quantile_start_MN", df_pg_pacmap[i,"n_mp"],
# 																				 "--quantile_start_FP", df_pg_pacmap[i,"n_fp"],
# 																				 "--n_neighbors", df_pg_pacmap[i,"n_neighbors"],
# 																				 "--MN_ratio", df_pg_pacmap[i,"mp_ratio"],
# 																				 "--FP_ratio", df_pg_pacmap[i,"fp_ratio"]),
# 						stdout = "", stderr = "")
# 
# 	}
# 
# }


#################################################################################
# 1. Get original OD flow data
################################################################################
available_schemas <- psql1_get_schemas(con)
print(available_schemas)
char_schema <- available_schemas[38, "schema_name"]

sf_data <- st_read(
	con,
	query = paste0("SELECT * FROM ", 
								 char_schema, 
								 ".data")) %>%
	mutate(cluster = as.integer(cluster_id))

# Parameterbereiche definieren
k_values <- seq(2, 25, by = 1)         # Beispiel: k von 5 bis 50
eps_values <- seq(2, 20, by = 1)      # Beispiel: eps von 1 bis 20
minpts_values <- seq(2, 20, by = 1)   # Beispiel: minpts von 2 bis 30

df_pg_snn <- expand.grid(k = k_values, eps = eps_values, minpts = minpts_values)
df_pg_snn <- df_pg_snn[df_pg_snn$eps < df_pg_snn$k & df_pg_snn$minpts < df_pg_snn$k, ]
nrow(df_pg_snn)



org_distmat_network <- np$load(here::here(path_python,
																					char_schema,
																					"flow_manhattan_pts_network",
																					"dist_mat.npy")) %>%
	as.matrix()


org_distmat_euclid <- np$load(here::here(path_python,
																				 char_schema,
																				 "flow_manhattan_pts_euclid",
																				 "dist_mat.npy")) %>%
	as.matrix()





run_instance_wrapper <- function(params, path_python, char_schema, df_pg_snn) {

	n_neighbors <- params$n_neighbors
	n_mp <- params$n_mp
	n_fp <- params$n_fp
	mp_ratio <- params$mp_ratio
	fp_ratio <- params$fp_ratio
	char_config <- paste0(n_neighbors, "nNB_",
										n_mp, "qsMN_",
										n_fp, "qsFP_",
										mp_ratio, "ratMN_",
										fp_ratio, "ratFP")
	char_embedding <- paste0(
		"embedding_4d_",
		char_config
	)
	folder <- here::here(path_python, char_schema, "flow_manhattan_pts_network")
	# system2("/usr/bin/python3", args = c(path_det_pacmap,
	# 																		 "--directory ", folder,
	# 																		 "--dim ", 4,
	# 																		 "--quantile_start_MN", n_mp,
	# 																		 "--quantile_start_FP", n_fp,
	# 																		 "--n_neighbors", n_neighbors,
	# 																		 "--MN_ratio", mp_ratio,
	# 																		 "--FP_ratio", fp_ratio),
	# 				stdout = "", stderr = "")
	
	# Pfade zu den Embeddings
	path_embedding_network <- here::here(path_python, 
																			 char_schema, 
																			 "flow_manhattan_pts_network",
																			 paste0(char_embedding, ".npy"))
	print(char_embedding)
	# path_embedding_euclid <- here::here(path_python, 
	# 																		char_schema, 
	# 																		"flow_manhattan_pts_euclid",
	# 																		paste0(char_embedding, ".npy"))
	
	# Überprüfen, ob die Dateien existieren
	if (!file.exists(path_embedding_network)) {
		warning(paste("Embedding file not found:", path_embedding_network))
		return(NULL)  # Abbruch oder alternative Behandlung
	}
	# if (!file.exists(path_embedding_euclid)) {
	# 	warning(paste("Embedding file not found:", path_embedding_euclid))
	# 	return(NULL)  # Abbruch oder alternative Behandlung
	# }
	
	# Dateien laden
	embedding_network <- np$load(path_embedding_network)
	print(head(embedding_network))
	# embedding_euclid <- np$load(path_embedding_euclid)
	
	# Lokale Ergebnistabelle
	df_ari_temp <- data.frame(
		dist_measure = character(),
		ari = numeric(),
		k = integer(),
		minpts = integer(),
		eps = integer(),
		n_cluster = integer(),
		n_noise = integer(),
		n_neighbors = numeric(),
		n_mp = numeric(),
		n_fp = numeric(),
		mp_ratio = numeric(),
		fp_ratio = numeric()
	)
	

	# Iteriere über df_pg_snn
	for (j in 1:nrow(df_pg_snn)) {
		k <- df_pg_snn[j, "k"]
		eps <- df_pg_snn[j, "eps"]
		minpts <- df_pg_snn[j, "minpts"]

		res_network <- ari_snn(embedding_network, 
													 k, 
													 eps, 
													 minpts, 
													 "pacmap_network",
													 n_neighbors,
													 n_mp,
													 n_fp,
													 mp_ratio,
													 fp_ratio)
		print(res_network)
		# res_euclid <- ari_snn(embedding_network, 
		# 											 k, 
		# 											 eps, 
		# 											 minpts, 
		# 											 "pacmap_euclid", 
		# 											 char_embedding,
		# 											 n_neighbors,
		# 											 n_mp,
		# 											 n_fp,
		# 											 mp_ratio,
		# 											 fp_ratio)
		# df_ari_temp <- rbind(df_ari_temp, res_network, res_euclid)
		df_ari_temp <- rbind(df_ari_temp, res_network)
	}
	write_rds(df_ari_temp, here::here(folder,
																		paste0("snn_results_", char_config, ".rds"))
					)
	return(df_ari_temp)
}



# Erstelle die Liste aus DataFrame-Zeilen
list_pg_pacmap <- split(df_pg_pacmap, seq(nrow(df_pg_pacmap)))
t1 <- proc.time()
# Parallelverarbeitung mit mclapply
results <- mclapply(list_pg_pacmap, function(params) {
	run_instance_wrapper(
		params = params,
		path_python = path_python,
		char_schema = char_schema,
		df_pg_snn = df_pg_snn
	)
}, mc.cores = 14)
t2 <- proc.time()
print(t2-t1)
# Ergebnisse kombinieren
df_ari <- do.call(rbind, results)

path_res <- here::here(path_python, 
																		 char_schema, 
																		 "flow_manhattan_pts_network")
write_rds(df_ari, here::here(path_res, "ari_snn_experiment.rds"))

#################################################################################
# Analysis of df_ari
################################################################################
# df_ari_file <- df_ari %>%
# 	group_by(n_neighbors, n_mp, n_fp, mp_ratio, fp_ratio, dist_measure) %>%
# 	summarise(median_ari = median(ari),
# 						mean_ari = mean(ari),
# 						max_ari = max(ari))
# 
# 
# 
# 
# df_ari_file %>%
# 	arrange(desc(median_ari)) %>%
# 	print(n=100)




























