Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

#################################################################################
# 1. Get original OD flow data
################################################################################
available_schemas <- psql1_get_schemas(con)
print(available_schemas)
char_schema <- available_schemas[28, "schema_name"]

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

df_ari <- data.frame(
	dist_measure = character(),
	ari = numeric(),
	k = integer(),
	minpts = integer(),
	eps = integer(),
	n_cluster = integer(),
	n_noise = integer()
)


str(sf_data)






ari_snn <- function(embedding, k, eps, minpts, dist_measure) {
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
		ari = num_ari,
		k = k,
		eps = eps,
		minpts = minpts,
		n_cluster = n_cluster,
		n_noise = n_noise)
	
	return(df_temp)
}


################################################################################
# 2. Run Clustering and CVI calculation
################################################################################
char_embedding <- "embedding_4d_3nNB_0.4qsMN_0.7qsFP_1.0ratMN_2.0ratFP"
org_distmat_network <- np$load(here::here(path_python,
																					char_schema,
																					"flow_manhattan_pts_network",
																					"dist_mat.npy")) %>%
	as.matrix()


n_samples <- nrow(org_distmat_network) %>% as.integer()

org_distmat_euclid <- np$load(here::here(path_python,
																				 char_schema,
																				 "flow_manhattan_pts_euclid",
																				 "dist_mat.npy")) %>%
	as.matrix()

embedding_network <- np$load(here::here(path_python, 
																				char_schema,
																				"flow_manhattan_pts_network",
																				paste0(char_embedding, ".npy"))) 

embedding_euclid <- np$load(here::here(path_python, 
																			 char_schema,
																			 "flow_manhattan_pts_euclid",
																			 paste0(char_embedding, ".npy"))) 


run_snn_configs <- function(params,
														org_distmat_network, 
														org_distmat_euclid, 
														embedding_network, 
														embedding_euclid) {
  df_ari_temp <- data.frame(
  	dist_measure = character(),
  	ari = numeric(),
  	k = integer(),
  	minpts = integer(),
  	eps = integer(),
  	n_cluster = integer(),
  	n_noise = integer()
  )
  k <- params$k
  eps <- params$eps
  minpts <- params$minpts
  
	res_org_net <- ari_snn(org_distmat_network, k, eps, minpts, "org_network")
	res_org_euclid <- ari_snn(org_distmat_euclid, k, eps, minpts, "org_euclid")
	res_emb_net <- ari_snn(embedding_network, k, eps, minpts, "pacmap_network")
	res_emb_euclid <- ari_snn(embedding_euclid, k, eps, minpts, "pacmap_euclid")
  
  df_ari_temp <- rbind(df_ari_temp, 
  										 res_org_net,
  										 res_org_euclid,
  										 res_emb_net,
  										 res_emb_euclid)
}



list_pg_snn <- split(df_pg_snn, seq(nrow(df_pg_snn)))

t1 <- proc.time()
results <- mclapply(list_pg_snn, function(params) {
	run_snn_configs(
		params = params,
		org_distmat_network = org_distmat_network,
		org_distmat_euclid = org_distmat_euclid,
		embedding_network = embedding_network,
		embedding_euclid = embedding_euclid
	)
}, mc.cores = 14)
df_ari <- do.call(rbind, results)
t2 <- proc.time()
print(t2-t1)
#write_rds(df_cvi, file = paste0("rand_", char_schema,".rds"))


# Plot erstellen
ggplot(df_ari, aes(x = ari, fill = dist_measure)) +
	geom_density(alpha = 0.6) +
	labs(
		title = 'Density Distributions by CVI and Distance Measure',
		y = 'Density',
		fill = 'Distance Measure'
	) +
	# facet_wrap(~ ari, scales = "free") +
	theme_minimal() +
	theme(strip.text = element_text(size = 10))

