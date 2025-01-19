Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

#################################################################################
# 1. Get original OD flow data
################################################################################
available_schemas <- psql1_get_schemas(con)
print(available_schemas)
char_schema <- available_schemas[15, "schema_name"]

sf_data <- st_read(
	con,
	query = paste0("SELECT * FROM ", 
								 char_schema, 
								 ".data"))

# Parameterbereiche definieren
k_values <- seq(2, 30, by = 1)         # Beispiel: k von 5 bis 50
eps_values <- seq(1, 20, by = 1)      # Beispiel: eps von 1 bis 20
minpts_values <- seq(1, 20, by = 1)   # Beispiel: minpts von 2 bis 30

param_grid <- expand.grid(k = k_values, eps = eps_values, minpts = minpts_values)
param_grid <- param_grid[param_grid$eps < param_grid$k & param_grid$minpts < param_grid$k, ]
nrow(param_grid)

df_cvi <- data.frame(
	sil_net = numeric(),
	ch_net = numeric(),
	db_net = numeric(),
	dunn_net = numeric(),
	s_dbw_net = numeric(),
	cdbw_net = numeric(),
	cvnn_net = numeric(),
	#extern cvi
)

################################################################################
# 2. Run Clustering and CVI calculation
################################################################################
for(i in 1:10){
	char_embedding <- paste0("embedding_4d_", i, "_seed") 
	pacmap_distmat_network <- proxy::dist(np$load(here::here(path_python, 
																									 char_schema,
																									 "flow_manhattan_pts_network",
																									 paste0(char_embedding, ".npy")))) %>%
		as.matrix()
	
	pacmap_distmat_euclid <- proxy::dist(np$load(here::here(path_python, 
																													 char_schema,
																													 "flow_manhattan_pts_euclid",
																													 paste0(char_embedding, ".npy")))) %>%
		as.matrix()
	
	for(j in 1:nrow(param_grid)){
		k <- param_grid[j, "k"]
		eps <- param_grid[j, "eps"]
		minpts <- param_grid[j, "minpts"]
		
		snn_network_res <- sNNclust(pacmap_distmat_network, k, eps, minpts) 
		snn_euclid_res <- sNNclust(pacmap_distmat_euclid, k, eps, minpts) 
		fpc_cluster_stats_network <- cluster.stats(d = pacmap_distmat_network, 
																							 clustering = snn_network_res$cluster)
		sil_score_network <- fpc_cluster_stats_network$avg.silwidth
		ch_network <- fpc_cluster_stats_network$ch
		?clusterCrit::extCriteria
		fpc_cluster_stats_euclid <- cluster.stats(d = pacmap_distmat_euclid, 
																							 clustering = snn_euclid_res$cluster)
		break
	}
	break
}
