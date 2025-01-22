Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

#################################################################################
# 1. Get original OD flow data
################################################################################
available_schemas <- psql1_get_schemas(con)
print(available_schemas)
char_schema <- available_schemas[19, "schema_name"]

sf_data <- st_read(
	con,
	query = paste0("SELECT * FROM ", 
								 char_schema, 
								 ".data")) %>%
	mutate(cluster = as.integer(cluster))

# Parameterbereiche definieren
k_values <- seq(2, 25, by = 1)         # Beispiel: k von 5 bis 50
eps_values <- seq(2, 20, by = 1)      # Beispiel: eps von 1 bis 20
minpts_values <- seq(2, 20, by = 1)   # Beispiel: minpts von 2 bis 30

param_grid <- expand.grid(k = k_values, eps = eps_values, minpts = minpts_values)
param_grid <- param_grid[param_grid$eps < param_grid$k & param_grid$minpts < param_grid$k, ]
nrow(param_grid)

df_cvi <- data.frame(
	dist_measure = character(),
	cvi = character(),
	value = character(),
	k = integer(),
	minpts = integer(),
	eps = integer(),
	n_cluster = integer(),
	n_noise = integer()
)

str(sf_data)




cvi_snn <- function(embedding, k, eps, minpts, dist_measure) {
	if(grepl("org", dist_measure)){
		snn_res <- sNNclust(embedding, k, eps, minpts) 
		n_cluster <- length(unique(snn_res$cluster))
		n_noise <- length(which(snn_res$cluster==0))
		
		cluster_crit_ext <- clusterCrit::extCriteria(part1 = sf_data$cluster,
																								 part2 = snn_res$cluster,
																								 crit = c("Folkes_Mallows",
																								 				 "Hubert",
																								 				 "Jaccard"))
		cvis <- c(names(unlist(cluster_crit_ext))) 
		values <- c(as.numeric(unlist(cluster_crit_ext))) 
		temp_df <- data.frame(
			dist_measure = rep(dist_measure, length(cvis)),
			cvi = cvis,
			value = values,
			k = rep(k, length(cvis)),
			eps = rep(eps, length(cvis)),
			minpts = rep(minpts, length(cvis)),
			n_cluster = rep(n_cluster, length(cvis)),
			n_noise = rep(n_noise, length(cvis))
		)
		
		df_cvi <<- rbind(df_cvi, temp_df)
	} else {
		snn_res <- sNNclust(embedding, k, eps, minpts) 
		n_cluster <- length(unique(snn_res$cluster))
		n_noise <- length(which(snn_res$cluster==0))
		
		
		cluster_crit_int <- clusterCrit::intCriteria(traj = embedding,
																								 part = snn_res$cluster,
																								 crit = c("Calinski_Harabasz",
																								 				 "Davies_Bouldin",
																								 				 "Dunn",
																								 				 "Xie_Beni",
																								 				 "SD_Scat",
																								 				 "Silhouette",
																								 				 "S_Dbw"))
		
		if(length(unique(snn_res$cluster)) > 2){
			cdbw_idx <- fpc::cdbw(embedding,
														clustering = snn_res$cluster)
		} else {
			cdbw_idx <- list(cdbw = NA) 
		}
		
		cluster_crit_ext <- clusterCrit::extCriteria(part1 = sf_data$cluster,
																								 part2 = snn_res$cluster,
																								 crit = c("Folkes_Mallows",
																								 				 "Hubert",
																								 				 "Jaccard"))
		cvis <- c(names(unlist(cluster_crit_int)), "cdbw",
							names(unlist(cluster_crit_ext))) 
		
		values <- c(as.numeric(unlist(cluster_crit_int)), cdbw_idx$cdbw, 
								as.numeric(unlist(cluster_crit_ext))) 
		
		temp_df <- data.frame(
			dist_measure = rep(dist_measure, length(cvis)),
			cvi = cvis,
			value = values,
			k = rep(k, length(cvis)),
			eps = rep(eps, length(cvis)),
			minpts = rep(minpts, length(cvis)),
			n_cluster = rep(n_cluster, length(cvis)),
			n_noise = rep(n_noise, length(cvis))
		)
		
		df_cvi <<- rbind(df_cvi, temp_df)
	}
}


################################################################################
# 2. Run Clustering and CVI calculation
################################################################################
char_embedding <- "embedding_4d_5nNB_0.3qsMN_0.6qsFP_2.0ratMN_4.0ratFP"
org_distmat_network <- np$load(here::here(path_python,
																								 char_schema,
																								 "flow_manhattan_pts_network",
																								 "dist_mat.npy")) %>%
	as.matrix()


n_samples <- nrow(org_distmat_network) %>% as.integer()
org_distmat_euclid <- np$memmap(here::here(path_python,
																												 char_schema,
																												 "flow_manhattan_pts_euclid",
																												"dist_mat.npy"),
																dtype = "float32", 
																mode = "r",
																shape = tuple(n_samples, n_samples)) %>%
	as.matrix()

embedding_network <- np$load(here::here(path_python, 
																												 char_schema,
																												 "flow_manhattan_pts_network",
																												 paste0(char_embedding, ".npy"))) 

embedding_euclid <- np$load(here::here(path_python, 
																												char_schema,
																												"flow_manhattan_pts_euclid",
																												paste0(char_embedding, ".npy"))) 
for(j in 1:nrow(param_grid)){
	#j <- 4582
	cat("j: ", j, "\n")
	k <- param_grid[j, "k"]
	eps <- param_grid[j, "eps"]
	minpts <- param_grid[j, "minpts"]

	# ### Org - Network ----------------------------------------------------------
	cvi_snn(org_distmat_network, k, eps, minpts, "org_network")
	# ### Org - Euclidean --------------------------------------------------------
	cvi_snn(org_distmat_euclid, k, eps, minpts, "org_euclid")
	### PaCMAP - Network -------------------------------------------------------
	cvi_snn(embedding_network, k, eps, minpts, "pacmap_network")
	### PaCMAP - Euclidean -----------------------------------------------------
	cvi_snn(embedding_euclid, k, eps, minpts, "pacmap_euclid")
}
write_rds(df_cvi, file = paste0("cvi_", char_schema,".rds"))


log_cvis <- c("calinski_harabasz", "cdbw", "dunn", "s_dbw", "xie_beni")

df_cvi_log <- df_cvi %>%
	mutate(
		value_edit = ifelse(cvi %in% log_cvis, log(value), value), # Werte logarithmieren, wenn nötig
		x_label = ifelse(cvi %in% log_cvis, "log(Value)", "Value") # Label je nach CVI
	)

# Plot erstellen
ggplot(df_cvi_log, aes(x = value_edit, fill = dist_measure)) +
	geom_density(alpha = 0.6) +
	labs(
		title = 'Density Distributions by CVI and Distance Measure',
		y = 'Density',
		fill = 'Distance Measure'
	) +
	facet_wrap(~ cvi, scales = "free", labeller = labeller(cvi = function(x) {
		ifelse(x %in% log_cvis, paste0("log(", x, ")"), x) # Beschriftung für log-Skala
	})) +
	theme_minimal() +
	theme(strip.text = element_text(size = 10))





# df_cvi <- df_cvi %>%
# 	group_by(k, minpts, eps) %>%  # Gruppiere nach k, minpts und eps
# 	mutate(id = cur_group_id()) %>%  # Erstelle eine eindeutige ID pro Gruppe
# 	ungroup() %>%
# 	as.data.frame()
# 
# 
# 
# 
# df_cvi %>%
# 	filter(dist_measure == "pacmap_euclid" & cvi == "hubert") %>%
# 	arrange(desc(value))
# 
# 
# 
# snn_network_res <- sNNclust(embedding_network, 9,8,6) 
# sf_data$network_cl <- snn_network_res$cluster
# snn_euclid_res <- sNNclust(embedding_euclid, 9,8,8) 
# sf_data$euclid_cl <- snn_euclid_res$cluster
# 
# snn_org_network_res <- sNNclust(org_distmat_network, 11,10,9)
# sf_data$org_network_cl <- snn_network_res$cluster
# snn_org_euclid_res <- sNNclust(org_distmat_euclid, 11,10,9)
# sf_data$org_euclid_cl <- snn_euclid_res$cluster
# 
# 
# 
# p <- ggplot(sf_data[sf_data$euclid_cl!=0,]) +
# 	geom_sf(aes(geometry = geometry, color = as.factor(euclid_cl))) +
# 	scale_color_discrete(name = "Cluster") +
# 	labs(
# 		title = "Visualization of Clusters",
# 		x = "Longitude",
# 		y = "Latitude"
# 	) +
# 	theme_minimal()
# ggplotly(p)










# ggplot(df_cvi, aes(x = id, 
# 														y = value, color = dist_measure)) +
# 	#geom_bar(stat = "identity") +  # Standardmäßig gestapelt
# 	geom_point()+
# 	labs(
# 		x = "ID (by Distance Measure)",
# 		y = "CVI",
# 		fill = "Distance measure"  # Legendenbeschriftung
# 	) +
# 	theme_minimal() +
# 	facet_wrap(~cvi, scales = "free_y") +
# 	theme(axis.text.x = element_text(angle = 45, hjust = 1)) 
# 
# table(df_cvi$n_cluster)


# ggplot(df_test, aes(x = id, y = n_cluster, color = dist_measure)) +
# 	geom_point() +
# 	labs(
# 		x = "k",
# 		y = "Number of cluster",
# 		color = "Distance measure"
# 	) +
# 	theme_minimal() 
# 
