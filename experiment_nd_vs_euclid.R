Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Network data
################################################################################
char_city <- "col"
dt_network <- st_read(con, paste0(char_city,
																	"_2po_4pgr")) %>% as.data.table
char_path_dt_dist_mat <- here::here("data", "input", "dt_dist_mat")
char_av_dt_dist_mat_files <- list.files(char_path_dt_dist_mat)
char_dt_dist_mat <-  char_av_dt_dist_mat_files[4]
print(char_dt_dist_mat)
char_buffer <- strsplit(char_dt_dist_mat, "_")[[1]][2]
dt_dist_mat <- read_rds(here::here(
	char_path_dt_dist_mat,
	char_dt_dist_mat))
# m-Spalte runden und in Integer umwandeln
dt_dist_mat <- dt_dist_mat %>%
	mutate(m = round(m, 0) %>% as.integer())

char_path_trips <- here::here("data", "experiment")
char_trip_files <- list.files(char_path_trips)
trip_file <- char_trip_files[18]
sf_trips_labelled <- read_rds(here::here(char_path_trips, trip_file)) 


char_path_cpp_maps<- here::here("data", "input", "cpp_map_dist_mat")
char_map_file <- here::here(char_path_cpp_maps,
														paste0(char_city,
																	 "_",
																	 char_buffer,
																	 ".bin"))



################################################################################
# 3. Calculate network distances between OD flows and put it into a matrix
################################################################################
tnd1 <- proc.time()
dt_flow_nd <- main_calc_flow_nd_dist_mat(sf_trips_labelled, 
																								 dt_network,
																								 dt_dist_mat)
dt_flow_nd <- dt_flow_nd[,-4]
dt_sym <- rbind(
	dt_flow_nd,
	dt_flow_nd[, .(flow_m = flow_n, flow_n = flow_m, distance = distance)]
)
dt_flow_nd <- dt_sym

tnd2 <- proc.time()
time_nd <- tnd2-tnd1
cat("Calculation of network distances took ", time_nd[[3]], " seconds.")
teuclid1 <- proc.time()
dt_flow_euclid <- main_calc_flow_euclid_dist_mat(sf_trips_labelled,
																								 buffer = 200)


teuclid2 <- proc.time()
time_euclid <- teuclid2-teuclid1
cat("Calculation of euclidean distances took ", time_euclid[[3]], " seconds.")



# ggplot(data = sf_trips_labelled[sf_trips_labelled$flow_id %in%
# 																	c(1,3220,4993),]) +
# 	geom_sf(data=st_as_sf(dt_network)) +
# 	geom_sf(aes(color = as.character(cluster_id)), size = 1) +
# 	theme_minimal() +
# 	labs(color = "Cluster ID",
# 			 title = paste0("Ground truth for",
# 			 							 trip_file))

################################################################################
# 4. Find a good value for k based on the RKD plot
################################################################################
# int_k_max <- 50
# 
# plot_optimal_k(k_max = int_k_max,
# 							 matrix_flow_distances = matrix_flow_nd_dist)


################################################################################
# 5. Set up possible parameter-combinations (Experiment part)
################################################################################
param_grid <- expand.grid(k = c(10,15,20,25,30,35,40,45,50),
						eps = c(5,10,15,20,25,30,35,40,45),
						minpts = c(5,10,15,20,25,30,35,40,45))

param_grid <- param_grid[rep(seq_len(nrow(param_grid)), each = 2), ]


param_grid <- param_grid %>%
	filter(eps < k)
char_methods <- c("rand_index",
									"adjusted_rand_index",
									"jaccard_index",
									"fowlkes_mallows_index",
									"mirkin_metric",
									"purity",
									"entropy",
									"nmi",
									"nvi")

df_cluster_valid <- data.frame(
	dist_measure = character(),
	k = integer(),
	eps = integer(),
	minpts = integer(),
	rand = numeric(),
	adj_rand = numeric(),
	jaccard = numeric(),
	fowlkes_mallows = numeric(),
	mirkin = numeric(),
	purity = numeric(),
	entropy = numeric(),
	nmi = numeric(),
	nvi = numeric()
	# sil = numeric(),
	# geom_sil = numeric(),
	# cdbw = numeric()
)

for(i in 1:nrow(param_grid)){
	print(i)
	int_k <- param_grid[i, "k"]
	int_eps <-  param_grid[i, "eps"]
	int_minpts <- param_grid[i, "minpts"]
	if((i %% 2) == 0) {
		dt_snn_pred_euclid <- snn_flow(sf_trips = sf_trips_labelled,
																	 k = int_k,
																	 eps = int_eps,
																	 minpts = int_minpts,
																	 dt_flow_distance = dt_flow_euclid)
		
		sf_snn_euclid <- dt_snn_pred_euclid %>%
			left_join(sf_trips_labelled %>% select(flow_id, geometry), 
								by = c("flow" = "flow_id")) %>%
			st_as_sf()
		
		cluster_valid_euclid <- lapply(char_methods, function(x){
			idx_euclid <- ClusterR::external_validation(sf_trips_labelled$cluster_id,
																									dt_snn_pred_euclid$cluster_pred,
																									method = x)
			
		})
		df_cluster_valid[i, "dist_measure"] <- "euclid" 
		df_cluster_valid[i, "k"] <- int_k
		df_cluster_valid[i, "eps"] <- int_eps
		df_cluster_valid[i, "minpts"] <- int_minpts
		df_cluster_valid[i, 5:13] <- sapply(cluster_valid_euclid, `[`, 1)
		# df_cluster_valid[i, "sil"] <- calc_sil_score(sf_trips = sf_snn_euclid)$sil
		# df_cluster_valid[i, "geom_sil"] <- calc_sil_score(sf_trips = sf_snn_euclid)$geom_sil
	} else {
		dt_snn_pred_nd <- snn_flow(sf_trips = sf_trips_labelled,
																k = int_k,
																eps = int_eps,
																minpts = int_minpts,
																dt_flow_distance = dt_flow_nd)
		sf_snn_nd <- dt_snn_pred_nd %>%
			left_join(sf_trips_labelled %>% select(flow_id, geometry), 
								by = c("flow" = "flow_id")) %>%
			st_as_sf()

		cluster_valid_nd <- lapply(char_methods, function(x){
			idx_nd <- ClusterR::external_validation(sf_trips_labelled$cluster_id,
																							dt_snn_pred_nd$cluster_pred,
																							method = x)
			
		})
		df_cluster_valid[i, "dist_measure"] <- "nd" 
		df_cluster_valid[i, "k"] <- int_k
		df_cluster_valid[i, "eps"] <- int_eps
		df_cluster_valid[i, "minpts"] <- int_minpts
		df_cluster_valid[i, 5:13] <- sapply(cluster_valid_nd, `[`, 1)
		### Calculation of silhoutte-score causes session abort due to big matrices
		# df_cluster_valid[i, "sil"] <- calc_sil_score(sf_trips = sf_snn_nd)$sil
		# df_cluster_valid[i, "geom_sil"] <- calc_sil_score(sf_trips = sf_snn_nd)$geom_sil
	}
}
write_rds(df_cluster_valid, here::here("data",
																			 "cluster_validation_results",
																			 paste0("cluster_val_ext", trip_file)))

################################################################################
# 7. Evaluate the results visually
################################################################################
# DataFrame für "nd" filtern und nach adj_rand sortieren
df_nd <- df_cluster_valid[df_cluster_valid$dist_measure == "nd", ]
df_nd_sorted <- df_nd[order(desc(df_nd$adj_rand)), ]

# DataFrame für "euclid" filtern und nach adj_rand sortieren
df_euclid <- df_cluster_valid[df_cluster_valid$dist_measure == "euclid", ]
df_euclid_sorted <- df_euclid[order(desc(df_euclid$adj_rand)), ]

# Die besten Ergebnisse anzeigen
cat("Best parameters for 'nd' based on adj_rand:\n")
print(head(df_nd_sorted))


cat("\nBest parameters for 'euclid' based on adj_rand:\n")
print(head(df_euclid_sorted))










# Ground truth
ggplot(data = sf_trips_labelled[sf_trips_labelled$cluster_id!=0,]) +
	geom_sf(data=st_as_sf(dt_network)) +
	geom_sf(aes(color = as.character(cluster_id)), size = 1) +
	theme_minimal() +
	labs(color = "Cluster ID",
			 title = paste0("Ground truth for",
			 							 trip_file))


best_euclid_k <- df_euclid_sorted[1, "k"]
best_euclid_eps <- df_euclid_sorted[1, "eps"]
best_euclid_minpts <- df_euclid_sorted[1, "minpts"]


dt_snn_pred_euclid <- snn_flow(sf_trips = sf_trips_labelled,
																					k = best_euclid_k,
																					eps = best_euclid_eps,
																					minpts = best_euclid_minpts,
																					dt_flow_distance = dt_flow_euclid)




sf_cluster_euclid_pred <- sf_trips_labelled %>%
	left_join(dt_snn_pred_euclid, by = c("flow_id" = "flow")) %>%
	select(cluster_pred, geometry)

ggplot(data = sf_cluster_euclid_pred[sf_cluster_euclid_pred$cluster_pred!=0,]) +
	geom_sf(data=st_as_sf(dt_network)) +
	geom_sf(aes(color = as.character(cluster_pred)), size = 1) +
	theme_minimal() +
	labs(color = "Cluster ID",
			 title = paste0("Euclidean distance cluster result for",
			 							 " (buffer", char_buffer, "_", trip_file, ")"),)

best_nd_k <- df_nd_sorted[1, "k"]
best_nd_eps <- df_nd_sorted[1, "eps"]
best_nd_minpts <- df_nd_sorted[1, "minpts"]



dt_snn_pred_nd <- snn_flow(sf_trips = sf_trips_labelled,
													 k = best_nd_k,
													 eps = best_nd_eps,
													 minpts = best_nd_minpts,
													 dt_flow_distance = dt_flow_nd)


sf_cluster_nd_pred <- sf_trips_labelled %>%
	left_join(dt_snn_pred_nd, by = c("flow_id" = "flow")) %>%
	select(cluster_pred, geometry)

# dir.create()
# char_path_plots <- here::here()
	
	
ggplot(data = sf_cluster_nd_pred[sf_cluster_nd_pred$cluster_pred!=0,]) +
	geom_sf(data=st_as_sf(dt_network)) +
	geom_sf(aes(color = as.character(cluster_pred)), size = 1) +
	theme_minimal() +
	labs(color = "Cluster ID",
			 title = paste0("Network distance cluster result for",
			 							 " (buffer", char_buffer, "_", trip_file, ")"),)




################################################################################
# 3d plot
################################################################################
# Network distance
plot_ly(
	df_nd_sorted, x = ~k, y = ~eps, z = ~minpts, color = ~adj_rand,
	type = 'scatter3d', mode = 'markers',
	marker = list(size = 5)
) %>%
	layout(title = paste0("Adjusted Rand Index for 'nd'",
											 " (buffer", char_buffer, "_", trip_file, ")"),
				 scene = list(
				 	xaxis = list(title = "k"),
				 	yaxis = list(title = "eps"),
				 	zaxis = list(title = "minpts")
				 ))

# plot_ly(
# 	df_nd_sorted, x = ~k, y = ~eps, z = ~minpts, color = ~sil,
# 	type = 'scatter3d', mode = 'markers',
# 	marker = list(size = 5)
# ) %>%
# 	layout(title = paste0("Silhoutte coefficient for 'nd'",
# 												" (buffer", char_buffer, "_", trip_file, ")"),
# 				 scene = list(
# 				 	xaxis = list(title = "k"),
# 				 	yaxis = list(title = "eps"),
# 				 	zaxis = list(title = "minpts")
# 				 ))
# 
# plot_ly(
# 	df_nd_sorted, x = ~k, y = ~eps, z = ~minpts, color = ~geom_sil,
# 	type = 'scatter3d', mode = 'markers',
# 	marker = list(size = 5)
# ) %>%
# 	layout(title = paste0("Geometric Silhoutte coefficient for 'nd'",
# 												" (buffer", char_buffer, "_", trip_file, ")"),
# 				 scene = list(
# 				 	xaxis = list(title = "k"),
# 				 	yaxis = list(title = "eps"),
# 				 	zaxis = list(title = "minpts")
# 				 ))

# Euclidean distance
plot_ly(
	df_euclid_sorted, x = ~k, y = ~eps, z = ~minpts, color = ~adj_rand,
	type = 'scatter3d', mode = 'markers',
	marker = list(size = 5)
) %>%
	layout(title = paste0("Adjusted Rand Index for 'euclid'",
												" (buffer", char_buffer, "_", trip_file, ")"),
				 scene = list(
				 	xaxis = list(title = "k"),
				 	yaxis = list(title = "eps"),
				 	zaxis = list(title = "minpts")
				 ))
# 
# subplot(plot1, plot2, nrows = 1, shareX = FALSE, shareY = FALSE, titleX = TRUE, titleY = TRUE)
# 
# 
# plot_ly(
# 	df_euclid_sorted, x = ~k, y = ~eps, z = ~minpts, color = ~sil,
# 	type = 'scatter3d', mode = 'markers',
# 	marker = list(size = 5)
# ) %>%
# 	layout(title = paste0("Silhoutte coefficient for 'euclid'",
# 												" (buffer", char_buffer, "_", trip_file, ")"),
# 				 scene = list(
# 				 	xaxis = list(title = "k"),
# 				 	yaxis = list(title = "eps"),
# 				 	zaxis = list(title = "minpts")
# 				 ))
# 
# plot_ly(
# 	df_euclid_sorted, x = ~k, y = ~eps, z = ~minpts, color = ~geom_sil,
# 	type = 'scatter3d', mode = 'markers',
# 	marker = list(size = 5)
# ) %>%
# 	layout(title = paste0("Geometric Silhoutte coefficient for 'euclid'",
# 												" (buffer", char_buffer, "_", trip_file, ")"),
# 				 scene = list(
# 				 	xaxis = list(title = "k"),
# 				 	yaxis = list(title = "eps"),
# 				 	zaxis = list(title = "minpts")
# 				 ))
