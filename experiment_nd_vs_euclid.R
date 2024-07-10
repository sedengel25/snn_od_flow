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
char_dt_dist_mat <-  char_av_dt_dist_mat_files[2]
char_buffer <- strsplit(char_dt_dist_mat, "_")[[1]][2]
dt_dist_mat <- read_rds(here::here(
	char_path_dt_dist_mat,
	char_dt_dist_mat))
# m-Spalte runden und in Integer umwandeln
dt_dist_mat <- dt_dist_mat %>%
	mutate(m = round(m, 0) %>% as.integer())


# # Erzeuge eine Shared BigMatrix im RAM
# bigmat <- big.matrix(
# 	nrow = nrow(dt_dist_mat),
# 	ncol = ncol(dt_dist_mat),
# 	type = "integer",
# 	shared = TRUE,
# 	dimnames = list(NULL, colnames(dt_dist_mat))
# )
# 
# # Fülle die BigMatrix mit den Werten aus der Datentabelle
# bigmat[,] <- as.matrix(dt_dist_mat)
# 
# # Entferne die ursprüngliche Datentabelle, um RAM freizugeben
# rm(dt_dist_mat)

# 
# # Speichern des Deskriptor-Dateipfads für späteren Zugriff
# descFile <- "dist_mat.desc"
# desc <- describe(bigmat)
# dput(desc, file = descFile)
# 
# # Lade die BigMatrix erneut für gemeinsamen Speicherzugriff
# bigmat_shared <- attach.big.matrix(desc)
# rm(bigmat)
# gc()

# Initialisiere die std::map einmal
# Hinweis: Hier wird die Karte nur einmal geladen
# initialize_map_once(bigmat_shared@address)
# save_dist_map("dist_map.bin")


	
char_path_trips <- here::here("data", "experiment")
char_trip_files <- list.files(char_path_trips)
trip_file <- char_trip_files[1]


char_path_cpp_maps<- here::here("data", "input", "cpp_map_dist_mat")
char_map_file <- here::here(char_path_cpp_maps,
														paste0(char_city,
																	 "_",
																	 char_buffer,
																	 ".bin"))

################################################################################
# 2. Trip data 
################################################################################
sf_trips_labelled <- read_rds(here::here(char_path_trips, trip_file)) 

sf_trips_labelled$origin_geom <- lwgeom::st_startpoint(sf_trips_labelled$geometry)
sf_trips_labelled$dest_geom <- lwgeom::st_endpoint(sf_trips_labelled$geometry)


dt_origin <- sf_trips_labelled %>%
	st_set_geometry("origin_geom") %>%
	select(flow_id, origin_id, origin_geom) %>%
	rename("id" = "flow_id",
				 "id_edge" = "origin_id",
				 "geom" = "origin_geom") %>%
	as.data.table()

dt_origin <- add_dist_start_end(dt_origin)

dt_dest <- sf_trips_labelled %>%
	st_set_geometry("dest_geom") %>%
	select(flow_id, dest_id, dest_geom) %>%
	rename("id" = "flow_id",
				 "id_edge" = "dest_id",
				 "geom" = "dest_geom") %>% 
	as.data.table

dt_dest <- add_dist_start_end(dt_dest)



dt_network <- dt_network %>%
	select(source, target, id, geom_way)

################################################################################
# 3. Calculate network distances between OD flows and put it into a matrix
################################################################################
cores <- 14
RcppParallel::setThreadOptions(numThreads = cores)
matrix_flow_nd_dist <- parallel_process_networks(dt_origin, 
																								 dt_network,
																								 dt_dist_mat,
																								 cores)

stop("RcppParallel succesfull")
matrix_flow_euclid_dist <- main_calc_flow_euclid_dist_mat(sf_trips_labelled)
matrix_flow_euclid_dist_geom <- calc_geom_dist_mat(sf_trips_labelled, 
																										matrix_flow_euclid_dist)

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
	nvi = numeric(),
	sil = numeric(),
	geom_sil = numeric(),
	cdbw = numeric()
)

for(i in 1:nrow(param_grid)){
	print(i)
	int_k <- param_grid[i, "k"]
	int_eps <-  param_grid[i, "eps"]
	int_minpts <- param_grid[i, "minpts"]
	if((i %% 2) == 0) {
		dt_snn_pred_euclid <- snn_flow(flow_dist_mat = matrix_flow_euclid_dist,
																	 k = int_k,
																	 eps = int_eps,
																	 minpts = int_minpts)
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
		df_cluster_valid[i, "sil"] <- calc_sil_score(sf_trips = sf_snn_euclid)$sil
		df_cluster_valid[i, "geom_sil"] <- calc_sil_score(sf_trips = sf_snn_euclid)$geom_sil
	} else {
		dt_snn_pred_nd <- snn_flow(flow_dist_mat = matrix_flow_nd_dist,
															 k = int_k,
															 eps = int_eps,
															 minpts = int_minpts)
		
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
		df_cluster_valid[i, "sil"] <- calc_sil_score(sf_trips = sf_snn_nd)$sil
		df_cluster_valid[i, "geom_sil"] <- calc_sil_score(sf_trips = sf_snn_nd)$geom_sil
	}
}
write_rds(df_cluster_valid, here::here("data",
																			 "cluster_validation_results",
																			 paste0("cluster_val_ext_int_cdbw", trip_file)))

head(df_cluster_valid)
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
			 title = "Ground truth")

best_euclid_k <- df_euclid_sorted[1, "k"]
best_euclid_eps <- df_euclid_sorted[1, "eps"]
best_euclid_minpts <- df_euclid_sorted[1, "minpts"]

dt_snn_pred_euclid <- snn_flow(flow_dist_mat = matrix_flow_euclid_dist,
															 k = best_euclid_k,
															 eps = best_euclid_eps,
															 minpts = best_euclid_minpts)

sf_cluster_euclid_pred <- sf_trips_labelled %>%
	left_join(dt_snn_pred_euclid, by = c("flow_id" = "flow")) %>%
	select(cluster_pred, geometry)

ggplot(data = sf_cluster_euclid_pred[sf_cluster_euclid_pred$cluster_pred!=0,]) +
	geom_sf(data=st_as_sf(dt_network)) +
	geom_sf(aes(color = as.character(cluster_pred)), size = 1) +
	theme_minimal() +
	labs(color = "Cluster ID",
			 title = "Euclidean distance")

best_nd_k <- df_nd_sorted[1, "k"]
best_nd_eps <- df_nd_sorted[1, "eps"]
best_nd_minpts <- df_nd_sorted[1, "minpts"]

dt_snn_pred_nd <- snn_flow(flow_dist_mat = matrix_flow_nd_dist,
															 k = best_nd_k,
															 eps = best_nd_eps,
															 minpts = best_nd_minpts)

sf_cluster_nd_pred <- sf_trips_labelled %>%
	left_join(dt_snn_pred_nd, by = c("flow_id" = "flow")) %>%
	select(cluster_pred, geometry)


ggplot(data = sf_cluster_nd_pred[sf_cluster_nd_pred$cluster_pred!=0,]) +
	geom_sf(data=st_as_sf(dt_network)) +
	geom_sf(aes(color = as.character(cluster_pred)), size = 1) +
	theme_minimal() +
	labs(color = "Cluster ID",
			 title = "Network distance")




################################################################################
# 3d plot
################################################################################
# Network distance
plot_ly(
	df_nd_sorted, x = ~k, y = ~eps, z = ~minpts, color = ~adj_rand,
	type = 'scatter3d', mode = 'markers',
	marker = list(size = 5)
) %>%
	layout(title = "Adjusted Rand Index for 'nd'",
				 scene = list(
				 	xaxis = list(title = "k"),
				 	yaxis = list(title = "eps"),
				 	zaxis = list(title = "minpts")
				 ))

plot_ly(
	df_nd_sorted, x = ~k, y = ~eps, z = ~minpts, color = ~sil,
	type = 'scatter3d', mode = 'markers',
	marker = list(size = 5)
) %>%
	layout(title = "Silhoutte coefficient for 'nd'",
				 scene = list(
				 	xaxis = list(title = "k"),
				 	yaxis = list(title = "eps"),
				 	zaxis = list(title = "minpts")
				 ))

plot_ly(
	df_nd_sorted, x = ~k, y = ~eps, z = ~minpts, color = ~geom_sil,
	type = 'scatter3d', mode = 'markers',
	marker = list(size = 5)
) %>%
	layout(title = "Geometric Silhoutte coefficient for 'nd'",
				 scene = list(
				 	xaxis = list(title = "k"),
				 	yaxis = list(title = "eps"),
				 	zaxis = list(title = "minpts")
				 ))
# Euclidean distance
plot_ly(
	df_euclid_sorted, x = ~k, y = ~eps, z = ~minpts, color = ~adj_rand,
	type = 'scatter3d', mode = 'markers',
	marker = list(size = 5)
) %>%
	layout(title = "Adjusted Rand Index for 'euclid'",
				 scene = list(
				 	xaxis = list(title = "k"),
				 	yaxis = list(title = "eps"),
				 	zaxis = list(title = "minpts")
				 ))


plot_ly(
	df_euclid_sorted, x = ~k, y = ~eps, z = ~minpts, color = ~sil,
	type = 'scatter3d', mode = 'markers',
	marker = list(size = 5)
) %>%
	layout(title = "Silhoutte coefficient for 'euclid'",
				 scene = list(
				 	xaxis = list(title = "k"),
				 	yaxis = list(title = "eps"),
				 	zaxis = list(title = "minpts")
				 ))

plot_ly(
	df_euclid_sorted, x = ~k, y = ~eps, z = ~minpts, color = ~geom_sil,
	type = 'scatter3d', mode = 'markers',
	marker = list(size = 5)
) %>%
	layout(title = "Geometric Silhoutte coefficient for 'euclid'",
				 scene = list(
				 	xaxis = list(title = "k"),
				 	yaxis = list(title = "eps"),
				 	zaxis = list(title = "minpts")
				 ))
