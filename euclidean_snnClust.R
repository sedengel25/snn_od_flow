source("./src/config.R")
source("./src/functions.R")

################################################################################
# 1. Read in synthetic data
################################################################################
path_files <- here::here("data",
												 "synthetic",
												 "network_distance")
char_files <- list.files(path_files)
char_files
sf_org <- read_rds(here::here(path_files, char_files[1]))

################################################################################
# 1. Read in real world data
################################################################################
# sf_origin <- st_read(con, "col_voi_m06_d05_weekday_h6_7_8_9_b5000_origin_mapped")
# sf_dest <- st_read(con, "col_voi_m06_d05_weekday_h6_7_8_9_b5000_dest_mapped")
# 
# sf_org <- st_read(con, "col_voi_m06_d05_weekday_h6_7_8_9_vis") %>%
# 	rename("flow_id" = "id",
# 				 "geometry" = "line_geom")
################################################################################
# 1. Get distances between all OD-points
################################################################################
main_calc_flow_euclid_dist_mat <- function(sf_org, lwgeom, st_startpoint, st_endpoint) {
  sf_org$origin <- lwgeom::st_startpoint(sf_org$geometry)
  sf_org$dest <- lwgeom::st_endpoint(sf_org$geometry)
  sf_origin_distances <- st_distance(sf_org$origin, by_element = FALSE)
  sf_dest_distances <- st_distance(sf_org$dest, by_element = FALSE)
  matrix_distances <- drop_units(sf_origin_distances + sf_dest_distances)
  matrix_geom_dist <- calc_geom_dist_mat(sf_org)
  matrix_distances <- matrix_geom_dist
}


################################################################################
# 2. Find a good value for k based on the RKD plot
################################################################################
int_k_max <- 50
#diag(matrix_distances) <- Inf



# plot_optimal_k(k_max = int_k_max,
# 							 matrix_flow_distances = matrix_distances)


int_k <- 20

################################################################################
# 3. Main
################################################################################
# Get a matrix where the columns represent the knn of the flow in the first column
matrix_knn_ind <- t(apply(matrix_distances, 1, r1_get_knn_ind, int_k + 1))
matrix_knn_dist <- t(apply(matrix_distances, 1, r1_get_knn_dist, int_k + 1))

num_flows <- nrow(matrix_knn_ind)
max_index <- max(matrix_knn_ind)  
binary_matrix <- matrix(0, nrow = num_flows, ncol = max_index)

# Fill binary matrix: 1 for flows that are knn of flow i
for (i in 1:num_flows) {
	binary_matrix[i, matrix_knn_ind[i, ]] <- 1
}

# Get number of shared knn
snn_matrix_euclid <- binary_matrix %*% t(binary_matrix)

eps <-  round(int_k*0.75)
minpts <- 16
# Calculate snn density per flow
snn_densities <- numeric(nrow(snn_matrix_euclid))
for (i in 1:nrow(matrix_knn_ind)) {
	knn_indices <- matrix_knn_ind[i, ]
	knn_indices <- knn_indices[!is.na(knn_indices)]
	if (length(knn_indices) > 0) {
		snn_densities[i] <- sum(snn_matrix_euclid[i, knn_indices] >= eps, na.rm = TRUE)
	}
}

# Execute the shared neaerest neighbor approach
set.seed(123)  

clusters <- sNNclust(snn_matrix_euclid, 
										 k = int_k, 
										 eps = eps,
										 minPts = minpts,
										 borderPoints = TRUE)
# Create dataframe to analyse the results
df_cluster <- data.frame(
	flow_id = 1:length(clusters$cluster),
	cluster_id = clusters$cluster,
	snn_density = snn_densities)


# Add the LINESTRING-geometry for visual evaluation
sf_snn_euclidean <- df_cluster %>%
	left_join(sf_org %>% select(flow_id, geometry), by = "flow_id") %>%
	st_as_sf()

################################################################################
# 4. Evaluate the results visually
################################################################################
df_sil_scores <- calc_geom_sil_score(sf_snn_euclidean[sf_snn_euclidean$cluster_id!=0,])

df_sil_scores <- df_sil_scores %>%
	group_by(cluster) %>%
	summarise(mean_sil_score = mean(sil_width)) %>%
	as.data.frame %>%
	arrange(desc(mean_sil_score))

top_clusters <- head(df_sil_scores$cluster, 20)
# df_sil_scores <- df_sil_scores[order(-df_sil_scores$sil_width), ]
# df_sil_scores$flow_id <- factor(df_sil_scores$flow_id, levels = df_sil_scores$flow_id)
# ggplot(data = df_sil_scores) +
# 	geom_point(aes(x = as.factor(flow_id), y = sil_width))


#calc_geom_sil_score(sf_trips_labelled[sf_trips_labelled$cluster_id!=0,])

# Ground truth:
ggplot(data = sf_trips_labelled[sf_trips_labelled$cluster_id!=0,]) +
	geom_sf(aes(color = as.character(cluster_id)), size = 1) +
	theme_void()

# Cluster result:
# ggplot(data = sf_snn_euclidean[sf_snn_euclidean$cluster_id %in% top_clusters,]) +
# 	geom_sf(aes(color = as.character(cluster_id), 
# 							alpha = snn_density, 
# 							size = snn_density)) +
# 	labs(title=paste0("k",int_k, "_eps", eps, "_minpts", minpts))

ggplot(data = sf_snn_euclidean[sf_snn_euclidean$cluster_id!=0,]) +
	geom_sf(aes(color = as.character(cluster_id), 
							alpha = snn_density, 
							size = snn_density)) +
	labs(title=paste0("k",int_k, "_eps", eps, "_minpts", minpts, " - euclidean_distance"))




df_snn_euclidean_eval <- sf_snn_euclidean %>%
	as.data.frame() %>%
	group_by(cluster_id) %>%
	summarise(mean_density = mean(snn_density)) %>%
	as.data.frame() 

df_snn_euclidean_eval <- df_snn_euclidean_eval %>%
	arrange(desc(mean_density))

top_clusters <- head(df_snn_euclidean_eval$cluster_id, 20)

ggplot(data = sf_snn_euclidean[sf_snn_euclidean$cluster_id %in% top_clusters,]) +
	geom_sf(aes(color = as.character(cluster_id), 
							alpha = snn_density, 
							size = snn_density)) +
	labs(title=paste0("k",int_k, "_eps", eps, "_minpts", minpts, "_euclid_top_cl_only"))


################################################################################
# 4. Evaluate the results numerically
################################################################################
ari <- mclust::adjustedRandIndex(sf_org$cluster_id, sf_snn_euclidean$cluster_id)
print(paste("Adjusted Rand Index:", ari))

