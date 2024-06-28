source("./src/config.R")
source("./src/functions.R")

################################################################################
# 1. Read in synthetic data
################################################################################
path_files <- here::here("data",
												 "synthetic",
												 "network_distance")
char_files <- list.files(path_files)

sf_org <- read_rds(here::here(path_files, char_files[1]))

################################################################################
# 1. Get distances between all OD-points
################################################################################
sf_org$origin <- lwgeom::st_startpoint(sf_org$geometry)
sf_org$dest <- lwgeom::st_endpoint(sf_org$geometry)
sf_origin_distances <- st_distance(sf_org$origin, by_element = FALSE) 
sf_dest_distances <- st_distance(sf_org$dest, by_element = FALSE)


matrix_distances <- drop_units(sf_origin_distances + sf_dest_distances)
matrix_geom_dist <- calc_geom_dist_mat(sf_org)
matrix_distances <- matrix_geom_dist


################################################################################
# 2. Find a good value for k based on the RKD plot
################################################################################
int_k_max <- 50
#diag(matrix_distances) <- Inf



plot_optimal_k(k_max = int_k_max,
							 matrix_flow_distances = matrix_distances)


int_k <- 20

################################################################################
# 3. Main
################################################################################
# Get a matrix where the columns represent the knn of the flow in the first column
matrix_knn_ind <- t(apply(matrix_distances, 1, r1_get_knn_ind, int_k + 1))
matrix_knn_dist <- t(apply(matrix_distances, 1, r1_get_knn_dist, int_k + 1))
matrix_knn_ind
matrix_knn_dist
num_flows <- nrow(matrix_knn_ind)
max_index <- max(matrix_knn_ind)  
binary_matrix <- matrix(0, nrow = num_flows, ncol = max_index)

# Fill binary matrix: 1 for flows that are knn of flow i
for (i in 1:num_flows) {
	binary_matrix[i, matrix_knn_ind[i, ]] <- 1
}

# Get number of shared knn
snn_matrix <- binary_matrix %*% t(binary_matrix)


# Execute the shared neaerest neighbor approach
set.seed(123)  
clusters <- sNNclust(snn_matrix, 
										 k = int_k, 
										 eps = round(int_k*0.75),
										 minPts = 13,
										 borderPoints = TRUE)

# Create dataframe to analyse the results
df_cluster <- data.frame(
	flow_id = 1:length(clusters$cluster),
	cluster_id = clusters$cluster)

# Add the LINESTRING-geometry for visual evaluation
sf_snn <- df_cluster %>%
	left_join(sf_org %>% select(flow_id, geometry), by = "flow_id") %>%
	st_as_sf()

################################################################################
# 4. Evaluate the results
################################################################################
calc_geom_sil_score(sf_snn[sf_snn$cluster_id!=0,])
calc_geom_sil_score(sf_org[sf_org$cluster_id!=0,])

# Ground truth:
ggplot(data = sf_org[sf_org$cluster_id!=0,]) +
	geom_sf(aes(color = as.character(cluster_id)), size = 1) +
	theme_void()

# Cluster result:
ggplot(data = sf_snn[sf_snn$cluster_id!=0,]) +
	geom_sf(aes(color = as.character(cluster_id)), size = 1) +
	theme_void()
