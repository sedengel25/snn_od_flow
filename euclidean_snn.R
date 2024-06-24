source("./src/config.R")
source("./src/functions.R")
library(units)
library(scales)
library(dbscan)

# Read in synthetic dataset 
sf_org <- read_rds("./data/synthetic/12_2000.rds")
sf_org$origin <- lwgeom::st_startpoint(sf_org$geometry)
sf_org$dest <- lwgeom::st_endpoint(sf_org$geometry)
sf_origin_distances <- st_distance(sf_org$origin, by_element = FALSE) 
sf_dest_distances <- st_distance(sf_org$dest, by_element = FALSE)

# Get distance matrix for flows
matrix_distances <- drop_units(sf_origin_distances + sf_dest_distances)
matrix_geom_dist <- calc_geom_dist_mat(sf_org)
matrix_distances <- matrix_geom_dist
# Get optimal k
int_k_max <- 50
#diag(matrix_distances) <- Inf



plot_optimal_k(k_max = int_k_max)


int_k <- 16

matrix_knn_ind <- t(apply(matrix_distances, 1, r1_get_knn_ind, int_k_max))

num_flows <- nrow(matrix_knn_ind)
max_index <- max(matrix_knn_ind)  
binary_matrix <- matrix(0, nrow = num_flows, ncol = max_index)

# Fill binary matrix: 1 for flows that are knn of flow i
for (i in 1:num_flows) {
	binary_matrix[i, matrix_knn_ind[i, ]] <- 1
}

# Get number of shared knn
snn_matrix <- binary_matrix %*% t(binary_matrix)
set.seed(123)  
clusters <- sNNclust(snn_matrix, 
										 k = int_k, 
										 eps = round(int_k*0.75),
										 minPts = 13,
										 borderPoints = TRUE)
df_cluster <- data.frame(
	flow_id = 1:length(clusters$cluster),
	cluster_id = clusters$cluster)


sf_snn <- df_cluster %>%
	left_join(sf_org %>% select(flow_id, geometry), by = "flow_id") %>%
	st_as_sf()


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

# ggplot(data = sf_snn[sf_snn$cluster_id==22,]) +
# 	geom_sf(aes(color = as.character(cluster_id)), size = 1) +
# 	theme_void()
# 
table(sf_snn$cluster_id)
