Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

#################################################################################
# 1. Network data
################################################################################
# available_networks <- psql1_get_available_networks(con)
# print(available_networks)
# char_network <- available_networks[3, "table_name"]
# sf_network <- st_read(con, char_network) 
#################################################################################
# 2. Get original OD flow data based on subsets
################################################################################
available_schemas <- psql1_get_schemas(con)
print(available_schemas)
char_schema <- available_schemas[14, "schema_name"]
# sf_trips_sub <- st_read(
# 	con,
# 	query = paste0("SELECT * FROM ",
# 								 char_schema,
# 								 ".data"))
# 
# sf_trips_sub$flow_id_temp <- 1:nrow(sf_trips_sub)

#################################################################################
# 3. Density distribution plot (NETWORK DISTANCE)
################################################################################
# org_distmat <- np$load(here::here(path_python, 
# 																	char_schema,
# 																	"flow_manhattan_pts_network",
# 																	"dist_mat.npy"))
# 
# min_val <- min(org_distmat)
# max_val <- max(org_distmat)
# org_distmat_scaled <- (org_distmat - min_val) / (max_val - min_val)
# org_distmat_scaled[1:5,1:5]
# dim(org_distmat_scaled)


pacmap_distmat <- proxy::dist(np$load(here::here(path_python, 
																								 char_schema,
																								 "flow_manhattan_pts_network",
																								 "embedding_4d_1.npy"))) %>%
	as.matrix()

min_val <- min(pacmap_distmat)
max_val <- max(pacmap_distmat)
pacmap_distmat_scaled <- (pacmap_distmat - min_val) / (max_val - min_val)
pacmap_distmat_scaled[1:5,1:5]
dim(pacmap_distmat_scaled)

#org_distances <- org_distmat_scaled[upper.tri(org_distmat_scaled)]
pacmap_distances <- pacmap_distmat_scaled[upper.tri(pacmap_distmat_scaled)]
df <- data.frame(
								 #org_distance = org_distances,
								 pacmap_distance = pacmap_distances)
df_long <- pivot_longer(df, cols = everything(), 
												names_to = "distance_type", 
												values_to = "distance")

ggplot(df_long, aes(x = distance, fill = distance_type)) +
	geom_density(alpha = 0.5) +
	labs(title = "Density distribution of distances (Network)", x = "Distance", y = "Density") +
	theme_minimal()



#################################################################################
# 3. Density distribution plot (EUCLIDEAN DISTANCE)
################################################################################
pacmap_distmat <- proxy::dist(np$load(here::here(path_python,
																									char_schema,
																									"flow_manhattan_pts_euclid",
																									"embedding_3d_1.npy"))) %>%
	as.matrix()

min_val <- min(pacmap_distmat)
max_val <- max(pacmap_distmat)
pacmap_distmat_scaled <- (pacmap_distmat - min_val) / (max_val - min_val)
pacmap_distmat_scaled[1:5,1:5]
dim(pacmap_distmat_scaled)
gc()


org_distmat <- np$memmap(
	here::here(path_python,
						 char_schema,
						 "flow_manhattan_pts_euclid",
						 "dist_mat.npy"),
	dtype = "float32",
	mode = "r",
	shape = tuple(as.integer(nrow(pacmap_distmat_scaled)),
								as.integer(ncol(pacmap_distmat_scaled)))
)


min_val <- min(org_distmat)
max_val <- max(org_distmat)
org_distmat_scaled <- (org_distmat - min_val) / (max_val - min_val)
org_distmat_scaled[1:5,1:5]
dim(org_distmat_scaled)
gc()

org_distances <- org_distmat_scaled[upper.tri(org_distmat_scaled)]
pacmap_distances <- pacmap_distmat_scaled[upper.tri(pacmap_distmat_scaled)]
df <- data.frame(org_distance = org_distances,
								 pacmap_distance = pacmap_distances)
df_long <- pivot_longer(df, cols = everything(), 
												names_to = "distance_type", 
												values_to = "distance")

ggplot(df_long, aes(x = distance, fill = distance_type)) +
	geom_density(alpha = 0.5) +
	labs(title = "Density distribution of distances (Euclid)", x = "Distance", y = "Density") +
	theme_minimal()




gc()




# ratio_distmat_network <- pacmap_distmat_scaled/org_distmat_scaled
# ratio_distmat_network[is.nan(ratio_distmat_network)] <- 1
# ratio_distmat_network[1:8,1:8]
