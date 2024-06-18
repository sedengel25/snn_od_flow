library(sf)
library(circular)
library(Directional)
library(tidyverse)
library(cluster) 
library(mclust)
library(lwgeom)
library(plotly)
source("./src/config.R")
source("./src/functions.R")

analyze_linestrings <- function(data, cluster_id) {
	
	# Consider one cluster
	cluster_data <- data[data$cluster_id == cluster_id, ]
	
	# Get the angles (in degrees) of all single linestrings to the horizontal x-axis
	if (nrow(cluster_data) > 1) {
		
		
		start_coords <- st_geometry(cluster_data) %>% 
			lwgeom::st_startpoint() %>%
			sf::st_coordinates()

		end_coords <- st_geometry(cluster_data) %>% 
			lwgeom::st_endpoint() %>%
			sf::st_coordinates()
		
		var_start <- var(start_coords)
		var_end <- var(end_coords)
		min_var <- min(var_start, var_end)
		max_var <- max(var_start, var_end)
		ratio_var <- abs(max_var/min_var)
		

		angles <- sapply(st_geometry(cluster_data), function(line) {
			coords <- st_coordinates(line)
			angle <- atan2(diff(coords[,2]), diff(coords[,1])) * (180 / pi)
			ifelse(angle < 0, angle + 360, angle)
		})
		
		angles_circular <- circular::circular(angles, 
																					units = "degrees", 
																					template = "geographics")
		# Convert degree to radiant
		angles_rad <- circular::conversion.circular(angles_circular, 
																								units = "radians")
		# Get the mean of the individual angles (mean direction)
		mean_direction <- circular::mean.circular(angles_rad)
		
		# Resultant = Pascal's idea of combining all vectors into one
		# Mean resultant length = Length of combined vecot/ number of vectors
		# (1 if parallel)
		mean_resultant_length <- circular::rho.circular(angles_rad)  
		
		# Mean variance = variation in the angles about the mean direction
		circ_var <- circular::var.circular(angles_rad)
		
		lengths <- st_length(cluster_data) %>% as.numeric
		min_length <- min(lengths) 
		max_length <- max(lengths)
		mean_length <- mean(lengths)
		var_length <- var(lengths)
		num_linestrings <- nrow(cluster_data)
		
		all_geom <- st_union(cluster_data)
		convex_hull <- st_convex_hull(all_geom)
		size <- st_area(convex_hull) %>% as.numeric
		size_norm <- size/num_linestrings
		
		return(data.frame(cluster_id = cluster_id, 
											mean_direction = mean_direction, 
											mean_resultant_length = mean_resultant_length, 
											circ_var = circ_var, 
											num_linestrings = num_linestrings, 
											min_length = min_length, 
											max_length = max_length, 
											mean_length = mean_length,
											var_length = var_length,
											min_var = min_var,
											max_var = max_var,
											ratio_var = ratio_var,
											size_norm = size_norm))
	} else {
		return(data.frame(cluster_id = cluster_id, 
											mean_direction = NA, 
											mean_resultant_length = NA, 
											circ_var = NA, 
											num_linestrings = nrow(cluster_data), 
											min_length = NA, 
											max_length = NA, 
											mean_length = NA,
											var_length = NA,
											min_var = NA,
											max_var = NA,
											ratio_var = NA,
											size_norm = NA))
	}
}

results_df <- data.frame(cluster_id = integer(), 
												 mean_direction = numeric(), 
												 mean_resultant_length = numeric(), 
												 circ_var = numeric(), 
												 num_linestrings = integer(), 
												 min_length = numeric(),
												 max_length = numeric(), 
												 mean_length = numeric(),
												 var_length = numeric(),
												 min_var = numeric(),
												 max_var = numeric(),
												 raio_var = numeric(),
												 size_norm = numeric())

sf_cluster <- st_read(con, 
											"cologne_voi_m06_d05_weekday_h17_18_5000_16_cluster_cleaned")
unique_clusters <- unique(sf_cluster$cluster_id)
for (cluster_id in unique_clusters) {
	cluster_results <- analyze_linestrings(sf_cluster, cluster_id)
	results_df <- rbind(results_df, cluster_results)
}

results_df <- results_df %>%
	filter(cluster_id != 0) %>%
	mutate(circ_var_norm = circ_var/num_linestrings) %>%
	arrange(circ_var_norm)



# ggplot(data = results_df, 
# 			 aes(x = reorder(cluster_id, circ_var), y = circ_var)) +
# 	geom_line(aes(group = 1)) +
# 	geom_point() +
# 	labs(x = "Cluster ID", y = "circ_var",
# 			 title = "circ_var of Line Angles by Cluster ID") +
# 	theme_minimal()


final_df <- results_df %>%
	select(cluster_id,
		# 		 mean_direction, 
		# 		 mean_resultant_length,
		#		 circ_var,
				 circ_var_norm,
				 num_linestrings,
				 # min_length,
				 # max_length,
				 mean_length,
				 # var_length,
				 min_var,
				 size_norm
				 # max_var,
				 # ratio_var
				 )

final_df_norm <- final_df[,-1] %>% 
	mutate(across(.cols = everything(), 
								.fns = ~scale(.x, center = TRUE, scale = TRUE)))


###K-mean
set.seed(42)  
k <- 4
kmeans_result <- kmeans(final_df_norm, centers = k)

final_df$cluster <- as.factor(kmeans_result$cluster)




ggplot(data = final_df, 
			 aes(x = reorder(cluster_id, circ_var_norm), 
			 		y = circ_var_norm, 
			 		color = as.factor(cluster))) +
	geom_point(size = 2) +  # Zeichnet größere Punkte für jede Beobachtung
	labs(x = "Cluster ID", y = "Circular Variance",
			 title = "Circular Variance of Line Angles by Cluster ID",
			 color = "Cluster") +
	theme_minimal() +
	theme(legend.position = "right")


ggplot(data = final_df, aes(x = circ_var_norm, y = mean_length, color = cluster)) +
	geom_point()

plot_ly(data = final_df, type = "scatter3d", mode = "markers",
				x = ~circ_var_norm, y = ~mean_length, z = ~size_norm,
				color = ~factor(cluster),
				marker = list(size = 5, opacity = 0.8)) %>%
	layout(title = "3D Plot of Clusters",
				 scene = list(xaxis = list(title = "Circular Variance"),
				 						 yaxis = list(title = "Mean Length"),
				 						 zaxis = list(title = "Size norm")))


# ###GMM
# model <- Mclust(as.matrix(final_df_norm))
# summary(model)
# plot(model, what = "classification")
# 
# 
# ###SNN
# snn_k <- 20
# snn_eps <- 5
# snn_minpts <- 50
# snn_result <- dbscan::sNNclust(final_df_norm,
# 															 k = snn_k,
# 															 eps = snn_eps,
# 															 minPts = snn_minpts
# 															 )
# 
# 
# final_df$cluster_snn <- snn_result$cluster
# ggplot(final_df, 
# 				aes(x = circ_var, y = min_var, color = as.factor(cluster_snn))) +
# 	geom_point() +
# 	theme_minimal(base_size = 15) +
# 	ggtitle(paste("SNN \nk:", snn_k, "eps:", snn_eps, "minPts:", snn_minpts)) +
# 	labs(x = "x",
# 			 y = "y",
# 			 color = "Cluster")
# 
# ggplot(data = final_df, 
# 			 aes(x = reorder(cluster_id, circ_var), 
# 			 		y = circ_var, 
# 			 		color = as.factor(cluster_snn))) +
# 	geom_point(size = 2) +  # Zeichnet größere Punkte für jede Beobachtung
# 	labs(x = "Cluster ID", y = "Circular Variance",
# 			 title = "Circular Variance of Line Angles by Cluster ID",
# 			 color = "Cluster") +
# 	theme_minimal() +
# 	theme(legend.position = "right")
head(results_df,50)
tail(results_df,50)



