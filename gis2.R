library(sf)
library(circular)
library(Directional)
library(tidyverse)
library(cluster) 
library(mclust)
library(lwgeom)
library(data.table)
library(plotly)
library(GGally)
library(mlr3)
library(mlr3viz)
library(mlr3learners)
library(mlr3pipelines)
library(partykit)
library(ggparty)
library(aricode)
library(dbscan)
source("./src/config.R")
source("./src/functions.R")


analyze_linestrings <- function(data, cluster_id) {
	
	# Consider one cluster
	cluster_data <- data[data$cluster_id == cluster_id, ]
	
	# Get the angles (in degrees) of all single linestrings to the horizontal x-axis
	if (nrow(cluster_data) > 1) {
		
		
		### Basic features -----------------------------------------------
		num_linestrings <- nrow(cluster_data)
		lengths <- st_length(cluster_data) %>% as.numeric
		min_length <- min(lengths) 
		max_length <- max(lengths)
		mean_length <- mean(lengths)
		var_length <- var(lengths)
		
		### ODSMMR -----------------------------------------------
		start_coords <- st_geometry(cluster_data) %>% 
			lwgeom::st_startpoint() %>%
			sf::st_coordinates()
		
		end_coords <- st_geometry(cluster_data) %>% 
			lwgeom::st_endpoint() %>%
			sf::st_coordinates()
		
		start_wcss = kmeans(start_coords, centers = 1)$tot.withinss
		end_wcss = kmeans(end_coords, centers = 1)$tot.withinss
		max_var = max(start_wcss, end_wcss)
		min_var = min(start_wcss, end_wcss)
		ratio_var <- abs(max_var/min_var)
		ratio_var_norm <- ratio_var*num_linestrings
		
		### Circular Variance -----------------------------------------------
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
		mean_direction <- circular::mean.circular(angles_rad) %>% as.numeric()
		
		# Resultant = Pascal's idea of combining all vectors into one
		# Mean resultant length = Length of combined vecot/ number of vectors
		# (1 if parallel)
		mean_resultant_length <- circular::rho.circular(angles_rad)  
		
		# Mean variance = variation in the angles about the mean direction
		circ_var <- circular::var.circular(angles_rad)
		circ_var_norm = circ_var/num_linestrings


		### Size of bounding box -----------------------------------------------
		all_geom <- st_union(cluster_data)
		convex_hull <- st_convex_hull(all_geom)
		size <- st_area(convex_hull) %>% as.numeric
		size_norm <- size/num_linestrings

		### Number of intersection -----------------------------------------------
		intersection_matrix <- st_intersects(cluster_data, sparse = FALSE)
		intersection_count <- sum(intersection_matrix) - nrow(cluster_data) 
		
		return(data.frame(cluster_id = cluster_id, 
											mean_direction = mean_direction, 
											mean_resultant_length = mean_resultant_length, 
											circ_var = circ_var, 
											circ_var_norm = circ_var_norm,
											num_linestrings = num_linestrings, 
											min_length = min_length, 
											max_length = max_length, 
											mean_length = mean_length,
											var_length = var_length,
											min_var = min_var,
											max_var = max_var,
											ratio_var = ratio_var,
											ratio_var_norm = ratio_var_norm,
											size = size,
											size_norm = size_norm,
											intersection_count = intersection_count))
	} else {
		return(data.frame(cluster_id = cluster_id, 
											mean_direction = NA, 
											mean_resultant_length = NA, 
											circ_var = NA, 
											circ_var_norm = NA,
											num_linestrings = nrow(cluster_data), 
											min_length = NA, 
											max_length = NA, 
											mean_length = NA,
											var_length = NA,
											min_var = NA,
											max_var = NA,
											ratio_var = NA,
											ratio_var_norm = NA,
											size = NA,
											size_norm = NA,
											intersection_count = NA))
	}
}

df_features <- data.frame(cluster_id = integer(), 
												 mean_direction = numeric(), 
												 mean_resultant_length = numeric(), 
												 circ_var = numeric(), 
												 circ_var_norm = numeric(),
												 num_linestrings = integer(), 
												 min_length = numeric(),
												 max_length = numeric(), 
												 mean_length = numeric(),
												 var_length = numeric(),
												 min_var = numeric(),
												 max_var = numeric(),
												 ratio_var = numeric(),
												 ratio_var_norm = numeric(),
												 size = numeric(),
												 size_norm = numeric(),
												 intersection_count = integer())

sf_cluster <- st_read(con, 
											"cologne_voi_m06_d05_weekday_h17_18_5000_16_cluster_cleaned")

unique_clusters <- unique(sf_cluster$cluster_id)

for (cluster_id in unique_clusters) {
	cluster_results <- analyze_linestrings(sf_cluster, cluster_id)
	df_features <- rbind(df_features, cluster_results)
}


df_features <- df_features %>%
	filter(cluster_id != 0) 
# %>%
# 	select(cluster_id,
# 				 circ_var_norm,
# 				 ratio_var_norm)

df_labels <- readODS::read_ods("./data/external/snn_flow_clsuters_label.ods") %>%
	as.data.frame() %>%
	select(cluster, assigned_category)

df <- df_features %>%
	left_join(df_labels, by = c("cluster_id" = "cluster"))

head(df)
df_norm <- df[,-c(1,ncol(df))] %>% 
	mutate(across(.cols = everything(), 
								.fns = ~scale(.x, center = TRUE, scale = TRUE)))

df_norm$assigned_category <-  df$assigned_category
dt <- as.data.table(df_norm) %>%
	mutate(assigned_category = as.factor(assigned_category))

################################################################################
# 1. Decision Tree
################################################################################
task <- TaskClassif$new(id = "test", 
														 backend = dt, 
														 target = "assigned_category")

learner <- lrn("classif.rpart", predict_type = "prob", keep_model = TRUE)
learner$train(task)
autoplot(learner, type = "ggparty")

################################################################################
# 2. Random Forest
################################################################################
task <- TaskClassif$new(id = "test", 
												backend = dt, 
												target = "assigned_category")

learner <- lrn("classif.ranger", predict_type = "prob", importance = "impurity")
learner$train(task)

importance <- learner$model$variable.importance
importance_df <- data.frame(Feature = names(importance), Importance = importance)
importance_df <- importance_df[order(-importance_df$Importance),]
ggplot(importance_df, aes(x = reorder(Feature, Importance), y = Importance)) +
	geom_col(fill = "steelblue") +
	coord_flip() +  # Dreht das Diagramm für bessere Lesbarkeit der Feature-Namen
	labs(title = "Feature Importance",
			 x = "Features",
			 y = "Importance") +
	theme_minimal()

################################################################################
# 2. DBSCAN
################################################################################
set.seed(123)  

k <- 10
# Bestimme einen angemessenen Wert für eps
dbscan::kNNdistplot(df_norm[,-ncol(df_norm)], k = k)
eps <- 3
abline(h = eps, col = "red")  

dbscan_res <- dbscan::dbscan(df_norm[,-ncol(df_norm)], eps = eps, minPts = k)

dbscan_pred_clusters <- dbscan_res$cluster
table(dbscan_pred_clusters)
################################################################################
# 2. kmeans()
################################################################################
set.seed(123) 
k <- 7
kmeans_res <- kmeans(df_norm[,-ncol(df_norm)], centers = k)
kmeans_pred_clusters <- kmeans_res$cluster
true_labels <- df_norm$assigned_category
# Adjusted Rand Index
ari <- aricode::AMI(true_labels, kmeans_pred_clusters)
# Normalized Mutual Information
nmi <- aricode::NMI(true_labels, kmeans_pred_clusters)

print(paste("Adjusted Rand Index:", ari))
print(paste("Normalized Mutual Information:", nmi))

################################################################################
# 3. GMM
################################################################################
set.seed(123) 
gmm_result <- Mclust(df_norm, G = 7)
gmm_pred_clusters <- gmm_result$classification


ari <- AMI(true_labels, gmm_pred_clusters)
nmi <- NMI(true_labels, gmm_pred_clusters)

print(paste("Adjusted Rand Index:", ari))
print(paste("Normalized Mutual Information:", nmi))
