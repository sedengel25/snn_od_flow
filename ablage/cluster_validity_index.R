source("./src/config.R")
source("./src/functions.R")
library(units)
library(scales)

# Read in synthetic dataset 
sf_data <- read_rds("./data/synthetic/12_2000.rds") %>%
	filter(cluster_id != 0)


calc_geom_sil_score(sf_data)
sf_data <- sf_data %>%
	left_join(df_sil_scores, by = "flow_id") %>%
	select(cluster_id, flow_id, sil_width, geometry)

sf_data %>%
	arrange(sil_width)
# n_clusters <- length(
# 	unique(sf_data$cluster_id))
# colors <- c(rgb(211/255, 211/255, 211/255, 0.5), rainbow(n_clusters))
# names(colors) <- c("0", as.character(1:n_clusters))


ggplot(data = sf_data) +
	geom_sf(aes(color = as.character(cluster_id)), size = 1) +
	# scale_color_manual(values = colors) +
	#theme_minimal() +
	theme_void()


ggplot(data = sf_data) +
	geom_sf(aes(color = sil_width), size = 1) +  
	scale_color_gradient(low = "blue", high = "red") +  
	theme_void() +
	labs(color = "Sil Width")
