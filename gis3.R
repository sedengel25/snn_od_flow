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
library(stats)
source("./src/config.R")
source("./src/functions.R")


sf_cluster <- st_read(con, 
											"cologne_voi_m06_d05_weekday_h17_18_5000_16_cluster_cleaned") %>%
	filter(cluster_id_final != 0)


################################################################################
# 1. Fourier Transformation
################################################################################
fourier_trafo <- function(sf_linestrings, cluster_id) {
	angles <- sapply(st_geometry(sf_linestrings), function(line) {
		coords <- st_coordinates(line)
		atan2(diff(coords[, 2]), diff(coords[, 1]))
	})
	
	ft <- fft(angles)
	abs_ft <- abs(ft)  
	top5 <- sort(abs_ft, decreasing = TRUE)[1:5]
	
	data.frame(cluster_id = cluster_id, 
						 ff_1 = top5[1], 
						 ff_2 = top5[2],
						 ff_3 = top5[3], 
						 ff_4 = top5[4],
						 ff_5 = top5[5])
}

plot_fft <- function(sf_linestrings, cluster_id){
	
	sf_linestrings <- sf_linestrings %>%
		filter(cluster_id_final == cluster_id)
	
	angles <- sapply(st_geometry(sf_linestrings), function(line) {
		coords <- st_coordinates(line)
		atan2(diff(coords[, 2]), diff(coords[, 1]))
	})
	
	ft <- fft(angles)
	abs_ft <- abs(ft)  
	return(
		plot(abs_ft, type = 'h')
	)
}

fft_res <- unique(sf_cluster$cluster_id_final) %>% 
	lapply(function(cluster_id) {
		cluster_data <- sf_cluster %>% filter(cluster_id_final == cluster_id)
		fourier_trafo(cluster_data, cluster_id)
	}) %>%
	bind_rows() 


fft_res$gap <- fft_res$ff_1 - fft_res$ff_2
fft_res <- fft_res %>% 
	arrange(desc(gap))
head(fft_res, 20)
tail(fft_res, 20)
ggplot(data = fft_res,
			 aes(x = reorder(cluster_id, gap), y = gap)) +
	geom_line(aes(group = 1)) +
	geom_point() +
	labs(x = "Cluster ID", y = "gap") +
	theme_minimal()
plot_fft(sf_cluster, 87)
################################################################################
# 2. PCA
################################################################################
# coords <- st_coordinates(sf_test)
# 
# pca <- prcomp(coords[,1:2], scale. = TRUE)
# 
# plot(pca$x[, 1:2], 
# 		 pch = 20, 
# 		 main = "PCA der Linestrings")
# abline(h = 0, v = 0, col = "grey")




# Parallel flows
base_angle <- pi / 4
n <- 20
set.seed(123)  
parallel_lines <- lapply(1:n, function(i) {
	start_x <- runif(1, 0, 10)
	start_y <- runif(1, 0, 10)
	length <- runif(1, 5, 10)
	angle_variation <- runif(1, -pi/36, pi/36)  # Kleine Variation um die Basisrichtung
	end_x <- start_x + cos(base_angle + angle_variation) * length
	end_y <- start_y + sin(base_angle + angle_variation) * length
	st_linestring(matrix(c(start_x, start_y, end_x, end_y), ncol = 2, byrow = TRUE))
})

parallel_sf <- st_sf(geometry = st_sfc(parallel_lines), crs = 4326)
plot(parallel_sf, col = 'blue')
plot_fft(parallel_sf)


random_lines <- lapply(1:n, function(i) {
	start_x <- runif(1, 0, 10)
	start_y <- runif(1, 0, 10)
	length <- runif(1, 5, 10)
	angle <- runif(1, 0, 2 * pi)  # Vollständig zufällige Richtung
	end_x <- start_x + cos(angle) * length
	end_y <- start_y + sin(angle) * length
	st_linestring(matrix(c(start_x, start_y, end_x, end_y), ncol = 2, byrow = TRUE))
})

random_sf <- st_sf(geometry = st_sfc(random_lines), crs = 4326)
plot(random_sf, col = 'red')
plot_fft(random_sf)
