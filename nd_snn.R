source("./src/config.R")
source("./src/functions.R")

char_path_rds <- here::here("data", "synthetic", "network_distance")
char_files <- list.files(char_path_rds)
sf_trips_labelled <- read_rds(here::here(char_path_rds,
																				 char_files[1]))
sf_network <- st_read(con, "col_2po_4pgr")

n_clusters <- length(unique(sf_trips_labelled$cluster_id))
colors <- c(rgb(211/255, 211/255, 211/255, 0.5), rainbow(n_clusters))
names(colors) <- c("0", as.character(1:n_clusters))

ggplot(data = sf_trips_labelled) +
	geom_sf(data=sf_network) +
	geom_sf(aes(color = as.character(cluster_id)), size = 1) +
	scale_color_manual(values = colors) +
	theme_minimal() +
	labs(color = "Cluster ID")

