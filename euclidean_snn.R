library(sf)
library(dplyr)
library(igraph)
library(irlba) 


sf_linestrings <- st_read(con,
													"cologne_voi_m06_d05_weekday_h17_18_5000_16_cluster_cleaned")  # Pfad zu Ihrem sf-Objekt


sf_test <- sf_linestrings %>%
	filter(cluster_id_final == 1)
distances <- st_distance(sf_test)
class(distances)
k <- 5
snn_matrix <- apply(distances, 1, function(row) {
	as.integer(rank(row) <= k + 1)  # +1, weil die Distanz zu sich selbst mit einbezogen wird
})

snn_matrix
snn_graph <- graph_from_adjacency_matrix(snn_matrix, 
																				 mode = "undirected", 
																				 weighted = TRUE)

# Clustering mit dem Walktrap-Algorithmus
clusters <- cluster_walktrap(snn_graph)
membership(clusters)

# Ergebnis
plot(clusters, snn_graph)
