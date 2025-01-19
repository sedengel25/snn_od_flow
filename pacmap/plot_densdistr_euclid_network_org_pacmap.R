Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

#################################################################################
# 1. Get original OD flow data
################################################################################
available_schemas <- psql1_get_schemas(con)
print(available_schemas)
char_schema <- available_schemas[15, "schema_name"]

#################################################################################
# 2. Density distribution plots
################################################################################

for(i in 1:10){
	char_embedding <- paste0("embedding_4d_", i, "_seed") 
	pacmap_distmat <- proxy::dist(np$load(here::here(path_python, 
																									 char_schema,
																									 "flow_manhattan_pts_network",
																									 paste0(char_embedding, ".npy")))) %>%
		as.matrix()
	
	min_val <- min(pacmap_distmat)
	max_val <- max(pacmap_distmat)
	pacmap_distmat <- (pacmap_distmat - min_val) / (max_val - min_val)
	network_distances <- pacmap_distmat[upper.tri(pacmap_distmat)]
	rm(pacmap_distmat)
	gc()
	
	
	pacmap_distmat <- proxy::dist(np$load(here::here(path_python, 
																									 char_schema,
																									 "flow_manhattan_pts_euclid",
																									 paste0(char_embedding, ".npy")))) %>%
		as.matrix()
	
	min_val <- min(pacmap_distmat)
	max_val <- max(pacmap_distmat)
	pacmap_distmat <- (pacmap_distmat - min_val) / (max_val - min_val)
	euclid_distances <- pacmap_distmat[upper.tri(pacmap_distmat)]
	rm(pacmap_distmat)
	gc()
	
	
	df <- data.frame(euclid_dist = euclid_distances,
									 network_dist = network_distances)
	rm(euclid_distances)
	gc()
	rm(network_distances)
	gc()
	df_long <- pivot_longer(df, cols = everything(),
													names_to = "distance_type",
													values_to = "distance")
	rm(df)
	gc()
	p <- ggplot(df_long, aes(x = distance, fill = distance_type)) +
		geom_density(alpha = 0.5) +
		labs(title = "Density distribution of distances", x = "Distance", y = "Density") +
		theme_minimal()
	filename_pdf <- paste0(char_embedding, "_density_comp.pdf")
	print(filename_pdf)
	ggsave(here::here(path_python, char_schema, filename_pdf), 
				 plot = p, 
				 device = "pdf", 
				 width = 8, 
				 height = 6)
}



