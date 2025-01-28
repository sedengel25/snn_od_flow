Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

#################################################################################
# 1. Get original OD flow data
################################################################################
available_schemas <- psql1_get_schemas(con)
print(available_schemas)
char_schema <- available_schemas[26, "schema_name"]
sf_data <- st_read(
	con,
	query = paste0("SELECT * FROM ", 
								 char_schema, 
								 ".data")) %>%
	mutate(cluster = as.integer(cluster_id))


psql1_get_available_networks(con)
sf_network <- st_read(con, "dd_synth_2po_4pgr_collapsed")
sf_network
# p <- ggplot(sf_data) +
# 	geom_sf(data = sf_network, aes(geometry = geom_way), 
# 					color = "black", size = 0.2) +
# 	geom_sf(aes(geometry = line_geom, color = as.factor(cluster))) +
# 	scale_color_discrete(name = "Cluster") +
# 	labs(
# 		title = "Visualization of Clusters",
# 		x = "Longitude",
# 		y = "Latitude"
# 	) +
# 	theme_minimal()
# ggplotly(p)

#################################################################################
# 2. Density distribution plots
################################################################################
char_embedding <- "embedding_4d_4nNB_0.4qsMN_0.6qsFP_0.5ratMN_2.0ratFP"




org_distmat <- np$load(here::here(path_python,
																					char_schema,
																					"flow_manhattan_pts_network",
																					"dist_mat.npy")) %>%
	as.matrix()


min_val <- min(org_distmat)
max_val <- max(org_distmat)
org_distmat <- (org_distmat - min_val) / (max_val - min_val)
org_network_distances <- org_distmat[upper.tri(org_distmat)]
n_samples <- nrow(org_distmat)
rm(org_distmat)
gc()



org_distmat <- np$load(here::here(path_python,
																	char_schema,
																	"flow_manhattan_pts_euclid",
																	"dist_mat.npy")) %>%
	as.matrix()


min_val <- min(org_distmat)
max_val <- max(org_distmat)
org_distmat <- (org_distmat - min_val) / (max_val - min_val)
org_euclid_distances <- org_distmat[upper.tri(org_distmat)]
rm(org_distmat)
gc()






pacmap_distmat <- proxy::dist(np$load(here::here(path_python, 
																								 char_schema,
																								 "flow_manhattan_pts_network",
																								 paste0(char_embedding, ".npy")))) %>%
	as.matrix()

min_val <- min(pacmap_distmat)
max_val <- max(pacmap_distmat)
pacmap_distmat <- (pacmap_distmat - min_val) / (max_val - min_val)
pacmap_network_distances <- pacmap_distmat[upper.tri(pacmap_distmat)]
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
pacmap_euclid_distances <- pacmap_distmat[upper.tri(pacmap_distmat)]
rm(pacmap_distmat)
gc()


df <- data.frame(org_network_distances = org_network_distances,
								 org_euclid_distances = org_euclid_distances,
								 pacmap_euclid_distances = pacmap_euclid_distances,
								 pacmap_network_distances = pacmap_network_distances)
rm(org_network_distances)
gc()
rm(org_euclid_distances)
gc()
rm(pacmap_euclid_distances)
gc()
rm(pacmap_network_distances)
gc()
df_long <- pivot_longer(df, cols = everything(),
												names_to = "distance_type",
												values_to = "distance")
rm(df)
gc()
p <- ggplot(df_long, aes(x = distance, fill = distance_type)) +
	geom_density(alpha = 0.6) + # Alpha für Transparenz
	labs(
		title = "Density Plots für verschiedene Distance-Typen",
		subtitle = char_embedding,
		x = "Distance",
		y = "Dichte",
		fill = "Distance Type"
	) +
	theme_minimal() +
	facet_wrap(~ distance_type, scales = "free", ncol = 2) + # 2x2 Layout
	theme(
		strip.text = element_text(size = 10, face = "bold"), # Facettentitel
		legend.position = "none" # Entfernt die Legende, da Facetten eindeutig sind
	)


filename_pdf <- paste0(char_embedding, "_density_comp.pdf")
print(filename_pdf)
p
ggsave(here::here(path_python, char_schema, filename_pdf), 
			 plot = p, 
			 device = "pdf", 
			 width = 8, 
			 height = 6)




