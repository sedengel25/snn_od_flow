Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")


path_cvnn <- "/home/sebastiandengel/cluster_validation/data"
csv_files <- list.files(path_cvnn, pattern = "cvi_results", full.names = TRUE)
df_res <- do.call(rbind, lapply(csv_files, read.csv))
df_res$Distance_Measure %>% unique

df_grouped <- df_res %>%
	group_by(Distance_Measure, Clustering_Type, Min_Cluster_Size) %>%
	summarise(n_cl = mean(Number_of_Clusters),
						n_noise = mean(Number_of_Noise_Points),
						cvnn_comp = mean(CVNN_comp),
						cvnn_sep = mean(CVNN_sep),
						dbcv = mean(DBCV),
						sil = mean(SIL),
						db = mean(DB),
						ch = mean(CH)) %>%
	as.data.frame()

# Berechnung der normierten Werte und des cvnn_index
df_grouped <- df_grouped %>%
	group_by(Distance_Measure, Clustering_Type) %>%
	mutate(
		max_cvnn_comp = max(cvnn_comp, na.rm = TRUE),
		max_cvnn_sep = max(cvnn_sep, na.rm = TRUE),
		norm_cvnn_comp = cvnn_comp / max_cvnn_comp,
		norm_cvnn_sep = cvnn_sep / cvnn_sep,
		cvnn_index = cvnn_comp + cvnn_sep
	) %>%
	ungroup() %>%
	as.data.frame() %>%
	select(Distance_Measure, Clustering_Type, Min_Cluster_Size, n_cl, n_noise,
				 cvnn_index, dbcv, sil, db, ch)

df_grouped


# Daten ins lange Format bringen
df_long <- df_grouped %>%
	pivot_longer(
		cols = c(cvnn_index, dbcv, sil, db, ch),
		names_to = "cvi",
		values_to = "value"
	) %>%
	as.data.frame()
	





ggplot(df_long, aes(x = Min_Cluster_Size, y = value, color = Distance_Measure)) +
	geom_line() +
	labs(
		x = "Min Cluster Size",
		y = "CVI",
		color = "Distance Measure"
	) +
	theme_minimal() +
	ggh4x::facet_grid2(Clustering_Type ~ cvi, scales = "free_y", independent = "y") +
	theme(
		text = element_text(size = 12),
		strip.text.x = element_text(size = 12),                  # Spaltenbeschriftung oben
		strip.text.y = element_text(size = 12, angle = 0),       # Zeilenbeschriftung rechts
		panel.spacing = unit(1, "lines")                         # Abstand zwischen Panels
	)






#write_rds(df_res, file = "hdbscan_cvnn_k20.rds")
reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")
df_pacmap <- np$load(here::here(path_python, 
													"data_10001_15001",
													"flow_manhattan_pts_network",
													"embedding_3d_5.npy")) %>%
	as.data.frame()%>%
	rename("x" = "V1", "y" = "V2", "z" = "V3")

plot_ly(
	data = df_pacmap, 
	x = ~x, y = ~y, z = ~z, 
	type = "scatter3d", 
	mode = "markers", 
	marker = list(
		size = 4, 
		opacity = 0.25 # Transparenz der Punkte
	)
) %>%
	layout(
		scene = list(
			xaxis = list(title = "x"),
			yaxis = list(title = "y"),
			zaxis = list(title = "z")
		)
	)
