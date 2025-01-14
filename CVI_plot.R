Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")


path_cvnn <- "/home/sebastiandengel/cluster_validation/data"
csv_files <- list.files(path_cvnn, pattern = "cvi_results", full.names = TRUE)
df_res <- do.call(rbind, lapply(csv_files, read.csv))
str(df_res)

df_grouped <- df_res %>%
	group_by(Distance_Measure, Clustering_Type, Min_Cluster_Size) %>%
	summarise(
		n_cl = mean(Number_of_Clusters),
		n_cl_min = min(Number_of_Clusters),
		n_cl_max = max(Number_of_Clusters),
		n_noise = mean(Number_of_Noise_Points),
		n_noise_min = min(Number_of_Noise_Points),
		n_noise_max = max(Number_of_Noise_Points),
		cvnn_comp = mean(CVNN_comp),
		cvnn_sep = mean(CVNN_sep),
		dbcv = mean(DBCV),
		sil = mean(SIL),
		db = mean(DB),
		ch = mean(CH),
		.groups = "drop"
	) %>%
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

str(df_grouped)


# ggplot(df_r_ber, aes(x = trips)) +
# 	geom_ribbon(aes(ymin = min_nd_od, ymax = max_nd_od, fill = "min/max"), alpha = 0.3) +
# 	geom_line(aes(y = mean_nd_od, color = "mean"), size = 0.5) +
# 	geom_line(aes(y = min_nd_od, color = "min/max"), linetype = "dashed", color = "#CC79A7") +
# 	geom_line(aes(y = max_nd_od, color = "min/max"), linetype = "dashed", color = "#CC79A7")



ggplot(df_grouped, aes(x = Min_Cluster_Size, y = log(n_cl), color = Distance_Measure)) +
	geom_line() +
	labs(
		x = "Min Cluster Size",
		y = "log(Avg. Number of Clusters)",
		color = "Distance Measure"
	) +
	theme_minimal() +
	facet_wrap(~Clustering_Type, scales = "free_y") +
	theme(
		axis.title.x = element_text(size = 14, margin = margin(t = 10)), 
		axis.title.y = element_text(size = 14, margin = margin(r = 10)),
		strip.text.x = element_text(size = 12),                  
		panel.spacing = unit(1, "lines")          
	)



ggplot(df_res, aes(x = factor(Min_Cluster_Size), y = Number_of_Clusters, fill = Distance_Measure)) +
	geom_boxplot(outlier.shape = NA) +
	labs(
		x = "Min Cluster Size",
		y = "Number of Clusters",
		fill = "Distance Measure"
	) +
	theme_minimal() +
	facet_wrap(~Clustering_Type, scales = "free_y") +
	theme(
		axis.title.x = element_text(size = 14, margin = margin(t = 10)), 
		axis.title.y = element_text(size = 14, margin = margin(r = 10)),
		strip.text.x = element_text(size = 12),                  
		panel.spacing = unit(1, "lines")          
	)

ggplot(df_res, aes(x = factor(Min_Cluster_Size), y = log(Number_of_Clusters), fill = Distance_Measure)) +
	geom_boxplot(outlier.shape = NA) +
	labs(
		x = "Min Cluster Size",
		y = "log(Number of Clusters)",
		fill = "Distance Measure"
	) +
	theme_minimal() +
	facet_wrap(~Clustering_Type, scales = "free_y") +
	theme(
		axis.title.x = element_text(size = 14, margin = margin(t = 10)), 
		axis.title.y = element_text(size = 14, margin = margin(r = 10)),
		strip.text.x = element_text(size = 12),                  
		panel.spacing = unit(1, "lines")          
	)


ggplot(df_grouped, aes(x = Min_Cluster_Size, y = log(n_noise), color = Distance_Measure)) +
	geom_line() +
	labs(
		x = "Min Cluster Size",
		y = "log(Avg. Number of Noise-Flows)",
		color = "Distance Measure"
	) +
	theme_minimal() +
	facet_wrap(~Clustering_Type, scales = "free_y") +
	theme(
		axis.title.x = element_text(size = 14, margin = margin(t = 10)), 
		axis.title.y = element_text(size = 14, margin = margin(r = 10)),
		strip.text.x = element_text(size = 12),                  
		panel.spacing = unit(1, "lines")          
	)


ggplot(df_res, aes(x = factor(Min_Cluster_Size), y = Number_of_Noise_Points, fill = Distance_Measure)) +
	geom_boxplot(outlier.shape = NA) +
	labs(
		x = "Min Cluster Size",
		y = "Number of Noise-Flows",
		fill = "Distance Measure"
	) +
	theme_minimal() +
	facet_wrap(~Clustering_Type, scales = "free_y") +
	theme(
		axis.title.x = element_text(size = 14, margin = margin(t = 10)), 
		axis.title.y = element_text(size = 14, margin = margin(r = 10)),
		strip.text.x = element_text(size = 12),                  
		panel.spacing = unit(1, "lines")          
	)


ggplot(df_res, aes(x = factor(Min_Cluster_Size), y = log(Number_of_Noise_Points), fill = Distance_Measure)) +
	geom_boxplot(outlier.shape = NA) +
	labs(
		x = "Min Cluster Size",
		y = "log(Number of Noise-Flows)",
		fill = "Distance Measure"
	) +
	theme_minimal() +
	facet_wrap(~Clustering_Type, scales = "free_y") +
	theme(
		axis.title.x = element_text(size = 14, margin = margin(t = 10)), 
		axis.title.y = element_text(size = 14, margin = margin(r = 10)),
		strip.text.x = element_text(size = 12),                  
		panel.spacing = unit(1, "lines")          
	)

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
		axis.title.x = element_text(size = 14, margin = margin(t = 10)), 
		axis.title.y = element_text(size = 14, margin = margin(r = 10)),
		text = element_text(size = 12),
		strip.text.x = element_text(size = 12),                  # Spaltenbeschriftung oben
		strip.text.y = element_text(size = 12, angle = 0),       # Zeilenbeschriftung rechts
		panel.spacing = unit(1, "lines")                         # Abstand zwischen Panels
	)


#write_rds(df_res, file = "hdbscan_cvnn_dbcv_sil_ch_db_pacmap2_3_4d.rds")



