Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")


################################################################################
# 1. PaCMAP data
################################################################################
df_res_pacmap <- read_rds(here::here(path_python, "df_res.rds"))
# char_dataset = "data_110001_115001"
# df_res_pacmap_sub <- df_res_pacmap %>%
# 	filter(dataset == char_dataset &  method == "hdbscan") %>%
# 		group_by(dataset, method, dist_measure, param) %>%
# 		summarise(avg_sil = mean(sil_score)) %>%
# 	as.data.frame()


df_res_pacmap_summary <- df_res_pacmap %>%
	group_by(method, dist_measure, param) %>%
	summarise(
		avg_sil = mean(sil_score),
		avg_cluster_size = mean(numb_cluster, na.rm = TRUE) # Berechnung des durchschnittlichen Clusterumfangs
	) %>%
	as.data.frame()

str(df_res_pacmap_summary)
	

ggplot(df_res_pacmap_summary, aes(x = param, y = avg_sil, color = dist_measure)) +
	geom_line(linewidth = 1) +  
	geom_point(size = 2) + 
	labs(
		title = paste0("Embedding labels on pacmap data"),		 	 
		x = "param",
		y = "avg silhouette",
		color = "distance measure"
	) +
	theme_minimal() + 
	facet_wrap(~method) +
	theme(
		text = element_text(size = 12), 
		legend.position = "none"    
	)
	
	
################################################################################
# 2. Original data
################################################################################
df_res_org <- read_csv("./python/hdbscan_results.csv") %>%
	as.data.frame()
df_res_org$dataset %>% unique()

df_res_org_sub <- df_res_org %>%
	filter(dataset == char_dataset)

df_res_org_summary <- df_res_org %>%
	group_by(dist_measure, min_cluster_size) %>%
	summarise(
		avg_sil = mean(sil_score),
		avg_cluster_size = mean(numb_cl, na.rm = TRUE) # Berechnung des durchschnittlichen Clusterumfangs
	) %>%
	as.data.frame()

ggplot(df_res_org_summary, aes(x = min_cluster_size, y = avg_sil, color = dist_measure)) +
	geom_line(linewidth = 1) +  
	geom_point(size = 2) + 
	labs(
		title = paste0("Original cluster labels on original dist mat"),		
		x = "min_cluster_size",
		y = "avg silhouette",
		color = "distance measure"
	) +
	theme_minimal() + 
	theme(
		text = element_text(size = 12), 
		legend.position = "bottom"    
	)




################################################################################
# 3. PaCMAP labels on original data
################################################################################
df_res_emb_cl_org_data <- read_csv("./python/hdbscan_emb_label_org_data.csv") %>%
	as.data.frame()
df_res_emb_cl_org_data$dataset %>% unique()

df_res_org_sub <- df_res_org %>%
	filter(dataset == char_dataset)

df_res_emb_cl_org_data_summary <- df_res_emb_cl_org_data %>%
	group_by(dist_measure, min_cluster_size) %>%
	summarise(
		avg_sil = mean(sil_score),
		avg_cluster_size = mean(numb_cl, na.rm = TRUE) # Berechnung des durchschnittlichen Clusterumfangs
	) %>%
	as.data.frame()

ggplot(df_res_emb_cl_org_data_summary, aes(x = min_cluster_size, y = avg_sil, color = dist_measure)) +
	geom_line(linewidth = 1) +  
	geom_point(size = 2) + 
	labs(
		title = "Embedding cluster labels on original dist mat",		 
		x = "min_cluster_size",
		y = "avg silhouette",
		color = "distance measure"
	) +
	theme_minimal() + 
	theme(
		text = element_text(size = 12), 
		legend.position = "bottom"    
	)
