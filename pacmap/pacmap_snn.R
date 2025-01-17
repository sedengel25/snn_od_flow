Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

path_cvi <- "/home/sebastiandengel/cluster_validation/data"

all_dirs <- list.dirs(path = path_cvi, recursive = FALSE)

filtered_dirs <- all_dirs[grep("data", all_dirs)]

char_dist_measures <- c("flow_manhattan_pts_euclid",
												"flow_manhattan_pts_network")



snn_k <- c(5,10,12,14,16,18,20,25,30,40,50,75,100)
length(snn_k)

save_np <- function(i, j, folder, k, eps, minpts) {
	file <- paste0("embedding_",j, "d_", i)
	file_cl <- paste0(file, "_labels_k", k, ".npy")
	if(file.exists(here::here(folder,file_cl))){
		return(NULL)
	}
	embedding <- np$load(here::here(folder, paste0(file, ".npy"))) %>%
		as.data.frame
	dist_obj <- proxy::dist(embedding)
	snn_res <- sNNclust(dist_obj, k, eps, minpts)
	np_obj <- np$array(snn_res$cluster - 1, dtype = "int32")
	np$save(here::here(folder, file_cl), np_obj)
}

process_folder <- function(char_data) {
	for (dist_measure in char_dist_measures) {
		folder <- here::here(char_data, dist_measure)
		print(folder)
		if(dist_measure != "flow_manhattan_pts_network"){
			org_dist <- np$memmap(
				here::here(folder, "dist_mat.npy"), 
				dtype = "float32", 
				mode = "r", 
				shape = tuple(as.integer(5001), as.integer(5001)) 
			)
			
		} else {
			org_dist <- np$load(here::here(folder, "dist_mat.npy"))
		}
		
		for (k in snn_k) {
			minpts <- round(k / 2)
			eps <- minpts + 1
			file_cl <- paste0("org_labels_k", k, ".npy")
			if(file.exists(here::here(folder,file_cl))){
				next()
			}
			snn_res_org <- sNNclust(org_dist, k, eps, minpts)

			np_org <- np$array(snn_res_org$cluster - 1, dtype = "int32")
			np$save(here::here(folder, file_cl), np_org)
			# for (i in 1:10) {
			# 	for(j in 2:4){
			# 		gc()
			# 		save_np(i, j, folder, k, eps, minpts)
			# 		gc()
			# 	}
			# }
		}
	}
}
mclapply(filtered_dirs, process_folder, mc.cores = int_cores)
# embedding_3d <- embedding_3d %>%
# 	rename(x= V1, y = V2, z = V3)
# plot_ly(
# 	data = embedding_3d %>% filter(cluster!=0), 
# 	x = ~x, 
# 	y = ~y, 
# 	z = ~z, 
# 	color = ~factor(cluster), # Cluster einfärben
# 	type = "scatter3d", 
# 	mode = "markers", 
# 	marker = list(
# 		size = 4, 
# 		opacity = 0.25 # Transparenz der Punkte
# 	)
# ) %>%
# 	layout(
# 		scene = list(
# 			xaxis = list(title = "x"),
# 			yaxis = list(title = "y"),
# 			zaxis = list(title = "z")
# 		)
# 	)

