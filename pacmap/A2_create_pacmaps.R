Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")



all_dirs <- list.dirs(path = path_python, recursive = FALSE)

filtered_dirs <- all_dirs[grep("data", all_dirs)]

char_dist_measures <- c("flow_manhattan_pts_euclid",
												"flow_chebyshev_pts_euclid",
												"flow_euclid",
												"flow_manhattan_pts_network")
for(char_data in filtered_dirs){
	char_dist_measures <- list.files(char_data) 
	for(dist_measure in char_dist_measures[4]){
		
		folder <- here::here(char_data, dist_measure)
		print(folder)
		for(i in 1:10){
			if(dist_measure != "flow_manhattan_pts_network"){
				system2("python3", args = c(here::here(path_python,
																							 "pacmap_cpu.py"),
																		"--directory ", folder,
																		"--distance ", dist_measure,
																		"--n 5001 ",
																		"--i ", i),
								stdout = "", stderr = "")
			} else {
				system2("python3", args = c(here::here(path_python,
																							 "pacmap_ram.py"),
																		"--directory ", folder,
																		"--distance ", dist_measure,
																		"--i ", i),
								stdout = "", stderr = "")
			}
		}
	}
}