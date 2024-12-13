Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Network data
################################################################################
available_networks <- psql1_get_available_networks(con)
print(available_networks)
char_network <- available_networks[2, "table_name"]
dt_network <- st_read(con, char_network) %>% as.data.table
sf_network <- st_as_sf(dt_network)
ggplot() +
	geom_sf(data=sf_network) 

as_sfnetwork(sf_network)

char_path_dt_dist_mat <- here::here("data", "input", "dt_dist_mat")
char_av_dt_dist_mat_files <- list.files(char_path_dt_dist_mat)
print(char_av_dt_dist_mat_files)
# stop("Have you chosen the right dist mat?")
char_dt_dist_mat <-  char_av_dt_dist_mat_files[20]
char_buffer <- "50000"
dt_dist_mat <- read_rds(here::here(
	char_path_dt_dist_mat,
	char_dt_dist_mat))


# m-Spalte runden und in Integer umwandeln
dt_dist_mat <- dt_dist_mat %>%
	mutate(m = round(m, 0) %>% as.integer())



available_mapped_trip_data <- psql1_get_mapped_trip_data(con)
print(available_mapped_trip_data)
char_data <- available_mapped_trip_data[2, "table_name"]
sf_trips <- st_read(con, char_data) %>%
	rename("origin_id" = "id_edge_origin",
				 "dest_id" = "id_edge_dest")

char_data <- substr(char_data, 1, nchar(char_data) - 7)
sf_trips$month <- lubridate::month(sf_trips$start_datetime)
sf_trips$week <- lubridate::week(sf_trips$start_datetime)
sf_trips$hour <- lubridate::hour(sf_trips$start_datetime)
sf_trips$weekday <- lubridate::wday(sf_trips$start_datetime, week_start = 1)
sf_trips <- sf_trips %>%
	arrange(start_datetime)
dist_filter <- 2000
int_kw <- c(9:11)
sf_trips_sub <- sf_trips %>%
	filter(week %in% int_kw) %>%
	filter(trip_distance >= dist_filter)
nrow(sf_trips_sub)
rm(sf_trips)
gc()

# if(char_prefix_data == "sr"){
# 	sf_trips <- sf_trips %>%
# 		filter(trip_distance > 2000)
# } else if(char_prefix_data == "nb"){

# } else if(char_prefix_data == "comb"){
# 	sf_trips_sr <- sf_trips %>%
# 		filter(source == "sr" & trip_distance > 2000) 
# 	
# 	sf_trips_nb <- sf_trips %>%
# 		filter(source =="nb" & trip_distance > 2000 & week %in% int_kw) %>%
# 		slice_head(n = nrow(sf_trips_sr))
# 	
# 	sf_trips <- rbind(sf_trips_sr, sf_trips_nb)
# }



sf_trips_sub$flow_id <- 1:nrow(sf_trips_sub)

sf_trips_sub <- sf_trips_sub %>% mutate(origin_id = as.integer(origin_id),
																				dest_id = as.integer(dest_id))





t_start <- proc.time()
################################################################################
# 3. Calculate network distances between OD flows and put it into a matrix
################################################################################
gc()
dt_pts_nd <- main_calc_flow_nd_dist_mat(sf_trips_sub, dt_network, dt_dist_mat)
rm(dt_dist_mat)
gc()
dt_o_pts_nd <- dt_pts_nd$dt_o_pts_nd %>% as.data.table()
dt_d_pts_nd <- dt_pts_nd$dt_d_pts_nd %>% as.data.table()




rm(dt_pts_nd)
gc()

dt_flow_nd <- merge(dt_o_pts_nd, dt_d_pts_nd, by = c("from", "to"))
rm(dt_o_pts_nd)
rm(dt_d_pts_nd)
gc()
dt_flow_nd[, distance := distance.x + distance.y]
dt_flow_nd <- dt_flow_nd[, .(flow_m = from, flow_n = to, distance)]


dt_sym <- rbind(
	dt_flow_nd,
	dt_flow_nd[, .(flow_m = flow_n, flow_n = flow_m, distance = distance)]
)
rm(dt_flow_nd)
gc()

dt_flow_nd <- dt_sym
rm(dt_sym)
gc()

dt_flow_nd <- dt_flow_nd %>%
	rename(from = flow_m,
				 to = flow_n)



# dbWriteTable(con, substr(char_dt_dist_mat, 
# 												 start = 1, 
# 												 stop = nchar(char_dt_dist_mat) - 4),
# 						 dt_flow_nd)

num_ids <- nrow(sf_trips_sub)
matrix_flow_nd <- matrix(0, nrow = num_ids, ncol = num_ids)
matrix_flow_nd[cbind(dt_flow_nd$from, dt_flow_nd$to)] <- dt_flow_nd$distance
matrix_flow_nd[cbind(dt_flow_nd$to, dt_flow_nd$from)] <- dt_flow_nd$distance
gc()



################################################################################
# pacmap-git
################################################################################
reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")
char_project_dir <- "/home/sebastiandengel/PaCMAP_JS/source/pacmap"
np_matrix_flow_nd <- np$array(matrix_flow_nd)
np$save(paste0(char_project_dir, "/nd_mat.npy"))
py_run_file(paste0(char_project_dir, "/pacmap.py"))
embedding <- np$load(paste0(char_project_dir, "/embedding.npy")) %>%
	as.data.frame()



df_embedding <- data.frame(
	x = embedding[, 1],
	y = embedding[, 2]
)

ggplot(df_embedding, aes(x = x, y = y)) +
	geom_point(alpha = 0.2) +  
	stat_density2d(aes(fill =after_stat(level)),
								 geom = "polygon",
								 alpha = 1,
								 adjust = 0.125,
								 n = 200) +
	scale_fill_viridis_c(option = "plasma") +  
	theme_minimal() +
	labs(
		title = "PACMAP based on network distance",
		x = "x",
		y = "y",
		fill = "Density"
	)





















################################################################################
# pacmap-conda
################################################################################
reticulate::py_install("pacmap", 
											 envname = "r-reticulate", 
											 method = "virtualenv",
											 pip = TRUE)
reticulate::use_virtualenv("r-reticulate", required = TRUE)
pacmap <- reticulate::import("pacmap")
np <- reticulate::import("numpy")
pacmap_model <- pacmap$PaCMAP(n_components = as.integer(2), 
															random_state = as.integer(42))


np_matrix_flow_nd <- np$array(matrix_flow_nd)

rm(matrix_flow_nd)
gc()
embedding <- pacmap_model$fit_transform(np_matrix_flow_nd)


df_embedding <- data.frame(
	x = embedding[, 1],
	y = embedding[, 2]
)

# ggplot(df_embedding, aes(x = x, y = y)) +
# 	geom_bin2d(bins = 150) +
# 	scale_fill_viridis(option = "plasma", direction = 1) +  # Kontrastreiche Palette
# 	theme_minimal() +
# 	labs(
# 		title = "Density Map (2D Binning mit Viridis)",
# 		x = "x",
# 		y = "y",
# 		fill = "Density"
# 	)


ggplot(df_embedding, aes(x = x, y = y)) +
	geom_point(alpha = 0.2) +  # Punkte anzeigen
	stat_density2d(aes(fill =after_stat(level)), 
								 geom = "polygon", 
								 alpha = 0.4,
								 adjust = 0.1,
								 n = 300) +  # Farbige Konturen
	scale_fill_viridis_c(option = "plasma") +  # Farbskala für die Dichte
	theme_minimal() +
	labs(
		title = "PACMAP based on network distance",
		x = "x",
		y = "y",
		fill = "Density"
	)


