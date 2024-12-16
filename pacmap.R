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
# 4. PaCMAP
################################################################################
reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")
char_project_dir <- "/home/sebastiandengel/PaCMAP_JS/source/pacmap"
# np_matrix_flow_nd <- np$array(matrix_flow_nd)
# np$save(paste0(char_project_dir, "/nd_mat.npy"))
#py_run_file(paste0(char_project_dir, "/pacmap.py"))
embedding <- np$load(paste0(char_project_dir, "/embedding.npy")) %>%
	as.data.frame()



df_embedding <- data.frame(
	x = embedding[, 1],
	y = embedding[, 2]
)
df_embedding$id <- 1:nrow(df_embedding)
ggplot(df_embedding, aes(x = x, y = y)) +
	geom_point(alpha = 0.1) +  
	stat_density2d(aes(fill =after_stat(level)),
								 geom = "polygon",
								 alpha = 1,
								 adjust = 0.1,
								 n = 150) +
	scale_fill_viridis_c(option = "plasma") +  
	theme_minimal() +
	labs(
		title = "PACMAP based on network distance",
		x = "x",
		y = "y",
		fill = "Density"
	)


################################################################################
# 5. Add cluster to PaCMAP
################################################################################
av_schemas <- psql1_get_schemas(con)
char_schema <- av_schemas[4, "schema_name"]
av_schema_tables <- psql1_get_tables_in_schema(con, char_schema)
char_data <- av_schema_tables[1, "table_name"]

sf_snn <- st_read(
	con,
	query = paste0("SELECT * FROM ",
								 char_schema,
								 ".",
								 char_data)
) 
df_snn <- sf_snn %>% as.data.frame()

if (nrow(df_snn) != nrow(df_embedding)){
	stop("Embedding and clsuter dataset have different dimensions")
}

df_pacmap <- df_embedding %>%
	left_join(df_snn %>% select(flow_id, cluster_pred),
						by = c("id" = "flow_id"))


int_clusters <- sort(unique(df_pacmap$cluster_pred))

valid_colors <- colors()[!grepl("black|gray|grey", colors(), ignore.case = TRUE)]
random_colors <- setNames(
	c("black", sample(valid_colors, length(int_clusters) - 1)),  # Verwende gefilterte Farben
	int_clusters
)


p <- ggplot(df_pacmap 
						%>% filter(cluster_pred != 0)
			 	, aes(x = x, y = y, color = factor(cluster_pred))) +
	geom_point(size = 1, alpha = 0.3) +
	scale_color_manual(values = random_colors) +  # Verwende scale_color_manual
	labs(
		title = "PaCMAP by Clusters",
		x = "x",
		y = "y"
	) +
	theme_minimal() +
	theme(
		legend.position = "none",
		plot.title = element_text(hjust = 0.5, size = 16),
		axis.title = element_text(size = 12)
	)


# Interaktives Plotly-Plot
ggplotly(p, tooltip = c("x", "y", "color"))

################################################################################
# 5. CDBW
################################################################################
# df_snn_cdbw <- st_coordinates(sf_snn) %>% as.data.frame()
# df_snn_cdbw <- df_snn_cdbw %>%
# 	mutate(row_id = row_number(), .by = L1) %>%   # Zeilen-ID pro Gruppe
# 	pivot_wider(
# 		names_from = row_id,
# 		values_from = c(X, Y),
# 		names_prefix = ""
# 	) %>%
# 	rename(ox = X_1, oy = Y_1, dx = X_2, dy = Y_2) %>%
# 	as.data.frame() %>%
# 	rename(flow_id = L1)
# 
# df_snn_cdbw$cluster_pred <- df_snn$cluster_pred
# 
# cdbv_idx_euclid <- cdbw(x = df_snn_cdbw[,c(2:5)],
# 												clustering = df_snn_cdbw[,6])
# cdbv_idx_euclid
# df_pacmap <- df_embedding %>%
# 	left_join(df_snn %>% select(flow_id, cluster_pred),
# 						by = c("id" = "flow_id"))
# 
# 
# cdbv_idx_pacmap <- cdbw(x = df_pacmap[,c(1,2)],
# 												clustering = df_pacmap[,4])
# 
# cdbv_idx_pacmap

################################################################################
# 6. Clustering of PaCMAP dimension reduced data
################################################################################
int_k <- 20
int_eps <- 10
int_minpts <- 12
df_pacmap_snn <- dbscan::sNNclust(df_embedding[,1:2], 
				 k = int_k,
				 eps = int_eps,
				 minPts = int_minpts)

df_pacmap_snn$cluster %>% unique

df_pacmap$cluster_pred_snn_pacmap <- df_pacmap_snn$cluster

p <- ggplot(df_pacmap 
						%>% filter(cluster_pred_snn_pacmap != 0)
						, aes(x = x, y = y, color = factor(cluster_pred_snn_pacmap))) +
	geom_point(size = 1, alpha = 0.3) +
	scale_color_manual(values = random_colors) +  # Verwende scale_color_manual
	labs(
		title = "PaCMAP by Clusters",
		x = "x",
		y = "y"
	) +
	theme_minimal() +
	theme(
		legend.position = "none",
		plot.title = element_text(hjust = 0.5, size = 16),
		axis.title = element_text(size = 12)
	)


# Interaktives Plotly-Plot
ggplotly(p, tooltip = c("x", "y", "color"))
cdbw(x = df_pacmap[,c(1,2)],
		 clustering = df_pacmap[,5])



sf_pacmap_snn <- df_snn %>%
	left_join(df_pacmap %>% select(id, cluster_pred_snn_pacmap), by = c("flow_id" = "id")) %>%
	st_as_sf()

char_table <- paste0("pacmap_snn_k",
										 int_k,
										 "_eps",
										 int_eps,
										 "_minpts",
										 int_minpts)


st_write(sf_pacmap_snn, con, Id(schema=char_schema, 
												 table = char_table))
