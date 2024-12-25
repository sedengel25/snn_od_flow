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
char_dt_dist_mat <-  char_av_dt_dist_mat_files[17]
char_buffer <- "2000"
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
dist_filter <- 1000
#int_kw <- c(9:11)
int_wday <- c(1:4)
int_hours <- c(16:18)
sf_trips_sub <- sf_trips %>%
	#filter(week %in% int_kw) %>%
	filter(trip_distance >= dist_filter) %>%
	filter(hour %in% int_hours) %>%
	filter(weekday %in% int_wday)
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

char_schema <- paste0(char_data, 
											"_min", 
											dist_filter,
											"m_hours",
											paste0(int_hours, collapse = "_"),
											"_wdays",
											paste0(int_wday, collapse = "_"),
											"m_buffer",
											char_buffer,
											"m")

query <- paste0("CREATE SCHEMA IF NOT EXISTS ", char_schema)
cat(query)
dbExecute(con, query)


st_write(sf_trips_sub, con, Id(schema=char_schema, 
												 table = "data"))

################################################################################
# 3. Calculate network distances between OD flows and put it into a matrix
################################################################################
gc()
dt_pts_nd <- main_nd_dist_mat_ram(sf_trips_sub, dt_network, dt_dist_mat)
rm(dt_dist_mat)
gc()
dt_o_pts_nd <- dt_pts_nd$dt_o_pts_nd %>% as.data.table()
dt_d_pts_nd <- dt_pts_nd$dt_d_pts_nd %>% as.data.table()
char_buffer



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
matrix_flow_nd <- matrix(99999, nrow = num_ids, ncol = num_ids)
matrix_flow_nd[cbind(dt_flow_nd$from, dt_flow_nd$to)] <- dt_flow_nd$distance
matrix_flow_nd[cbind(dt_flow_nd$to, dt_flow_nd$from)] <- dt_flow_nd$distance
gc()


################################################################################
# Test MDS
################################################################################
# n <- nrow(sf_trips_sub)
# matrix_flow_nd <- matrix(0, nrow = n, ncol = n)
# 
# 
# matrix_flow_nd[dt_flow_nd$from + (dt_flow_nd$to - 1) * n] <- dt_flow_nd$distance
# matrix_flow_nd[dt_flow_nd$to + (dt_flow_nd$from - 1) * n] <- dt_flow_nd$distance


# res <- cmdscale(matrix_flow_nd)
# 
# df_res <- data.frame(
# 	x = res[, 1],
# 	y = res[, 2]
# )




# l <- nrow(sf_trips_sub)       # Klassische MDS-Größe
# r <- 2         # Extrahiere 2 Dimensionen
# s_points <- 1 # Anzahl der Punkte zum Kombinieren (5*r)
# n_cores <- 14   # Anzahl der verwendeten Kerne (abhängig von deinem Rechner)
# 
# # Fast MDS ausführen
# res_fast <- fast_mds(matrix_flow_nd, l, s_points, r, n_cores)
# df_res_fast <- data.frame(
# 	x = res_fast$points[, 1],
# 	y = res_fast$points[, 2]
# )
# # Plot mit ggplot2
# ggplot(df_res_fast, aes(x = x, y = y)) +
# 	geom_point(color = "blue", size = 1.5) + # Punkte hinzufügen
# 	theme_minimal() + # Minimalistisches Design
# 	labs(
# 		title = "MDS Plot",
# 		x = "x",
# 		y = "y"
# 	)
# 
# 
# ggplot(df_res, aes(x = x, y = y)) +
# 	geom_point(color = "blue", size = 1.5) + # Punkte hinzufügen
# 	theme_minimal() + # Minimalistisches Design
# 	labs(
# 		title = "MDS Plot",
# 		x = "x",
# 		y = "y"
# 	)


################################################################################
# Algorithm
################################################################################
int_k <- 20
int_eps <- 10
int_minpts <- 12

dt_snn_pred_nd <- snn_flow(ids = sf_trips_sub$flow_id,
													 k = int_k,
													 eps = int_eps,
													 minpts = int_minpts,
													 dt_flow_distance = dt_flow_nd)

rm(dt_flow_nd)
gc()
t_end<- proc.time()
print(t_end - t_start)
table(dt_snn_pred_nd$cluster_pred)
gc()


################################################################################
# 4. Postprocess cluster results and write them into PSQL-DB
################################################################################
sf_snn <- sf_trips_sub %>%
	left_join(dt_snn_pred_nd, by = c("flow_id" = "id"))
st_drop_geometry(sf_snn)
st_geometry(sf_snn) <- "line_geom"
sf_snn <- sf_snn %>%
	select(-origin_geom, -dest_geom, -o_closest_point, -d_closest_point)




ggplot(data = sf_snn[sf_snn$cluster_pred!=0,]) +
	geom_sf(data=sf_network) +
	geom_sf(aes(color = as.character(cluster_pred)), size = 1) +
	theme_minimal()


# char_schema <- paste0(char_data, 
# 											"_kw_",
# 											paste0(int_kw, collapse = "_"),
# 											"_min", 
# 											dist_filter,
# 											
# 											"m_buffer",
# 											char_buffer,
# 											"m")


char_table <- paste0("snn1_k",
										 int_k,
										 "_eps",
										 int_eps,
										 "_minpts",
										 int_minpts)


st_write(sf_snn, con, Id(schema=char_schema, 
																					table = char_table))

################################################################################
# 5. kmeans
################################################################################
dt_kmeans <- data.table(
	id = sf_trips_sub$flow_id,
	ox = st_coordinates(sf_trips_sub$o_closest_point)[,1],
	oy = st_coordinates(sf_trips_sub$o_closest_point)[,2],
	dx = st_coordinates(sf_trips_sub$d_closest_point)[,1],
	yx = st_coordinates(sf_trips_sub$d_closest_point)[,2],
	line_geom = sf_trips_sub$line_geom
)

matrix_kmeans <- as.matrix(dt_kmeans[, !c("id", "line_geom"), with = FALSE])

k <- max(sf_cluster_nd_pred$cluster_pred)

set.seed(123) 
kmeans_result <- kmeans(matrix_kmeans, centers = k, nstart = 100)
kmeans_result$cluster
dt_kmeans[, cluster_pred := kmeans_result$cluster]
sf_kmeans <- st_as_sf(dt_kmeans)



sf_kmeans$cluster_pred %>% table() %>% as.numeric() %>% sum()
st_write(sf_kmeans, con, Id(schema=char_schema, 
														table = "kmeans"))
rm(dt_kmeans)
################################################################################
# 6. DBSCAN
################################################################################
int_minpts <- 7
k_distances <- kNNdist(as.dist(matrix_flow_nd), k = int_minpts - 1)

df_k_dist <- data.frame(
	point = seq_along(k_distances),
	distance = sort(k_distances)
)
df_k_dist <- df_k_dist %>%
	filter(distance < 99999)

ggplot(df_k_dist, aes(x = point, y = distance)) +
	geom_line(color = "blue") +
	geom_point(color = "red") +
	labs(
		title = paste0("k-Dist-Graph (minPts = ", int_minpts, ")"),
		x = "Punkte (sortiert)",
		y = paste0(int_minpts - 1, "-Distanz")
	) +
	theme_minimal()

int_eps <- 500
dbscan_res <- dbscan::dbscan(x = as.dist(matrix_flow_nd),
														 eps = int_eps,
														 minPts = int_minpts)
gc()
sf_dbscan <- data.frame(
	cluster_pred = dbscan_res$cluster,
	line_geom = sf_trips_sub$line_geom) %>%
	st_as_sf()

rm(dbscan_res)
gc()

st_write(sf_dbscan, con, Id(schema=char_schema, 
														table = paste0(
															"dbscan_eps",
															int_eps,
															"_minpts",
															int_minpts)))


################################################################################
# 7. HDBSCAN
################################################################################
gc()
hdbscan_minpts <- 30
hdbscan_res <- dbscan::hdbscan(x = as.dist(matrix_flow_nd),
															 minPts = hdbscan_minpts)
gc()



sf_hdbscan <- data.frame(
	flow_id = sf_trips_sub$flow_id,
	cluster_pred = hdbscan_res$cluster,
	line_geom = sf_trips_sub$line_geom) %>%
	st_as_sf()

gc()

st_write(sf_hdbscan, con, Id(schema=char_schema, 
														table = paste0(
															"hdbscan_minpts",
															hdbscan_minpts)))
################################################################################
# 8. OPTICS
################################################################################
gc()
optics_res <- dbscan::optics(x = as.dist(matrix_flow_nd),
														 eps = max(matrix_flow_nd),
														 minPts = 10)
gc()
plot(optics_res)
int_epscl <- 200
optics_res_cluster <- extractDBSCAN(optics_res, eps_cl = int_epscl)

# reachability <- optics_res$reachdist[optics_res$order]
# clusters <- optics_res_cluster$cluster[optics_res$order]
# plot(reachability, col = clusters + 1, pch = 20)


df_reach_plot_optics <- data.frame(
	order = seq_along(optics_res$order),
	reachability = optics_res$reachdist[optics_res$order],
	cluster = as.factor(optics_res_cluster$cluster[optics_res$order])
)


df_reach_plot_snn <- data.frame(
	order = seq_along(optics_res$order),
	reachability = optics_res$reachdist[optics_res$order],
	cluster = as.factor(sf_snn$cluster_pred[optics_res$order])
)

valid_colors <- colors()[!grepl("black|gray|grey", colors(), ignore.case = TRUE)]
random_colors <- setNames(
	c("black", sample(valid_colors, length(unique_clusters) - 1)),  # Verwende gefilterte Farben
	unique_clusters
)


ggplot(df_reach_plot_snn[1:3000,], aes(x = order, y = reachability, fill = cluster)) +
	geom_bar(stat = "identity", width = 1) +
	scale_fill_manual(values = random_colors) +
	theme_minimal() +
	theme(legend.position = "none") +
	labs(title = "Reachability Plot", 
			 x = "Data Points (Ordering)", 
			 y = "Reachability Distance",
			 fill = "Cluster")






sf_optics <- data.frame(
	flow_id = sf_trips_sub$flow_id,
	cluster_pred = optics_res_cluster$cluster,
	line_geom = sf_trips_sub$line_geom) %>%
	st_as_sf()

rm(optics_res)
rm(optics_res_cluster)
gc()

st_write(sf_optics, con, Id(schema=char_schema, 
														table = paste0(
															"optics_eps_cl",
															int_epscl)))
################################################################################
# 9. Silhouette Coefficient
################################################################################

get_sil_df <- function(cluster, matrix_flow_nd) {
  silhouette_values <- silhouette(cluster, as.dist(matrix_flow_nd))
  df_sil <- as.data.frame(silhouette_values)
  df_sil_per_cl <- df_sil %>%
  	group_by(cluster) %>%
  	summarise(sil_width_mean = mean(sil_width)) %>%
  	as.data.frame()
  
  
  df_sil_per_cl %>%
  	arrange(desc(sil_width_mean))
}
df_sil_kmeans <- get_sil_df(sf_kmeans$cluster_pred, matrix_flow_nd)
df_sil_snn <- get_sil_df(sf_cluster_nd_pred$cluster_pred, matrix_flow_nd)
df_sil_snn$cluster %>% sort
df_sil_dbscan <- get_sil_df(sf_dbscan$cluster, matrix_flow_nd)
df_sil_dbscan$cluster %>% sort
df_sil_optics <- get_sil_df(sf_optics$cluster, matrix_flow_nd)
df_sil_optics$cluster %>% sort
mean(df_sil_kmeans$sil_width_mean)
mean(df_sil_snn$sil_width_mean)
mean(df_sil_dbscan$sil_width_mean)
mean(df_sil_optics$sil_width_mean)
