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
int_kw <- c(8:10)
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
dt_pts_nd <- main_calc_flow_nd_dist_mat(sf_trips_sub, dt_network, dt_dist_mat)
dt_o_pts_nd <- dt_pts_nd$dt_o_pts_nd %>% as.data.table()
dt_d_pts_nd <- dt_pts_nd$dt_d_pts_nd %>% as.data.table()




rm(dt_pts_nd)


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

gc()
rm(dt_flow_nd)
dt_flow_nd <- dt_sym
dt_flow_nd <- dt_flow_nd %>%
	rename(from = flow_m,
				 to = flow_n)

setindex(dt_flow_nd, from, to)

################################################################################
# Test MDS
################################################################################
n <- nrow(sf_trips_sub)
matrix_flow_nd <- matrix(0, nrow = n, ncol = n)


matrix_flow_nd[dt_flow_nd$from + (dt_flow_nd$to - 1) * n] <- dt_flow_nd$distance
matrix_flow_nd[dt_flow_nd$to + (dt_flow_nd$from - 1) * n] <- dt_flow_nd$distance


# res <- cmdscale(matrix_flow_nd)
# 
# df_res <- data.frame(
# 	x = res[, 1],
# 	y = res[, 2]
# )




l <- nrow(sf_trips_sub)       # Klassische MDS-Größe
r <- 2         # Extrahiere 2 Dimensionen
s_points <- 1 # Anzahl der Punkte zum Kombinieren (5*r)
n_cores <- 14   # Anzahl der verwendeten Kerne (abhängig von deinem Rechner)

# Fast MDS ausführen
res_fast <- fast_mds(matrix_flow_nd, l, s_points, r, n_cores)
df_res_fast <- data.frame(
	x = res_fast$points[, 1],
	y = res_fast$points[, 2]
)
# Plot mit ggplot2
ggplot(df_res_fast, aes(x = x, y = y)) +
	geom_point(color = "blue", size = 1.5) + # Punkte hinzufügen
	theme_minimal() + # Minimalistisches Design
	labs(
		title = "MDS Plot",
		x = "x",
		y = "y"
	)


ggplot(df_res, aes(x = x, y = y)) +
	geom_point(color = "blue", size = 1.5) + # Punkte hinzufügen
	theme_minimal() + # Minimalistisches Design
	labs(
		title = "MDS Plot",
		x = "x",
		y = "y"
	)


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
t_end<- proc.time()
print(t_end - t_start)
table(dt_snn_pred_nd$cluster_pred)
gc()


################################################################################
# 4. Postprocess cluster results and write them into PSQL-DB
################################################################################
sf_cluster_nd_pred <- sf_trips_sub %>%
	left_join(dt_snn_pred_nd, by = c("flow_id" = "id"))
st_drop_geometry(sf_cluster_nd_pred)
st_geometry(sf_cluster_nd_pred) <- "line_geom"
sf_cluster_nd_pred <- sf_cluster_nd_pred %>%
	select(-origin_geom, -dest_geom, -o_closest_point, -d_closest_point)




ggplot(data = sf_cluster_nd_pred[sf_cluster_nd_pred$cluster_pred!=0,]) +
	geom_sf(data=sf_network) +
	geom_sf(aes(color = as.character(cluster_pred)), size = 1) +
	theme_minimal()


char_schema <- paste0(char_data, 
											"_kw_",
											paste0(int_kw, collapse = "_"),
											"_min", 
											dist_filter,
											"m")
query <- paste0("CREATE SCHEMA IF NOT EXISTS ", char_schema)
cat(query)
dbExecute(con, query)
char_table <- paste0("snn1_k",
										 int_k,
										 "_eps",
										 int_eps,
										 "_minpts",
										 int_minpts)


st_write(sf_cluster_nd_pred, con, Id(schema=char_schema, 
																					table = char_table))


