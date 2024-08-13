Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Network data
################################################################################
char_city <- "dd"
char_prefix_data <- "comb"
char_data <- paste0(char_prefix_data, "_", char_city)

dt_network <- st_read(con, paste0(char_city,
																	"_2po_4pgr")) %>% as.data.table
sf_network <- st_as_sf(dt_network)

ggplot() +
	geom_sf(data=sf_network) 

char_path_dt_dist_mat <- here::here("data", "input", "dt_dist_mat")
char_av_dt_dist_mat_files <- list.files(char_path_dt_dist_mat)
print(char_av_dt_dist_mat_files)
# stop("Have you chosen the right dist mat?")
char_dt_dist_mat <-  char_av_dt_dist_mat_files[13]
char_buffer <- strsplit(char_dt_dist_mat, "_")[[1]][2]
dt_dist_mat <- read_rds(here::here(
	char_path_dt_dist_mat,
	char_dt_dist_mat))


# m-Spalte runden und in Integer umwandeln
dt_dist_mat <- dt_dist_mat %>%
	mutate(m = round(m, 0) %>% as.integer())



sf_trips <- st_read(con, char_data) %>%
	rename("origin_id" = "id_edge_origin",
				 "dest_id" = "id_edge_dest")

head(sf_trips)
sf_trips$month <- lubridate::month(sf_trips$start_datetime)
sf_trips$week <- lubridate::week(sf_trips$start_datetime)

sf_trips %>%
	arrange(start_datetime)
int_kw <- c(9,10,11)
if(char_prefix_data == "sr"){
	sf_trips <- sf_trips %>%
		filter(trip_distance > 2000)
} else if(char_prefix_data == "nb"){
	sf_trips <- sf_trips %>%
		filter(trip_distance > 2000) %>%
		filter(week %in% int_kw)
} else if(char_prefix_data == "comb"){
	sf_trips_sr <- sf_trips %>%
		filter(source == "sr" & trip_distance > 2000) 
	
	sf_trips_nb <- sf_trips %>%
		filter(source =="nb" & trip_distance > 2000 & week %in% int_kw) %>%
		slice_head(n = nrow(sf_trips_sr))
	
	sf_trips <- rbind(sf_trips_sr, sf_trips_nb)
}

nrow(sf_trips)
table(sf_trips$source)


sf_trips$flow_id <- 1:nrow(sf_trips)

sf_trips <- sf_trips %>% mutate(origin_id = as.integer(origin_id),
																dest_id = as.integer(dest_id))

plot(sf_trips$line_geom)
################################################################################
# 3. Calculate network distances between OD flows and put it into a matrix
################################################################################
dt_pts_nd <- main_calc_flow_nd_dist_mat(sf_trips, dt_network, dt_dist_mat)
dt_o_pts_nd <- dt_pts_nd$dt_o_pts_nd %>% as.data.table()
dt_d_pts_nd <- dt_pts_nd$dt_d_pts_nd %>% as.data.table()
rm(dt_pts_nd)


# dt_flow_nd <- dt_o_pts_nd %>%
# 	inner_join(dt_d_pts_nd, by = c("from" = "from", "to" = "to")) %>%
# 	mutate(distance = distance.x + distance.y) %>%
# 	select(flow_m = from, flow_n = to, distance) %>%
# 	as.data.table
# dt_flow_nd <- dt_flow_nd %>%
# 	group_by(flow_m) %>%
# 	mutate(row_id = row_number()) %>%
# 	ungroup() %>%
# 	as.data.table
# 
# 
# dt_flow_nd <- dt_flow_nd[,-4]

dt_flow_nd <- merge(dt_o_pts_nd, dt_d_pts_nd, by = c("from", "to"))
gc()
dt_flow_nd[, distance := distance.x + distance.y]
dt_flow_nd <- dt_flow_nd[, .(flow_m = from, flow_n = to, distance)]


dt_sym <- rbind(
	dt_flow_nd,
	dt_flow_nd[, .(flow_m = flow_n, flow_n = flow_m, distance = distance)]
)

gc()
dt_flow_nd <- dt_sym

int_k <- 40
int_eps <- 20
int_minpts <- 25

dt_snn_pred_nd <- snn_flow(sf_trips = sf_trips,
													 k = int_k,
													 eps = int_eps,
													 minpts = int_minpts,
													 dt_flow_distance = dt_flow_nd)

table(dt_snn_pred_nd$cluster_pred)


################################################################################
# 4. Postprocess cluster results and write them into PSQL-DB
################################################################################
sf_cluster_nd_pred <- sf_trips %>%
	left_join(dt_snn_pred_nd, by = c("flow_id" = "flow"))
st_drop_geometry(sf_cluster_nd_pred)
st_geometry(sf_cluster_nd_pred) <- "line_geom"
sf_cluster_nd_pred <- sf_cluster_nd_pred %>%
	select(-origin_geom, -dest_geom, -o_closest_point, -d_closest_point)




ggplot(data = sf_cluster_nd_pred[sf_cluster_nd_pred$cluster_pred!=0,]) +
	geom_sf(data=sf_network) +
	geom_sf(aes(color = as.character(cluster_pred)), size = 1) +
	theme_minimal()

if(char_prefix_data == "sr"){
	st_write(sf_cluster_nd_pred, con, paste0(char_data, "_cluster"))
} else {
	st_write(sf_cluster_nd_pred, con, paste0(char_data, "_cluster"))
}


