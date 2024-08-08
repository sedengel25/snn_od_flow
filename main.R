Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

################################################################################
# 1. Network data
################################################################################
char_city <- "dd"
dt_network <- st_read(con, paste0(char_city,
																	"_2po_4pgr")) %>% as.data.table

ggplot() +
	geom_sf(data=st_as_sf(dt_network)) 

char_path_dt_dist_mat <- here::here("data", "input", "dt_dist_mat")
char_av_dt_dist_mat_files <- list.files(char_path_dt_dist_mat)
char_dt_dist_mat <-  char_av_dt_dist_mat_files[13]
print(char_dt_dist_mat)
char_buffer <- strsplit(char_dt_dist_mat, "_")[[1]][2]
dt_dist_mat <- read_rds(here::here(
	char_path_dt_dist_mat,
	char_dt_dist_mat))

summary(dt_dist_mat)
# m-Spalte runden und in Integer umwandeln
dt_dist_mat <- dt_dist_mat %>%
	mutate(m = round(m, 0) %>% as.integer())

char_data <- "sr_dd"

sf_trips <- st_read(con, char_data) %>%
	rename("origin_id" = "id_edge_origin",
				 "dest_id" = "id_edge_dest")

# sf_trips$month <- lubridate::month(sf_trips$start_time)
sf_trips <- sf_trips %>%
	filter(trip_distance > 2000)
# 	filter(month == 2)
	#filter(flow_id == 53491 | flow_id == 62197)

sf_trips$flow_id <- 1:nrow(sf_trips)

sf_trips <- sf_trips %>% mutate(origin_id = as.integer(origin_id),
																dest_id = as.integer(dest_id))
################################################################################
# 3. Calculate network distances between OD flows and put it into a matrix
################################################################################
dt_pts_nd <- main_calc_flow_nd_dist_mat(sf_trips, dt_network, dt_dist_mat)
dt_o_pts_nd <- dt_pts_nd$dt_o_pts_nd
dt_d_pts_nd <- dt_pts_nd$dt_d_pts_nd

# dt_o_pts_nd <- data.table::fread(input = paste0("./data/",
# 																						char_data,
# 																						"_",
# 																						char_buffer,
# 																						"_nd_origin.csv"),
# 														 showProgress = TRUE)
# 
# 
# dt_d_pts_nd <- data.table::fread(input = paste0("./data/",
# 																						char_data,
# 																						"_",
# 																						char_buffer,
# 																						"_nd_dest.csv"),
# 														 showProgress = TRUE)

dt_flow_nd <- dt_o_pts_nd %>%
	inner_join(dt_d_pts_nd, by = c("from" = "from", "to" = "to")) %>%
	mutate(distance = distance.x + distance.y) %>%
	select(flow_m = from, flow_n = to, distance) %>%
	as.data.table

dt_flow_nd <- dt_flow_nd %>%
	group_by(flow_m) %>%
	mutate(row_id = row_number()) %>%
	ungroup() %>%
	as.data.table


dt_flow_nd <- dt_flow_nd[,-4]
dt_sym <- rbind(
	dt_flow_nd,
	dt_flow_nd[, .(flow_m = flow_n, flow_n = flow_m, distance = distance)]
)
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



sf_cluster_nd_pred <- sf_trips %>%
	left_join(dt_snn_pred_nd, by = c("flow_id" = "flow"))

# st_geometry(sf_cluster_nd_pred) <- sf_cluster_nd_pred$line_geom
# sf_cluster_nd_pred <-  sf_cluster_nd_pred %>%
# 	select(cluster_pred, line_geom)
# 
# st_write(sf_cluster_nd_pred, con, paste0(char_data, "_sub_clustered"))
ggplot(data = sf_cluster_nd_pred[sf_cluster_nd_pred$cluster_pred!=0,]) +
	geom_sf(data=st_as_sf(dt_network)) +
	geom_sf(aes(color = as.character(cluster_pred)), size = 1) +
	theme_minimal()

st_write(sf_cluster_nd_pred, con, "cluster_test_sr_dd_large")
sf_final <- sf_cluster_nd_pred %>%
	left_join(sf_trips, by = c("id_new" = "id_new"))


indutry_trips <- sf_final %>%
	filter(cluster_pred %in% c(89,90)) 

table(indutry_trips$device_id)
unique(indutry_trips$device_id)

################################################################################
# Playground
################################################################################
sf_cluster_nd_pred <- st_read(con, "cluster_test_sr_dd_large")
st_geometry(sf_cluster_nd_pred) <- sf_cluster_nd_pred$line_geom
cluster_ids <- 1:max(sf_cluster_nd_pred$cluster_pred)

# Leere Liste für Polygone
polygon_list <- list()

# Loop durch die Cluster und berechne die konvexen Hüllen
for (cluster_id in cluster_ids) {
	print(cluster_id)
	polygon <- sf_cluster_nd_pred %>%
		filter(cluster_pred == cluster_id) %>%
		summarize(geometry = st_union(line_geom) %>% st_convex_hull())
	polygon_list[[as.character(cluster_id)]] <- polygon
}

polygons <- do.call(rbind, polygon_list)
polygons$cluster_id <- 1:max(sf_cluster_nd_pred$cluster_pred)

# Funktion zur Berechnung der Übereinstimmungsfläche zwischen zwei Polygonen
calculate_overlap <- function(poly1, poly2) {
	intersection <- st_intersection(poly1, poly2)
	if (length(intersection) == 0 || is.null(st_area(intersection))) {
		return(list(overlap = 0, intersection = NULL))
	} else {
		# Berechnen der Schnittfläche
		intersection_area <- st_area(intersection)
		# Berechnen der Überlappung für beide Polygone
		overlap1 <- as.numeric(intersection_area / st_area(poly1))
		overlap2 <- as.numeric(intersection_area / st_area(poly2))
		# Mitteln der beiden Überlappungen
		mean_overlap <- (overlap1 + overlap2) / 2
		return(list(overlap = mean_overlap, intersection = intersection))
	}
}

# Berechnen der Centroiden
polygons <- polygons %>% 
	mutate(centroid = st_centroid(geometry))

# Leere Liste für Ergebnisse
results <- list()
intersections <- list()

# Loop durch jedes Polygon
for (i in 1:nrow(polygons)) {
	print(i)
	poly1 <- polygons[i,]
	centroid1 <- poly1$centroid
	
	# Finden der Polygone innerhalb eines Radius von 500 Metern
	distances <- st_distance(polygons$centroid, centroid1)
	neighbors <- polygons %>%
		filter(distances < units::set_units(1000, "m") & row_number() != i)
	
	max_overlap <- 0
	best_match <- NULL
	best_intersection <- NULL
	
	# Berechnen der Übereinstimmungsfläche mit jedem Nachbarpolygon
	for (j in 1:nrow(neighbors)) {
		poly2 <- neighbors[j,]
		result <- calculate_overlap(poly1$geometry, poly2$geometry)
		
		if (result$overlap > max_overlap) {
			max_overlap <- result$overlap
			best_match <- poly2
			best_intersection <- result$intersection
		}
	}
	
	results[[i]] <- data.frame(
		polygon_id = i,
		best_match_id = ifelse(is.null(best_match), NA, best_match$cluster_id),
		max_overlap = max_overlap
	)
	
	if (!is.null(best_intersection)) {
		intersections[[i]] <- st_sf(
			polygon_id_1 = i,
			polygon_id_2 = ifelse(is.null(best_match), NA, best_match$cluster_id),
			geometry = best_intersection
		)
	}
}

# Ergebnisse in einen DataFrame konvertieren
results_df <- do.call(rbind, results)
results_df
results_df %>%
	arrange(desc(max_overlap))


results_df %>%
	filter(max_overlap <= 0.7)


intersections_sf <- do.call(rbind, intersections)
intersections_sf <- st_set_crs(intersections_sf, st_crs(polygons))
plot(st_geometry(polygons[c(162,163),]))

st_intersection(polygons[c(89,107),])

