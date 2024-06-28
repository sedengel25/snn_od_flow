source("./src/config.R")
source("./src/functions.R")
Rcpp::sourceCpp("./src/helper_functions.cpp")






################################################################################
# 1. Prepare data
################################################################################
char_city <- "col"
dt_network <- st_read(con, paste0(char_city,
																	"_2po_4pgr")) %>% as.data.table

char_files <- list.files(here::here("data", "input"))
char_files
dt_dist_mat <- read_rds(here::here("data",
																	 "input",
																	 char_files[2]))
dt_dist_mat <- dt_dist_mat %>%
	mutate(m = round(m,0) %>% as.integer)

char_path_rds <- here::here("data", "synthetic", "network_distance")
char_files <- list.files(char_path_rds)


sf_trips_labelled <- read_rds(here::here(char_path_rds,
																				 char_files[1])) 


sf_trips_labelled$origin_geom <- lwgeom::st_startpoint(sf_trips_labelled$geometry)
sf_trips_labelled$dest_geom <- lwgeom::st_endpoint(sf_trips_labelled$geometry)
st_write(sf_trips_labelled, con, "test", delete_layer = TRUE)
# n_clusters <- length(unique(sf_trips_labelled$cluster_id))
# colors <- c(rgb(211/255, 211/255, 211/255, 0.5), rainbow(n_clusters))
# names(colors) <- c("0", as.character(1:n_clusters))
# 
# ggplot(data = sf_trips_labelled) +
# 	geom_sf(data=sf_network) +
# 	geom_sf(aes(color = as.character(cluster_id)), size = 1) +
# 	scale_color_manual(values = colors) +
# 	theme_minimal() +
# 	labs(color = "Cluster ID")


dt_origin <- sf_trips_labelled %>%
	st_set_geometry("origin_geom") %>%
	select(flow_id, origin_id, origin_geom) %>%
	rename("id" = "flow_id",
				 "id_edge" = "origin_id",
				 "geom" = "origin_geom") %>%
	as.data.table()

dt_origin <- add_dist_start_end(dt_origin)

dt_dest <- sf_trips_labelled %>%
	st_set_geometry("dest_geom") %>%
	select(flow_id, dest_id, dest_geom) %>%
	rename("id" = "flow_id",
				 "id_edge" = "dest_id",
				 "geom" = "dest_geom") %>% 
	as.data.table

dt_dest <- add_dist_start_end(dt_dest)



dt_network <- dt_network %>%
	select(source, target, id, geom_way)


################################################################################
# 2. Calc ND between OD points
################################################################################
int_n_od_pts <- nrow(dt_origin)

int_chunks <- r1_create_chunks(cores = int_cores, n = int_n_od_pts)

str(dt_origin)
str(dt_dest)
str(dt_network)
str(dt_dist_mat)
# ND betwenn origin points
dt_o_pts_nd <- parallel::mclapply(1:length(int_chunks), function(i) {
	id_start <- ifelse(i == 1, 1, int_chunks[i - 1] + 1)
	id_end <- int_chunks[i]
	
	cat("From ", id_start, " to ", id_end, "\n")
	
	process_networks(dt_od_pts_sub = dt_origin[id_start:id_end,],
									 dt_od_pts_full = dt_origin,
									 dt_network = dt_network,
									 dt_dist_mat = dt_dist_mat)
	
}, mc.cores = int_cores)
dt_o_pts_nd <- rbindlist(dt_o_pts_nd)

# ND between dest points
dt_d_pts_nd <- parallel::mclapply(1:length(int_chunks), function(i) {
	id_start <- ifelse(i == 1, 1, int_chunks[i - 1] + 1)
	id_end <- int_chunks[i]
	
	cat("From ", id_start, " to ", id_end, "\n")
	
	process_networks(dt_od_pts_sub = dt_dest[id_start:id_end,],
									 dt_od_pts_full = dt_dest,
									 dt_network = dt_network,
									 dt_dist_mat = dt_dist_mat)
	
}, mc.cores = int_cores)
dt_d_pts_nd <- rbindlist(dt_d_pts_nd)


# ND between OD flows
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

# Create distance matrix between flow
int_max_value <- max(dt_flow_nd$flow_m, dt_flow_nd$flow_n)

int_big_m <- 9e10
matrix_flow_nd <- matrix(int_big_m, nrow = int_max_value, ncol = int_max_value)


matrix_flow_nd[cbind(dt_flow_nd$flow_m, dt_flow_nd$flow_n)] <- dt_flow_nd$distance

################################################################################
# 2. Find a good value for k based on the RKD plot
################################################################################
int_k_max <- 50
#diag(matrix_distances) <- Inf



plot_optimal_k(k_max = int_k_max,
							 matrix_flow_distances = matrix_flow_nd)


int_k <- 20

################################################################################
# 3. Main
################################################################################
### CHECK HOW TO HANDLE INCOMPLETE KNNS HERE ###


# Get a matrix where the columns represent the knn of the flow in the first column
matrix_knn_ind <- t(apply(matrix_flow_nd, 1, r1_get_knn_ind, int_k))
matrix_knn_ind
