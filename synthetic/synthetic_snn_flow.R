Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

char_path_synthetic_data <- "./data/synthetic/network_distance/"
list.files(char_path_synthetic_data)
sf_trips <- read_rds(here(char_path_synthetic_data, "300_10000.rds"))

summary(sf_trips)
ggplot(data = sf_trips) +
	geom_sf(aes(color = as.character(cluster_id)), size = 1) +
	theme_minimal() +
	guides(color = "none") +
	labs(color = "Cluster ID")


psql1_get_available_networks(con)
char_network <- "dd_complete_2po_4pgr"
char_area <- paste0(strsplit(char_network, "_")[[1]][1:2], collapse = "_")
sf_network <- st_read(con, char_network) 
dt_network <- sf_network %>% as.data.table

char_path_dt_dist_mat <- here::here("data", "input", "dt_dist_mat")
char_av_dt_dist_mat_files <- list.files(char_path_dt_dist_mat, pattern = char_area)
print(char_av_dt_dist_mat_files)
char_dt_dist_mat <-  "dd_complete_2000_dist_mat"
dt_dist_mat <- read_rds(here::here(
	char_path_dt_dist_mat,
	paste0(char_dt_dist_mat, ".rds")))


dt_dist_mat <- dt_dist_mat %>%
	mutate(m = round(m, 0) %>% as.integer())

################################################################################
# 3. Calculate network distances between OD flows and put it into a matrix
################################################################################
str(dt_dist_mat)
dt_pts_nd <- main_calc_flow_nd_dist_mat_synth_data(sf_trips, dt_network, dt_dist_mat)
dt_o_pts_nd <- dt_pts_nd$dt_o_pts_nd %>% as.data.table()
dt_d_pts_nd <- dt_pts_nd$dt_d_pts_nd %>% as.data.table()
rm(dt_pts_nd)



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
int_minpts <- 22

dt_snn_pred_nd <- snn_flow(sf_trips = sf_trips,
													 k = int_k,
													 eps = int_eps,
													 minpts = int_minpts,
													 dt_flow_distance = dt_flow_nd)

table(dt_snn_pred_nd$cluster_pred)
gc()


################################################################################
# 4. Postprocess cluster results and write them into PSQL-DB
################################################################################
sf_cluster_nd_pred <- sf_trips %>%
	left_join(dt_snn_pred_nd, by = c("flow_id" = "flow"))

char_methods <- c("rand_index",
									"adjusted_rand_index",
									"jaccard_index",
									"fowlkes_mallows_index",
									"mirkin_metric",
									"purity",
									"entropy",
									"nmi",
									"nvi")

cluster_valid_euclid <- lapply(char_methods, function(x){
	idx_euclid <- ClusterR::external_validation(sf_cluster_nd_pred$cluster_id,
																							sf_cluster_nd_pred$cluster_pred,
																							method = x)
})

print(cluster_valid_euclid)
ggplot(data = sf_cluster_nd_pred[sf_cluster_nd_pred$cluster_pred!=0,]) +
	geom_sf(data=sf_network) +
	geom_sf(aes(color = as.character(cluster_pred)), size = 1) +
	theme_minimal() +
	guides(color = "none") +
	labs(title = "Predicted clusters")


ggplot(data = sf_cluster_nd_pred[sf_cluster_nd_pred$cluster_id!=0,]) +
	geom_sf(data=sf_network) +
	geom_sf(aes(color = as.character(cluster_id)), size = 1) +
	theme_minimal() +
	guides(color = "none") +
	labs(title = "True clusters")
