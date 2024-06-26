################################################################################
# Source
################################################################################
source("./src/config.R")
source("./src/functions.R")
Rcpp::sourceCpp("./src/helper_functions.cpp")
library(lwgeom)


dt_trips <- st_read(con, "col_synth_12_3000")
#dt_origin <- lwgeom::st_startpoint()
################################################################################
# RCPP based nd calculation
################################################################################
p1 <- proc.time()
dt_o_pts_nd <- parallel::mclapply(1:length(int_chunks), function(i) {
	id_start <- ifelse(i == 1, 1, int_chunks[i - 1] + 1)
	id_end <- int_chunks[i]
	
	cat("From ", id_start, " to ", id_end, "\n")
	
	process_networks(dt_od_pts_sub = dt_origin[id_start:id_end,],
									 dt_od_pts_full = dt_origin,
									 dt_network = dt_network,
									 dt_dist_mat = dt_dist_mat)
	
}, mc.cores = int_cores)
p2 <- proc.time()
print(p2-p1)
