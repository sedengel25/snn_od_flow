Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

char_city <- "dd"
char_prefix_data <- "sr"
char_data <- paste0(char_prefix_data, "_", char_city)

sf_cluster_nd_pred <- st_read(con, paste0(char_data, "_cluster"))
st_geometry(sf_cluster_nd_pred)


sf_cluster_nd_pred <- sf_cluster_nd_pred %>%
	filter(cluster_pred != 0)

sf_cluster_nd_pred <- sf_cluster_nd_pred %>%
	mutate(
		start_point = lwgeom::st_startpoint(line_geom),
		end_point = lwgeom::st_endpoint(line_geom)
	)

sf_cluster_nd_pred_grouped <- sf_cluster_nd_pred %>%
	group_by(cluster_pred) %>%
	summarise(
		centroid_start = st_centroid(st_union(start_point)),
		centroid_end = st_centroid(st_union(end_point)),
		count = n(),
		.groups = 'drop'
	) 


sf_cluster_nd_pred_grouped <- sf_cluster_nd_pred_grouped %>%
	mutate(
		representative_line = st_sfc(st_linestring(rbind(st_coordinates(centroid_start), 
																										 st_coordinates(centroid_end))), 
																 crs = st_crs(sf_cluster_nd_pred))
	)

# Exportiere als GeoPackage
st_write(sf_cluster_nd_pred_grouped, 
				 con,
				 layer = "representative_lines")
