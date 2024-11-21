Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")
availabe_cluster_tables <- psql1_get_cluster_tables(con)
print(availabe_cluster_tables)
char_data_cluster <- availabe_cluster_tables[12, "table_name"]


sf_cluster <- st_read(con, char_data_cluster)

sf_cluster_origin <- sf_cluster %>%
	mutate(origin = lwgeom::st_startpoint(line_geom))
st_geometry(sf_cluster_origin) <- "origin"
sf_cluster_origin <- sf_cluster_origin %>%
	select(cluster_pred, origin, origin_id, o_dist_to_start, o_dist_to_end)
st_write(sf_cluster_origin, con, "origin_points_from_flows", delete_layer = TRUE)


sf_cluster_dest <- sf_cluster %>%
	mutate(dest = lwgeom::st_endpoint(line_geom))
st_geometry(sf_cluster_dest) <- "dest"
sf_cluster_dest <- sf_cluster_dest %>%
	select(cluster_pred, dest, dest_id, d_dist_to_start, d_dist_to_end)

st_write(sf_cluster_dest, con, "dest_points_from_flows", delete_layer = TRUE)

available_networks <- psql1_get_available_networks(con)
print(available_networks)
char_network <- available_networks[12, "table_name"]
sf_network <- st_read(con, char_network) 



################################################################################
# PLAYGROUND START
################################################################################
# int_cluster <- 205
# bbox <- st_bbox(sf_cluster_dest[sf_cluster_dest$cluster_pred == int_cluster, ])
# ggplot() +
# 	geom_sf(data = sf_network) +
# 	geom_sf(data = sf_cluster_dest[sf_cluster_dest$cluster_pred==int_cluster,], color = "red") +
# 	coord_sf(xlim = c(bbox["xmin"], bbox["xmax"]),
# 					 ylim = c(bbox["ymin"], bbox["ymax"]))
# 
# test <- sf_cluster_dest[sf_cluster_dest$cluster_pred == int_cluster, ]
# lof(st_coordinates(test), minPts = 20)
# 
# test$lof <- lof(st_coordinates(test), minPts = 20)
# test <- test %>%
# 	filter(lof<3)
# ggplot() +
# 	geom_sf(data = sf_network) +
# 	geom_sf(data =test, color = "red") +
# 	coord_sf(xlim = c(bbox["xmin"], bbox["xmax"]),
# 					 ylim = c(bbox["ymin"], bbox["ymax"]))

sf_cluster_origin_or <- clean_point_clusters_lof(sf_cluster = sf_cluster_origin)
sf_cluster_dest_or <- clean_point_clusters_lof(sf_cluster = sf_cluster_dest)




################################################################################
# PLAYGROUND END
################################################################################
sf_cluster_origin_or <- sf_cluster_origin_or %>%
	group_by(cluster_pred) %>%
	summarize(do_union = FALSE) %>%
	as.data.frame() %>%
	st_as_sf() %>%
	mutate(pol = st_convex_hull(origin))
st_geometry(sf_cluster_origin_or) <- "pol"
setnames(sf_cluster_origin_or, old = "origin", new = "points")

sf_cluster_dest_or <- sf_cluster_dest_or %>%
	group_by(cluster_pred) %>%
	summarize(do_union = FALSE) %>%
	as.data.frame() %>%
	st_as_sf() %>%
	mutate(pol = st_convex_hull(dest))
st_geometry(sf_cluster_dest_or) <- "pol"
setnames(sf_cluster_dest_or, old = "dest", new = "points")

sf_pol <- rbind(sf_cluster_origin_or, sf_cluster_dest_or)

sf_pol <- sf_pol %>%
	filter(cluster_pred!=0)

sf_pol <- sf_pol %>%
	filter(st_geometry_type(.) == "POLYGON")

sf_pol <- sf_pol %>% 
	mutate(cluster_pred = 1:nrow(sf_pol)) %>%
	as.data.frame() %>%
	rename(id = cluster_pred) %>%
	st_as_sf()



st_geometry(sf_pol) <- "points"
char_sf_pol <- paste0(char_data_cluster, "_pols")
st_write(sf_pol, con, char_sf_pol, 
				 layer_options = c("GEOMETRY_NAME=points", "GEOMETRY_TYPE=POINT"),
				 delete_layer = TRUE)
rm(sf_cluster)
rm(sf_cluster_dest)
rm(sf_cluster_origin)
