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
	select(cluster_pred, origin)
st_write(sf_cluster_origin, con, "origin_points_from_flows", delete_layer = TRUE)


sf_cluster_dest <- sf_cluster %>%
	mutate(dest = lwgeom::st_endpoint(line_geom))
st_geometry(sf_cluster_dest) <- "dest"
sf_cluster_dest <- sf_cluster_dest %>%
	select(cluster_pred, dest)

st_write(sf_cluster_dest, con, "dest_points_from_flows", delete_layer = TRUE)


sf_cluster_origin <- sf_cluster_origin %>%
	group_by(cluster_pred) %>%
	summarize(do_union = FALSE) %>%
	as.data.frame() %>%
	st_as_sf() %>%
	mutate(pol = st_convex_hull(origin))
st_geometry(sf_cluster_origin) <- "pol"
sf_cluster_origin <- sf_cluster_origin %>%
	select(-origin)
sf_cluster_dest <- sf_cluster_dest %>%
	group_by(cluster_pred) %>%
	summarize(do_union = FALSE) %>%
	as.data.frame() %>%
	st_as_sf() %>%
	mutate(pol = st_convex_hull(dest))
st_geometry(sf_cluster_dest) <- "pol"
sf_cluster_dest <- sf_cluster_dest %>%
	select(-dest)

sf_pol <- rbind(sf_cluster_origin, sf_cluster_dest)
sf_pol <- sf_pol %>%
	filter(cluster_pred!=0)

sf_pol <- sf_pol %>%
	filter(st_geometry_type(.) == "POLYGON")

sf_pol <- sf_pol %>% 
	mutate(cluster_pred = 1:nrow(sf_pol)) %>%
	rename(id = cluster_pred)


rm(sf_cluster)
rm(sf_cluster_dest)
rm(sf_cluster_origin)
