Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")
path_grass <- "/home/sebastiandengel/grassdata/utm32n/PERMANENT"
list.files(path = path_grass, pattern = "collapsed")
filename <- "dd_synth_2po_4pgr_collapsed"
shp_file <- paste0(filename, ".shp")
sf_network <- st_read(here::here(path_grass, shp_file)) %>%
	rename(id=cat) %>%
	select(id, geometry)

sf_network <- main_remove_dangling_edges(sf_network) %>%
	select(geometry)

n_roads <- 1:nrow(sf_network)
sf_network <- as_sfnetwork(sf_network)

# # Konvertiere die Edgelist in ein normales sf-Objekt
sf_network <- sf_network %>%
	activate("edges") %>%  # Aktiviere den "edges"-Teil des sfnetwork-Objekts
	as_tibble() %>%        # Konvertiere in ein tibble für Datenmanipulation
	mutate(
		id = n_roads,
		source = from,                      # source entspricht der Spalte 'from'
		target = to,                        # target entspricht der Spalte 'to'
		m = st_length(geometry) %>% as.numeric()            # Berechne die Länge der Geometrie (LINESSTRING)
	) %>%
	select(id, source, target, m, geometry) %>%
		as.data.frame() %>%
		st_as_sf()


st_write(obj = sf_network, dsn = con, layer = filename)
ggplot()+
	geom_sf(data=sf_network)
int_buffer=5000
dt_dist_mat <- calc_local_node_dist_mat(buffer = int_buffer)

igraph_dist_mat <- dt_dist_mat %>%
	as.data.frame() %>%
	rename("from" = "source",
				 "to" = "target") %>%
	select(from, to) %>%
	igraph::graph_from_data_frame()

igraph_dist_mat_components <- igraph::components(igraph_dist_mat)

cat("Total components: ",
		igraph_dist_mat_components$csize %>% sum, "\n")

cat("Biggest number of cohrerent components: ",
		max(igraph_dist_mat_components$csize), "\n")
write_rds(x = dt_dist_mat, file = paste0("./data/input/dt_dist_mat/",
																				 "dd_synth_collapsed_",
																				 int_buffer,
																				 "_dist_mat.rds"))
summary(sf_network$m)
