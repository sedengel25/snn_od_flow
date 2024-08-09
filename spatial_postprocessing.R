Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

char_city <- "dd"
char_prefix_data <- "sr"
char_data <- paste0(char_prefix_data, "_", char_city)

df <- read_delim("/home/sebastiandengel/Dokumente/sr_dd_cluster_202408091614.csv") %>%
	as.data.frame()
sf_cluster_nd_pred <- st_as_sf(df, wkt = "line_geom" )
sf_cluster_nd_pred <- st_read(con, paste0(char_data, "_cluster"))
