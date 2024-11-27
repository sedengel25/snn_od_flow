Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")
################################################################################
# Input
################################################################################
available_network_clusters <- psql1_get_network_clusters(con)
char_network_clusters <- available_network_clusters[1, "table_name"]
sf_network_clusters <- st_read(con, char_network_clusters)