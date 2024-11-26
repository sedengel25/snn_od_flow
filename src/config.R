################################################################################
# Libraries
################################################################################
library(dplyr)
library(here)
library(processx)
library(yaml)
library(RPostgres)
library(parallel)
library(crayon)
library(sf)
library(reticulate)
library(data.table)
library(Rcpp)
library(RcppThread)
library(readr)
library(lubridate)
library(profvis)
library(bit64)
library(ggplot2)
library(lwgeom)
library(units)
library(scales)
library(dbscan)
library(mclust)
library(clue)
library(igraph)
library(ClusterR)
library(plotly)
library(clValid)
library(cluster)
library(fpc)
library(RANN)
library(Rfast2)
library(doParallel)
library(geosphere)
library(foreach)
library(bigstatsr)
library(tidyr)
library(prettymapr)
library(ggspatial)
library(leaflet)
library(reshape2)
library(osmdata)
library(sfnetworks)
library(tidygraph)
library(purrr)
library(concaveman)
################################################################################
# Postgresql-DB connection
################################################################################
config = yaml.load_file("config.yml")
host <- config$psql_db$host
user <- config$psql_db$user
pw <- config$psql_db$password
dbname <- config$psql_db$dbname
port <- config$psql_db$port




if(!exists("con")){
	con <- dbConnect(RPostgres::Postgres(), 
									 dbname = dbname, 
									 host = host, 
									 user = user, 
									 password = pw,
									 port = port,
									 options="-c client_min_messages=warning")
	print("Connection to PostgreSQL-DB establised")
}



char_os <-   Sys.info()["sysname"] %>% as.character()
int_cores <- detectCores()-2

RcppParallel::setThreadOptions(numThreads = int_cores)

################################################################################
# External software required
################################################################################
external_path_osm2po <- config$paths$path_osm2po
external_path_osmconvert <- config$paths$path_osmconvert
external_path_psql <- config$paths$path_psql
external_path_postgis <- config$paths$path_postgis
char_all_externalPaths <- ls(pattern = "^external")
list_external_paths <- mget(char_all_externalPaths)

for(i in 1:length(list_external_paths)){
	file <- list_external_paths[[i]]
	if(file.exists(file)){
		message(file, " exists. ", green("\u2713"))
	} else {
		message(file, " doesn't exist. ", red("\u274C"))
	}
}


log_check_postgis <- dbGetQuery(con, 
																"SELECT EXISTS(SELECT 1 FROM pg_extension 
																WHERE extname = 'postgis');") %>% pull

if (log_check_postgis) {
	message("PostGIS-extension for Postgresql exists. ", 
					green("\u2713"))
} else {
	message("PostGIS-extension for Postgresql getd created.")
	dbExecute(con, "CREATE EXTENSION postgis")
}


################################################################################
# Paths
################################################################################
path_bbox_coordinates <- "data/bbox_coordinates"

path_osm_pbf <- "data/osm_pbf"

path_osm_sql <- here::here("data/osm_sql")

path_input_data <- "data/input"
################################################################################
# File
################################################################################
file_ger_osm_pbf <- here::here(path_osm_pbf, 
															 "germany-latest.osm.pbf")  

file_ch_osm_pbf <- here::here(path_osm_pbf, 
															 "china-latest.osm.pbf")  

file_beijing_osm_pbf <- here::here(path_osm_pbf, 
															 "beijing-latest.osm.pbf")  

file_osm2po_jar <- here::here(external_path_osm2po,
															"osm2po-core-5.5.5-signed.jar")
################################################################################
# File-endings
################################################################################
ending_polygon <- ".poly"


################################################################################
# Folders
################################################################################
path_variables <- ls(pattern = "^path")

path_variables <- path_variables[-length(path_variables)]

lapply(mget(path_variables), function(char_paths) {
	print(char_paths)
	for (x in char_paths) {
		if (!dir.exists(x)){
			dir.create(x, recursive = TRUE)
		}
	}
})

