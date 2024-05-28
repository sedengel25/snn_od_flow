################################################################################
# Source
################################################################################
source("./src/config.R")
source("./src/functions.R")


################################################################################
# Selection
################################################################################
list.files(path = external_path_osm2po, pattern = "sh")

char_region <- "sachsen"

################################################################################
# Configuration
################################################################################
char_sh_file <- paste0(char_region, ".sh")

char_prefix_value <- sub(".*prefix=([^ ]+).*", "\\1", 
												 readLines(file.path(external_path_osm2po,
												 										char_sh_file)))

char_path_region <- here::here(external_path_osm2po, char_prefix_value)



char_network <-  paste0(char_prefix_value, "_2po_4pgr")

char_sql_filename <- paste0(char_network, ".sql")


################################################################################
# 1. Create routable network from given region
################################################################################
osm2po_create_routable_network()

# Definieren Sie die Pfade und Koordinaten
input_pbf <- "/pfad/zum/germany-latest.osm.pbf"
output_pbf <- "/pfad/zum/output.pbf"
lon_min <- 10.0
lat_min <- 50.0
lon_max <- 10.5
lat_max <- 50.5

# Erstellen Sie den osmconvert Befehl
command <- paste(
	"osmconvert", shQuote(input_pbf),
	"-b=", paste(lon_min, lat_min, lon_max, lat_max, sep=","),
	"-o=", shQuote(output_pbf)
)

# Befehl ausführen
system(command)


sf_network <- st_read(con, char_network)





