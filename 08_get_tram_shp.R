source("./src/config.R")

# Definiere die Bounding Box für München
munich_bbox <- getbb("München, Deutschland")

# OSM-Abfrage für Straßenbahnlinien definieren
tram_query <- opq(bbox = munich_bbox) %>%
	add_osm_feature(key = "railway", value = "tram")

# Daten herunterladen
tram_data <- osmdata_sf(tram_query)


tram_lines <- tram_data$osm_lines
# Speichere als Shapefile
st_write(tram_lines, con, layer = "mun_tram_network", delete_layer = TRUE)

