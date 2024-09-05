Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")





polygon_coords <- matrix(c(
	13.816595, 51.12529,
	13.83522, 51.11958,
	13.8517, 51.121681,
	13.860197, 51.126206,
	13.855648, 51.130084,
	13.842688, 51.136601,
	13.81505, 51.134232,
	13.816595, 51.12529
), ncol = 2, byrow = TRUE)

polygon <- st_polygon(list(polygon_coords))
polygon_sf <- st_sfc(polygon, crs = 4326)  # WGS 84


avaialble_trip_data <- psql1_get_mapped_trip_data(con)
print(avaialble_trip_data)
char_data <- avaialble_trip_data[6, "table_name"]
sf_trips <- st_read(con, char_data)


sf_trips$start_hour <- lubridate::hour(sf_trips$start_datetime)
sf_trips$start_min <- lubridate::minute(sf_trips$start_datetime)
sf_trips$end_hour <- lubridate::hour(sf_trips$end_datetime)
sf_trips$end_min <- lubridate::minute(sf_trips$end_datetime)
sf_trips$day <- lubridate::day(sf_trips$start_datetime)
sf_trips$weekday <- lubridate::wday(sf_trips$start_datetime,
																							week_start = 1)
sf_trips$date <- as.Date(sf_trips$start_datetime)




# sf_trips <- sf_trips %>%
# 	filter(weekday %in% c(1:5)) %>%
# 	filter(start_hour %in% c(6,7)) %>%
# 	filter(trip_distance > 2000)


sf_trips$start_day_min <- rescale(sf_trips$start_hour * 60 +
																		sf_trips$start_min)

polygon_sf <- st_transform(polygon_sf, st_crs(sf_trips))

start_points <- lwgeom::st_startpoint(sf_trips$line_geom)
end_points <- lwgeom::st_endpoint(sf_trips$line_geom)

flows_in_polygon <- sf_trips[which(
	st_intersects(start_points, polygon_sf, sparse = FALSE) | 
		st_intersects(end_points, polygon_sf, sparse = FALSE)
), ]


plot(flows_in_polygon$line_geom)
palette <- colorNumeric(palette = "viridis", domain = sf_trips$start_day_min)

st_geometry(flows_in_polygon) <- flows_in_polygon$line_geom
st_geometry(flows_in_polygon) 
flows_in_polygon_wgs84 <- st_transform(flows_in_polygon, crs = 4326)

leaflet(flows_in_polygon_wgs84) %>%
	addTiles() %>%  # Basiskarte hinzufügen
	addPolylines(color = "blue", weight = 2)

# Leaflet-Karte erstellen
leaflet(data = flows_in_polygon) %>%
	addTiles() %>%  # OSM-Basiskarte hinzufügen
	addCircleMarkers(~start_lng, ~start_lat, 
									 color = ~palette(start_day_min), 
									 fillOpacity = 0.7,
									 radius = 5,
									 popup = ~paste("Startzeit: ", start_datetime, "<br>",
									 							 "Start Longitude: ", start_lng, "<br>",
									 							 "Start Latitude: ", start_lat, "<br>",
									 							 "Start Day Min (scaled): ", round(start_day_min, 2))) %>%
	addLegend("bottomright", 
						pal = palette, 
						values = ~start_day_min,
						title = "Startzeit (Minuten, skaliert)",
						opacity = 1)
leaflet(sf_trips) %>%
	addTiles() %>%  # Basiskarte hinzufügen
	addPolylines(color = "blue", weight = 2)
