Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")



available_cluster_tables <- psql1_get_cluster_tables(con)
print(available_cluster_tables)
char_data <- available_cluster_tables[3, "table_name"]
sf_cluster_nd_pred <- st_read(con, char_data)
sf_cluster_nd_pred <- sf_cluster_nd_pred %>%
	filter(cluster_pred != 0)

sf_cluster_nd_pred$start_hour <- lubridate::hour(sf_cluster_nd_pred$start_datetime)
sf_cluster_nd_pred$start_min <- lubridate::minute(sf_cluster_nd_pred$start_datetime)
sf_cluster_nd_pred$end_hour <- lubridate::hour(sf_cluster_nd_pred$end_datetime)
sf_cluster_nd_pred$end_min <- lubridate::minute(sf_cluster_nd_pred$end_datetime)
sf_cluster_nd_pred$day <- lubridate::day(sf_cluster_nd_pred$start_datetime)
sf_cluster_nd_pred$weekday <- lubridate::wday(sf_cluster_nd_pred$start_datetime,
																							week_start = 1)
sf_cluster_nd_pred$date <- as.Date(sf_cluster_nd_pred$start_datetime)

# if(char_prefix_data == "nb"){
# 	char_data_cluster <- paste0(char_data, "_", 
# 															paste0(int_kw, collapse = "_"),
# 															"_cluster")
# 	sf_cluster_nd_pred <- st_read(con, char_data_cluster)
# 	
# 	sf_cluster_nd_pred$start_hour <- lubridate::hour(sf_cluster_nd_pred$start_datetime)
# 	sf_cluster_nd_pred$weekday <- lubridate::wday(sf_cluster_nd_pred$start_datetime,
# 																								week_start = 1)
# 	sf_cluster_nd_pred$date <- as.Date(sf_cluster_nd_pred$start_datetime)
# } else {
# 	char_data_cluster <- paste0(char_data,
# 															"_cluster")
# 	sf_cluster_nd_pred <- st_read(con, char_data_cluster)
# 	
# 	sf_cluster_nd_pred$start_time <- as.POSIXct(
# 		paste(
# 			sf_cluster_nd_pred$start_date,
# 			sf_cluster_nd_pred$start_time),
# 		format =  "%d.%m.%Y %H:%M:%S", tz = "UTC")
# 	
# 	sf_cluster_nd_pred$start_hour <- lubridate::hour(sf_cluster_nd_pred$start_datetime)
# 	sf_cluster_nd_pred$weekday <- lubridate::wday(sf_cluster_nd_pred$start_datetime,
# 																								week_start = 1)
# 	sf_cluster_nd_pred$date <- as.Date(sf_cluster_nd_pred$start_datetime)
# }

################################################################################
# Get clusters with most concentrated trips per day
################################################################################
dt_cluster_nd_pred <- sf_cluster_nd_pred %>% as.data.table
cluster_date_counts <- dt_cluster_nd_pred %>%
	group_by(cluster_pred, day) %>%
	summarise(fahrten_anzahl = n(), .groups = 'drop')

# Berechnung der Varianz der Startstunden pro Cluster
cluster_date_variance <- cluster_date_counts %>%
	group_by(cluster_pred) %>%
	summarise(varianz_date = var(fahrten_anzahl, na.rm = TRUE)) %>%
	arrange(desc(varianz_date))

# Die Cluster anzeigen, die die geringste Varianz bezüglich der Startstunden haben
print(cluster_date_variance)

dt_cluster_nd_pred %>%
	filter(cluster_pred == 126) %>%
	pull(day) %>%
	table()
################################################################################
# Get clusters with most concentrated trips per day
################################################################################
cluster_hourly_counts <- dt_cluster_nd_pred %>%
	group_by(cluster_pred, start_hour) %>%
	summarise(fahrten_anzahl = n(), .groups = 'drop')

# Berechnung der Varianz der Startstunden pro Cluster
cluster_hourly_variance <- cluster_hourly_counts %>%
	group_by(cluster_pred) %>%
	summarise(varianz_start_hour = var(fahrten_anzahl, na.rm = TRUE)) %>%
	arrange(desc(varianz_start_hour))

# Die Cluster anzeigen, die die geringste Varianz bezüglich der Startstunden haben
print(cluster_hourly_variance, n = 30)

dt_cluster_nd_pred %>%
	filter(cluster_pred == 20) %>%
	pull(start_hour) %>%
	table()

cluster_a <- c(150,118,77,54,53,33,47,50,34,74,115,133,109,51)   # Beispiel-Vektor für direction_a
cluster_b <- c(137,136,112,96,65,32,36,35,49,90,66,63,30)  # Beispiel-Vektor für direction_b
 
sf_cluster <- sf_cluster_nd_pred %>%
	filter(cluster_pred %in% cluster_a | cluster_pred %in% cluster_b) %>%
	mutate(direction = case_when(
		cluster_pred %in% cluster_a ~ "uk_neustadt",
		cluster_pred %in% cluster_b ~ "neustadt_uk",
		TRUE ~ NA_character_ 
	))
nrow(sf_cluster)

sf_cluster %>%
	filter(cluster_pred %in%  cluster_a) %>%
	summary()

sf_cluster %>%
	filter(cluster_pred %in%  cluster_b) %>%
	summary()
# sf_cluster_b_morning <- sf_cluster_nd_pred %>%
# 	filter(cluster_pred %in% cluster_b) %>%
# 	filter(start_hour %in% c(6,7,8,9))
# 
# sf_cluster_b_morning$start_day_min <- rescale(sf_cluster_b_morning$start_hour * 60 + 
# 	sf_cluster_b_morning$start_min)
# 
# sf_cluster_a_evening <- sf_cluster_nd_pred %>%
# 	filter(cluster_pred %in% cluster_a) %>%
# 	filter(start_hour %in% c(15,16,17))
# 
# sf_cluster_a_evening$end_day_min <- rescale(sf_cluster_a_evening$end_hour * 60 + 
# 																								sf_cluster_a_evening$end_min)
# 
# palette <- colorNumeric(palette = "viridis", domain = sf_cluster_b_morning$start_day_min)
# 
# # Leaflet-Karte erstellen
# leaflet(data = sf_cluster_b_morning) %>%
# 	addTiles() %>%  # OSM-Basiskarte hinzufügen
# 	addCircleMarkers(~start_lng, ~start_lat, 
# 									 color = ~palette(start_day_min), 
# 									 fillOpacity = 0.7,
# 									 radius = 5,
# 									 popup = ~paste("Startzeit: ", start_datetime, "<br>",
# 									 							 "Start Longitude: ", start_lng, "<br>",
# 									 							 "Start Latitude: ", start_lat, "<br>",
# 									 							 "Start Day Min (scaled): ", round(start_day_min, 2))) %>%
# 	addLegend("bottomright", 
# 						pal = palette, 
# 						values = ~start_day_min,
# 						title = "Startzeit (Minuten, skaliert)",
# 						opacity = 1)
# palette <- colorNumeric(palette = "viridis", 
# 												domain = sf_cluster_a_evening$end_day_min)
# 
# leaflet(data = sf_cluster_a_evening) %>%
# 	addTiles() %>%  # OSM-Basiskarte hinzufügen
# 	addCircleMarkers(~end_lng, ~end_lat, 
# 									 color = ~palette(end_day_min), 
# 									 fillOpacity = 0.7,
# 									 radius = 5,
# 									 popup = ~paste("Startzeit: ", end_datetime, "<br>",
# 									 							 "Start Longitude: ", start_lng, "<br>",
# 									 							 "Start Latitude: ", start_lat, "<br>",
# 									 							 "Start Day Min (scaled): ", round(end_day_min, 2))) %>%
# 	addLegend("bottomright", 
# 						pal = palette, 
# 						values = ~end_day_min,
# 						title = "Endzeit (Minuten, skaliert)",
# 						opacity = 1)

int_base_size <- 20


################################################################################
# Hour
################################################################################
all_minutes <- 0:59

trips_per_min_startmin_group <- sf_cluster %>%
	group_by(start_min, direction) %>%
	summarise(trip_count = n()) %>%
	ungroup() %>%
	complete(start_min = all_minutes, direction, fill = list(trip_count = 0))
trips_per_min_startmin_group$direction  <- as.factor(trips_per_min_startmin_group$direction)
trips_per_min_startmin_group$direction <- factor(trips_per_min_startmin_group$direction, 
																							 levels = rev(levels(trips_per_min_startmin_group$direction)))

ggplot(trips_per_min_startmin_group, aes(x = start_min %% 60, y = trip_count, color = direction)) +
	geom_point(size = 4) +
	geom_line(aes(group = direction)) +
	geom_text(aes(label = trip_count), hjust = 0.5, vjust = -1, size = 3) +
	scale_x_continuous(breaks = seq(0, 59, by = 10), limits = c(0, 59)) +
	labs(x = "Start Minute", y = "Trip Count", color = "Direction") +
	coord_polar(start = 0) +
	theme_minimal()


trips_per_min_startmin <- sf_cluster %>%
	group_by(start_min) %>%
	summarise(trip_count = n()) %>%
	ungroup() %>%
	complete(start_min = all_minutes, fill = list(trip_count = 0))



trips_per_min_endmin_group <- sf_cluster %>%
	group_by(end_min, direction) %>%
	summarise(trip_count = n()) %>%
	ungroup() %>%
	complete(end_min = all_minutes, direction, fill = list(trip_count = 0))

trips_per_min_endmin_group$direction  <- as.factor(trips_per_min_endmin_group$direction)
trips_per_min_endmin_group$direction <- factor(trips_per_min_endmin_group$direction, 
																	 levels = rev(levels(trips_per_min_endmin_group$direction)))

ggplot(trips_per_min_endmin_group, aes(x = end_min %% 60, y = trip_count, color = direction)) +
	geom_point(size = 4) +
	geom_line(aes(group = direction)) +
	geom_text(aes(label = trip_count), hjust = 0.5, vjust = -1, size = 3) +
	scale_x_continuous(breaks = seq(0, 59, by = 10), limits = c(0, 59)) +
	labs(x = "end Minute", y = "Trip Count", color = "Direction") +
	coord_polar(start = 0) +
	theme_minimal()


trips_per_min_endmin <- sf_cluster %>%
	group_by(end_min) %>%
	summarise(trip_count = n()) %>%
	ungroup() %>%
	complete(end_min = all_minutes, fill = list(trip_count = 0))



ggplot(trips_per_min_startmin, aes(x = start_min %% 60, y = trip_count)) +
	geom_point(size = 4) +
	geom_line(aes(group = 1)) +
	geom_text(aes(label = trip_count), hjust = 0.5, vjust = -1, size = 3) +
	scale_x_continuous(breaks = seq(0, 59, by = 10), limits = c(0, 59)) +
	labs(x = "start Minute", y = "Trip Count", color = "Direction") +
	coord_polar(start = 0) +
	theme_minimal()

ggplot(trips_per_min_endmin, aes(x = end_min %% 60, y = trip_count)) +
	geom_point(size = 4) +
	geom_line(aes(group = 1)) +
	geom_text(aes(label = trip_count), hjust = 0.5, vjust = -1, size = 3) +
	scale_x_continuous(breaks = seq(0, 59, by = 10), limits = c(0, 59)) +
	labs(x = "end Minute", y = "Trip Count", color = "Direction") +
	coord_polar(start = 0) +
	theme_minimal()

################################################################################
# Hour
################################################################################
all_hours <- 0:23

trips_per_hour <- sf_cluster %>%
	group_by(start_hour, direction) %>%
	summarise(trip_count = n()) %>%
	ungroup() %>%
	complete(start_hour = all_hours, direction, fill = list(trip_count = 0))
trips_per_hour$direction  <- as.factor(trips_per_hour$direction)
trips_per_hour$direction <- factor(trips_per_hour$direction, 
																	 levels = rev(levels(trips_per_hour$direction)))

ggplot(trips_per_hour, aes(x = factor(start_hour), 
													 y = trip_count, 
													 fill = direction)) +
	geom_bar(stat = "identity", position = "dodge") +
	labs(x = "Start Hour", 
			 y = "Number of Trips", 
			 title = "Number of Trips by Hour and Direction") + 
	theme_minimal(base_size = int_base_size)

################################################################################
# Weekday
################################################################################
all_weekdays <- 1:7

trips_per_weekday <- sf_cluster %>%
	group_by(weekday, direction) %>%
	summarise(trip_count = n()) %>%
	ungroup() %>%
	complete(weekday = all_weekdays, direction, fill = list(trip_count = 0))



ggplot(trips_per_weekday, aes(x = factor(weekday), 
													 y = trip_count, 
													 fill = direction)) +
	geom_bar(stat = "identity", position = "dodge") +
	labs(x = "Weekday", 
			 y = "Number of Trips", 
			 title = "Number of Trips by Weekday and Direction") + 
	theme_minimal(base_size = int_base_size)

################################################################################
# Date
################################################################################
trips_per_day <- sf_cluster %>%
	group_by(date) %>%
	summarise(trip_count = n()) %>%
	ungroup()

sf_anal <- sf_cluster_nd_pred %>%
	as.data.frame() %>%
	select(date)

trips_per_day_total <- sf_anal %>%
	group_by(date) %>%
	summarise(trip_count = n()) %>%
	ungroup()

# if(char_prefix_data == "sr"){
# 	trips_per_day <- trips_per_day %>%
# 		filter(format(date, "%Y") == "2022")
# 	
# }


all_days <- seq.Date(min(trips_per_day$date), max(trips_per_day$date), by = "day")

trips_per_day <- trips_per_day %>%
	complete(date = all_days, fill = list(trip_count = 0))

trips_per_day_total <- trips_per_day_total %>%
	complete(date = all_days, fill = list(trip_count = 0))

# Create the time series plot
ggplot(trips_per_day, aes(x = date, y = trip_count)) +
	geom_bar(stat = "identity") +
	labs(x = "Date", 
			 y = "Number of Trips", 
			 title = "Number of Trips per Day") + 
	theme_minimal(base_size = int_base_size) +
	scale_x_date(date_breaks = "5 days", date_labels = "%d-%b") +  
	theme(axis.text.x = element_text(angle = 45, hjust = 1, vjust = 1))


ggplot(trips_per_day_total, aes(x = date, y = trip_count)) +
	geom_bar(stat = "identity") +
	labs(x = "Date", 
			 y = "Number of Trips", 
			 title = "Number of Trips per Day") + 
	theme_minimal(base_size = int_base_size) +
	scale_x_date(date_breaks = "5 days", date_labels = "%d-%b") +  
	theme(axis.text.x = element_text(angle = 45, hjust = 1, vjust = 1))


