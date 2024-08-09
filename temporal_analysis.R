Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

char_city <- "dd"
char_prefix_data <- "sr"
char_data <- paste0(char_prefix_data, "_", char_city)
sf_cluster_nd_pred <- st_read(con, char_data)

sf_cluster_nd_pred$start_time <- as.POSIXct(
	paste(
		sf_cluster_nd_pred$start_date,
		sf_cluster_nd_pred$start_time),
	format =  "%d.%m.%Y %H:%M:%S", tz = "UTC")

sf_cluster_nd_pred$start_hour <- lubridate::hour(sf_cluster_nd_pred$start_time)
sf_cluster_nd_pred$weekday <- lubridate::wday(sf_cluster_nd_pred$start_time,
																							week_start = 1)
sf_cluster_nd_pred$date <- as.Date(sf_cluster_nd_pred$start_time)

cluster_a <- 144
cluster_b <- 199
sf_cluster <- sf_cluster_nd_pred %>%
	filter(cluster_pred == cluster_a | cluster_pred == cluster_b) %>%
	mutate(direction = case_when(
		cluster_pred == cluster_a ~ "direction_a",
		cluster_pred == cluster_b ~ "direction_b",
		TRUE ~ NA_character_ 
	))

int_base_size <- 20
################################################################################
# Hour
################################################################################
all_hours <- 0:23

trips_per_hour <- sf_cluster %>%
	group_by(start_hour, direction) %>%
	summarise(trip_count = n()) %>%
	ungroup() %>%
	complete(start_hour = all_hours, direction, fill = list(trip_count = 0))


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
	labs(x = "Weekday Hour", 
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

trips_per_day <- trips_per_day %>%
	filter(format(date, "%Y") == "2022")


all_days <- seq.Date(min(trips_per_day$date), max(trips_per_day$date), by = "day")

trips_per_day <- trips_per_day %>%
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
