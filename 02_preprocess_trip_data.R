Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")





################################################################################
# Scooter
################################################################################
path_scooter_data <- "./data/trips/raw/"
list.files(path_scooter_data)
scooter_file <- read_rds(paste0(path_scooter_data, 
																"dt_voi_munich_06_05", 
																".rds")) %>% 
	as.data.table
head(scooter_file)

# scooter_file <- scooter_file %>%
# 	mutate(source = "voi",
# 				 trip_id = as.character(NA),
# 				 device_id = as.character(NA),
# 				 gender = as.character(NA),
# 				 age = as.numeric(NA),
# 				 duration_s = as.numeric(NA),
# 				 distance_m = as.numeric(NA)) %>%
# 	select(-ride) %>%
# 	rename("start_datetime" = "start_time",
# 				 "end_datetime" = "dest_time",
# 				 "start_lat" = "start_loc_lat",
# 				 "start_lng" = "start_loc_lon",
# 				 "end_lat" = "dest_loc_lat",
# 				 "end_lng" = "dest_loc_lon",
# 				 "bike_number" = "id")
scooter_file <- scooter_file %>%
	mutate(source = "voi",
				 trip_id = as.character(NA),
				 device_id = as.character(NA),
				 gender = as.character(NA),
				 age = as.numeric(NA)) %>%
	select(-ride) %>%
	rename("duration_s" = "duration",
				 "distance_m" = "distance",
				 "start_datetime" = "start_time",
				 "end_datetime" = "dest_time",
				 "start_lat" = "start_loc_lat",
				 "start_lng" = "start_loc_lon",
				 "end_lat" = "dest_loc_lat",
				 "end_lng" = "dest_loc_lon",
				 "bike_number" = "scooter_id")



scooter_file <- scooter_file %>%
	select(trip_id, device_id, bike_number, start_lat, start_lng, 
				 end_lat, end_lng, start_datetime, end_datetime, 
				 duration_s, distance_m, gender, age, source)

#write_csv(scooter_file, "./data/data_ber_pp.csv")
st_write(scooter_file, con, "voi_munich_06_05_complete")

################################################################################
# Nextbike & Stadtradeln
################################################################################
char_city <- "dd"
char_prefix_data <- "sr"
sr_trip_file <- read_delim(paste0("./data/", char_prefix_data, "_data_dd.csv")) %>% 
	as.data.frame


sr_trip_file <- sr_trip_file %>%
	mutate(source = "sr",
				 bike_number = as.numeric(NA),
				 start_datetime = as.POSIXct(
				 	paste(
				 		sr_trip_file$start_date,
				 		sr_trip_file$start_time),
				 	format =  "%d.%m.%Y %H:%M:%S", tz = "UTC"),
				 end_datetime = as.POSIXct(
				 	paste(
				 		sr_trip_file$end_date,
				 		sr_trip_file$end_time),
				 	format =  "%d.%m.%Y %H:%M:%S", tz = "UTC")) %>%
	select(-ars, -measurement, -points, -type, -unknown, 
				 -start_date, -start_time, -end_date, -end_time) %>%
	rename("duration_s" = "duration [s]",
				 "distance_m" = "distance [m]")
sr_trip_file <- sr_trip_file[-which(is.na(sr_trip_file$end_lng)),]

sr_trip_file <- sr_trip_file %>%
	select(trip_id, device_id, bike_number, start_lat, start_lng, 
				 end_lat, end_lng, start_datetime, end_datetime, 
				 duration_s, distance_m, gender, age, source)

# write_csv(sr_trip_file, "./data/sr_data_dd_pp.csv")
# st_write(sr_trip_file, con, "sr_dd_complete")
char_prefix_data <- "nb"
nb_trip_file <- read_delim(paste0("./data/", char_prefix_data, "_data_dd.csv")) %>% 
	as.data.frame %>%
	select(-lat, -lng, -timestamp) %>%
	mutate(source = "nb",
				 trip_id = as.character(NA),
				 device_id = as.character(NA),
				 duration_s = as.numeric(difftime(end_time, start_time, units = "secs")),
				 distance_m = as.numeric(NA),
				 gender = as.character(NA),
				 age = as.numeric(NA)) %>%
	rename("start_datetime" = "start_time",
				 "end_datetime" = "end_time")

str(nb_trip_file)

# write_csv(nb_trip_file, "./data/nb_data_dd_pp.csv")
# st_write(nb_trip_file, con, "nb_dd_complete")
# Reorder nb_trip_file columns to match sr_trip_file
nb_trip_file <- nb_trip_file %>%
	select(trip_id, device_id, bike_number, start_lat, start_lng, 
				 end_lat, end_lng, start_datetime, end_datetime, 
				 duration_s, distance_m, gender, age, source)

# combined_trip_file <- rbind(sr_trip_file, nb_trip_file)
# nrow(combined_trip_file)
# combined_trip_file <- combined_trip_file[-which(is.na(combined_trip_file$end_lng)),]
# nrow(combined_trip_file)
# summary(combined_trip_file)
# table(year(combined_trip_file$start_datetime))
# 
# combined_trip_file <- combined_trip_file %>%
# 	filter(year(combined_trip_file$start_datetime) >= 2022)
# write_csv(combined_trip_file, "./data/comb_data_dd.csv")
# st_write(combined_trip_file, con, "sr_nb_dd_complete")


str(scooter_file)
str(nb_trip_file)
str(sr_trip_file)