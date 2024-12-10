Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")


################################################################################
# 1. Input data
################################################################################
available_data <- psql1_get_schemas(con)
available_data
char_schema <- available_data[2, "schema_name"]
dt_time_series <- dbGetQuery(
	con, 
	paste0("SELECT * FROM ", char_schema, ".time_series")
) %>% as.data.table()


dt_time_series[, minute := as.numeric(format(timestamp, "%M"))]
dt_time_series[, hour := as.numeric(format(timestamp, "%H"))]
dt_time_series[, day := as.numeric(format(timestamp, "%d"))]
dt_time_series[, weekday := weekdays(timestamp)]
dt_time_series$weekday <- factor(dt_time_series$weekday, levels = c("Montag", 
																										"Dienstag", 
																										"Mittwoch", 
																										"Donnerstag", 
																										"Freitag", 
																										"Samstag", 
																										"Sonntag"))

dt_time_series[, week := as.numeric(format(timestamp, "%V"))]
dt_time_series[, time_of_day := format(timestamp, "%H:%M")]
dt_time_series[, rel_count := count / max(count), by = cluster]


c_cl <- c(111:118) # 4-15, 64-65, 95-96, 84-108

dt_sub <- dt_time_series %>%
	filter(cluster %in% c_cl)
dt_sub <- dt_sub %>%
	filter(!weekday %in% c("Samstag", "Sonntag") & week < 13)


#oszilieren kommt von week 13 die wahrscheinlich nur ganz knapp angeshcnitten wird 
#und dann macht die durchschnitt auslastung für dei hour-min keinen sinn mehr

dt_sub <- dt_sub[order(cluster, time_of_day)]
dt_sub

dt_sub <- dt_sub[, .(
	average_count = mean(count),
	average_rel_count = mean(rel_count)
), by = .(cluster, time_of_day, week)]

dt_sub[, time_of_day := as.factor(time_of_day)]


ggplot(dt_sub, aes(x = time_of_day, 
									 y = average_rel_count, 
									 color = factor(cluster), 
									 group = cluster)) +
	geom_line(linewidth = 0.7) +
	geom_point() +
	labs(
		title = "Verfügbarkeit über den gesamten Zeitraum für ausgewählte Cluster",
		x = "Zeitstempel",
		y = "Anzahl der Verfügbarkeiten",
		color = "Cluster"
	) +
	scale_x_discrete(
		breaks = levels(dt_sub$time_of_day)[seq(1, length(levels(dt_sub$time_of_day)), by = 20)]
	) +
	facet_wrap(~ week, ncol = 2) +  # Facetten für jede Woche
	theme_minimal() +
	theme(
		text = element_text(size = 12),
		axis.text.x = element_text(angle = 45, hjust = 1)
	)

