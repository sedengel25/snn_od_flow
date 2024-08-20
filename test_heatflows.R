Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")

char_city <- "dd"
char_prefix_data <- "sr"
char_data <- paste0(char_prefix_data, "_", char_city)

sf_cluster_nd_pred <- st_read(con, paste0(char_data, "_cluster"))

sf_filter <- sf_cluster_nd_pred %>%
	filter(cluster_pred == 3)

test <- st_line_sample(sf_filter["line_geom"], n = 500)
test_points <- st_cast(test, "POINT") 
point_data <- st_coordinates(test_points) %>%
	as_tibble() %>%
	rename(X = X, Y = Y)

ggplot(point_data, aes(x = X, y = Y)) +
	geom_point(alpha = 0.1, color = "gray") +  # Zeige Punkte zur Orientierung
	stat_density_2d(aes(fill = ..level..),
									geom = "polygon",
									color = "white",
									alpha = 0.5) +  # Dichtekonturen als gefüllte Polygone
	scale_fill_viridis_c(option = "C") +  # Verwenden einer Viridis-Farbskala
	coord_fixed() +  # Fixiertes Koordinatenverhältnis für korrekte Proportionen
	theme_minimal() +
	labs(title = "Density Map of Cluster Movements")



density_plot <- ggplot(point_data, aes(x = X, y = Y)) +
	stat_density_2d(aes(fill = ..level..), geom = "polygon", contour = TRUE, n = 100, h = c(50, 50))

# Anwenden von ggplot_build auf das vollständige ggplot-Objekt
density_data <- ggplot_build(density_plot)

# Wähle die Top-N-Dichtepunkte aus (zum Beispiel die Top 10)
top_n_density_points <- density_data$data[[1]] %>%
	arrange(desc(level)) %>%
	slice(1:1000)  # Auswahl der Top 10 Dichtepunkte

# Konvertiere die Top-Density-Punkte in ein sf-Objekt
top_density_sf <- st_as_sf(top_n_density_points, coords = c("x", "y"), crs = st_crs(point_data))

# Berechne die konvexe Hülle um die Top-Density-Punkte
convex_hull <- st_convex_hull(st_union(top_density_sf))

# Zeichnen des Plots mit Originaldaten und konvexer Hülle
ggplot() +
	geom_point(data = point_data, aes(x = X, y = Y), color = "black", alpha = 0.01) +  # Originaldaten in Schwarz
	geom_sf(data = convex_hull, fill = "blue", alpha = 0.2, color = "blue", size = 1) +  # Konvexe Hülle in Blau
	geom_point(data = top_n_density_points, aes(x = x, y = y), color = "red", size = 2) +  # Zeigt die Punkte der höchsten Dichte
	theme_minimal() +
	labs(title = "Convex Hull around Top-N Density Points with Original Data")
