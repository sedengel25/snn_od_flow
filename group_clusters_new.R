library(sf)
library(dplyr)
library(units)
source("./src/config.R")

calculate_overlap <- function(poly1, poly2) {
	intersection <- st_intersection(poly1, poly2)
	if (length(intersection) == 0 || is.null(st_area(intersection))) {
		return(list(overlap = 0, intersection = NULL))
	} else {
		# Berechnen der Schnittfläche
		intersection_area <- st_area(intersection)
		# Berechnen der Überlappung für beide Polygone
		overlap1 <- as.numeric(intersection_area / st_area(poly1))
		overlap2 <- as.numeric(intersection_area / st_area(poly2))
		# Mitteln der beiden Überlappungen
		mean_overlap <- (overlap1 + overlap2) / 2
		return(list(overlap = mean_overlap, intersection = intersection))
	}
}

# 1. Extrahieren des Start- und Endpunktes von Linestrings
extract_points <- function(line_geom) {
	coords <- st_coordinates(line_geom)
	origin_geom <- st_point(coords[1, ])
	dest_geom <- st_point(coords[nrow(coords), ])
	return(list(origin_geom = origin_geom, dest_geom = dest_geom))
}

# 2. Erstellen des zentralen Linestrings für jeden Cluster
create_central_linestring <- function(cluster_data) {
	origin_points <- st_sfc()
	dest_points <- st_sfc()
	
	for (i in 1:nrow(cluster_data)) {
		points <- extract_points(cluster_data$line_geom[i])
		origin_points <- c(origin_points, st_sfc(points$origin_geom))
		dest_points <- c(dest_points, st_sfc(points$dest_geom))
	}
	
	if (length(origin_points) > 0 & length(dest_points) > 0) {
		origin_centroid <- st_centroid(st_union(origin_points))
		dest_centroid <- st_centroid(st_union(dest_points))
		central_linestring <- st_sfc(st_linestring(rbind(st_coordinates(origin_centroid), st_coordinates(dest_centroid))))
	} else {
		central_linestring <- NULL
	}
	return(central_linestring)
}

# 3. Berechnung des Winkels zwischen zwei Linestrings
calculate_angle <- function(line1, line2) {
	if (is.null(line1) || is.null(line2)) {
		return(0)
	}
	
	coords1 <- st_coordinates(line1)
	coords2 <- st_coordinates(line2)
	
	vec1 <- coords1[2, ] - coords1[1, ]
	vec2 <- coords2[2, ] - coords2[1, ]
	
	dot_product <- sum(vec1 * vec2)
	magnitude1 <- sqrt(sum(vec1^2))
	magnitude2 <- sqrt(sum(vec2^2))
	
	cos_theta <- dot_product / (magnitude1 * magnitude2)
	angle <- acos(cos_theta) * 180 / pi  # Winkel in Grad
	
	# Sicherstellen, dass der Winkel zwischen 0 und 90 Grad liegt
	angle <- min(angle, 180 - angle)
	
	# Normierung des Winkels zwischen 0 und 1 (0 Grad = 1, 90 Grad = 0)
	norm_angle <- (90 - abs(angle)) / 90
	return(norm_angle)
}

char_city <- "dd"
char_prefix_data <- "sr"
char_data <- paste0(char_prefix_data, "_", char_city)
buffer <- 500

sf_cluster_nd_pred <- st_read(con, paste0(char_data, "_cluster"))
sf_cluster_nd_pred <- sf_cluster_nd_pred %>%
	filter(cluster_pred != 0)

line_counts <- sf_cluster_nd_pred %>%
	group_by(cluster_pred) %>%
	summarize(count = n())

cluster_ids <- 1:max(sf_cluster_nd_pred$cluster_pred)

# Leere Liste für Polygone und zentrale Linestrings
polygon_list <- list()
central_linestrings <- list()

# Loop durch die Cluster und berechne die konvexen Hüllen sowie zentrale Linestrings
for (cluster_id in cluster_ids) {
	print(cluster_id)
	
	cluster_data <- sf_cluster_nd_pred %>% filter(cluster_pred == cluster_id)
	polygon <- cluster_data %>%
		summarize(geometry = st_union(line_geom) %>% st_convex_hull())
	polygon_list[[as.character(cluster_id)]] <- polygon
	
	central_linestring <- create_central_linestring(cluster_data)
	central_linestrings[[as.character(cluster_id)]] <- central_linestring
}

polygons <- do.call(rbind, polygon_list)
polygons$cluster_id <- 1:max(sf_cluster_nd_pred$cluster_pred)

# Berechnen der Centroiden
polygons <- polygons %>% mutate(centroid = st_centroid(geometry))

# Leere Liste für Ergebnisse
results <- list()
intersections <- list()

# Loop durch jedes Polygon
for (i in 1:nrow(polygons)) {
	print(i)
	poly1 <- polygons[i,]
	centroid1 <- poly1$centroid
	line1 <- central_linestrings[[as.character(poly1$cluster_id)]]
	cluster_id_1 <- as.character(poly1$cluster_id)
	count1 <- line_counts$count[line_counts$cluster_pred == as.numeric(cluster_id_1)]  
	
	# Finden der Polygone innerhalb eines Radius von 1000 Metern
	distances <- st_distance(polygons$centroid, centroid1)
	neighbors <- polygons %>%
		filter(distances < units::set_units(buffer, "m") & row_number() != i)
	
	if (nrow(neighbors) == 0) {
		results[[i]] <- data.frame(
			polygon_id = i,
			best_match_id = NA,
			overlap_metric = NA,
			angle_metric = NA,
			combined_metric = NA,
			top_1_id = NA,
			top_2_id = NA,
			top_3_id = NA,
			count1 = count1,  # Anzahl der Linienstrings in polygon_id
			count2 = NA  # Anzahl der Linienstrings in best_match_id
		)
		next
	}
	
	metrics <- data.frame(
		polygon_id = integer(),
		cluster_id = integer(),
		overlap_metric = numeric(),
		angle_metric = numeric(),
		combined_metric = numeric(),
		count1 = integer(),
		count2 = integer(),
		best_intersection = st_sfc()
	)
	
	# Berechnen der Metriken mit jedem Nachbarpolygon
	for (j in 1:nrow(neighbors)) {
		poly2 <- neighbors[j,]
		cluster_id_2 <- as.character(poly2$cluster_id)
		count2 <- line_counts$count[line_counts$cluster_pred == as.numeric(cluster_id_2)]  
		if (!is.null(cluster_id_2) && cluster_id_2 %in% names(central_linestrings)) {
			line2 <- central_linestrings[[cluster_id_2]]
			
			result <- calculate_overlap(poly1$geometry, poly2$geometry)
			overlap_metric <- result$overlap
			angle_metric <- calculate_angle(line1, line2)
			
			combined_metric <- overlap_metric + angle_metric
			
			metrics <- rbind(metrics, data.frame(
				polygon_id = i,
				cluster_id = as.integer(cluster_id_2),
				overlap_metric = overlap_metric,
				angle_metric = angle_metric,
				combined_metric = combined_metric,
				count1 = count1,
				count2 = count2,
				best_intersection = result$intersection
			))
		}
	}
	
	if (nrow(metrics) == 0) {
		results[[i]] <- data.frame(
			polygon_id = i,
			best_match_id = NA,
			overlap_metric = NA,
			angle_metric = NA,
			combined_metric = NA,
			top_1_id = NA,
			top_2_id = NA,
			top_3_id = NA,
			count1 = count1,  
			count2 = NA  
		)
	} else {
		metrics <- metrics %>% arrange(desc(combined_metric))
		top_3 <- head(metrics, 3)
		
		best_intersection <- top_3$best_intersection[1]
		
		results[[i]] <- data.frame(
			polygon_id = i,
			best_match_id = top_3$cluster_id[1],
			overlap_metric = top_3$overlap_metric[1],
			angle_metric = top_3$angle_metric[1],
			combined_metric = top_3$combined_metric[1],
			top_1_id = top_3$cluster_id[1],
			top_2_id = ifelse(nrow(top_3) > 1, top_3$cluster_id[2], NA),
			top_3_id = ifelse(nrow(top_3) > 2, top_3$cluster_id[3], NA),
			count1 = count1,  
			count2 = ifelse(nrow(top_3) > 0, top_3$count2[1], NA)
		)
	}
	
	if (!is.null(best_intersection) && length(best_intersection) > 0) {
		intersections[[i]] <- st_sf(
			polygon_id_1 = i,
			polygon_id_2 = ifelse(is.null(top_3$cluster_id[1]), NA, top_3$cluster_id[1]),
			geometry = best_intersection
		)
	}
}

# Ergebnisse in einen DataFrame konvertieren
results_df <- do.call(rbind, results)

results_df <- results_df %>%
	arrange(desc(combined_metric)) %>%
	select(-overlap_metric, -angle_metric)


sf_rep <- st_read(con, paste0(char_data, "_rep_ls"))



results_df <- results_df %>%
	left_join(sf_rep %>% select(cluster_pred, geometry), 
						by = c("polygon_id" = "cluster_pred")) %>%
	rename(rep_geom = geometry)




results_sf <- st_as_sf(results_df, sf_column_name = "rep_geom", crs = 32632)

results_sf <- results_sf %>%
	mutate(row_type = ifelse(row_number() %% 2 == 1, "odd", "even"),
				 color = ifelse(row_type == "odd", "blue", "red"))

sf_head <- head(results_sf, 20)
sum(sf_head$count1)
sf_head <- st_transform(sf_head, crs = 4326)
leaflet(data = sf_head) %>% 
	addTiles() %>% 
	addPolylines(color = ~color, weight = 2) %>% 
	addLegend("bottomright", colors = c("blue", "red"), 
						labels = c("Odd", "Even"), title = "Row Type")
