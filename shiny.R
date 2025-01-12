Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")
library(coop)
#################################################################################
# Functions
################################################################################

get_length_var_cos_sim <- function(sf_trips, group_by_column = "cluster_ent") {
	if (!group_by_column %in% names(sf_trips)) {
		stop("The specified group_by_column does not exist in the dataset.")
	}
	
	# Sicherstellen, dass die Geometrie nur Punkte enthält
	if (!all(st_geometry_type(sf_trips$o_closest_point) == "POINT")) {
		stop("o_closest_point must contain only POINT geometries.")
	}
	
	if (!all(st_geometry_type(sf_trips$d_closest_point) == "POINT")) {
		stop("d_closest_point must contain only POINT geometries.")
	}
	
	sf_trips %>%
		group_by(across(all_of(group_by_column))) %>%
		summarize(
			mean_trip_distance = mean(trip_distance, na.rm = TRUE),
			sd_trip_distance = sd(trip_distance, na.rm = TRUE),  # Standardabweichung
			cosine_similarity_flows = {
				# Extrahiere Start- und Ziel-Koordinaten der aktuellen Gruppe
				origin_matrix <- st_coordinates(st_geometry(o_closest_point[cur_group_rows()]))
				dest_matrix <- st_coordinates(st_geometry(d_closest_point[cur_group_rows()]))
				
				# Kombiniere die Matrizen, um eine einzige Matrix zu erstellen
				combined_matrix <- rbind(origin_matrix, dest_matrix)
				print("Combined matrix:")
				print(head(combined_matrix))
				# Cosine Similarity für die kombinierte Matrix berechnen
				cosine_matrix <- coop::cosine(t(combined_matrix))  # Transponieren für Spaltenvektoren
				print("Cosine matrix:")
				print(cosine_matrix[1:5, 1:5])
				# Extrahiere relevante Werte (oberer Dreiecksteil ohne Diagonale)
				cosine_values <- cosine_matrix[lower.tri(cosine_matrix)]
				
				# Mittelwert der Cosine Similarities
				mean(cosine_values, na.rm = TRUE)
			},
			.groups = "drop"
		)
}



cosine_similarity <- function(v1, v2) {
	sum(v1 * v2) / (sqrt(sum(v1^2)) * sqrt(sum(v2^2)))
}




reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")
hdbscan <- reticulate::import("hdbscan")
#################################################################################
# 1. Network data
################################################################################
available_networks <- psql1_get_available_networks(con)
print(available_networks)
char_network <- available_networks[3, "table_name"]
sf_network <- st_read(con, char_network) 
#################################################################################
# 2. Get original OD flow data based on subsets
################################################################################
# subset <- c(15001, 20001)
# char_schema <- paste0("data_", paste0(subset, collapse = "_"))
available_schemas <- psql1_get_schemas(con)
print(available_schemas)
char_schema <- available_schemas[6, "schema_name"]
sf_trips_sub <- st_read(
	con,
	query = paste0("SELECT * FROM ", 
								 char_schema, 
								 ".data")) %>%
	rename(flow_id_temp = flow_id)
# available_mapped_trip_data <- psql1_get_mapped_trip_data(con)
# 
# char_data <- available_mapped_trip_data[1, "table_name"]
# 
# sf_trips <- st_read(con, char_data) %>%
# 	rename("origin_id" = "id_edge_origin",
# 				 "dest_id" = "id_edge_dest") 
# 
# sf_trips <- sf_trips %>%
# 	arrange(start_datetime) %>%
# 	filter(trip_distance >= 100)
# 
# sf_trips_sub <- sf_trips %>%
# 	mutate(flow_id = 1:nrow(sf_trips)) %>%
# 	filter(flow_id >= min(subset) & flow_id <= max(subset))
# sf_trips_sub$flow_id_temp <- 1:nrow(sf_trips_sub)




int_min_cl_size <- 10
char_schema <- "nb_dd_mickten_min100m_kw7_8_9_10_11_12_wdays1_2_3_4_5m_buffer50000m"
matrix_pacmap_euclid <- np$load(here::here(path_python, 
																		char_schema,
																		"flow_manhattan_pts_euclid",
																		"embedding_3d_2.npy")) 
df_pacmap_euclid <-  py_hdbscan(np, hdbscan, matrix_pacmap_euclid, int_min_cl_size)

matrix_pacmap_network <- np$load(here::here(path_python, 
																		char_schema,
																		"flow_manhattan_pts_network",
																		"embedding_3d_2.npy")) 
	
df_pacmap_network <-  py_hdbscan(np, hdbscan, matrix_pacmap_network, int_min_cl_size)


# sf_trips_sub$cluster_net <- df_pacmap_network$cluster
# sf_trips_sub$cluster_euclid <- df_pacmap_euclid$cluster
# sf_trips_sub <- sf_trips_sub %>%
# 	filter(st_geometry_type(o_closest_point) == "POINT", 
# 				 st_geometry_type(d_closest_point) == "POINT")
# cluster_stats_net <- get_length_var_cos_sim(sf_trips_sub, "cluster_net")
# mean(cluster_stats_net %>% filter(cluster_net != -1) %>% pull(sd_trip_distance))
# cluster_stats_euclid <- get_length_var_cos_sim(sf_trips_sub, "cluster_euclid")
# mean(cluster_stats_euclid %>% filter(cluster_euclid != -1) %>% pull(sd_trip_distance))



library(shiny)
library(plotly)
library(leaflet)
library(dplyr)
library(sf)

ui <- fluidPage(
	titlePanel("Interactive Flow Selection"),
	fluidRow(
		column(6, 
					 selectizeInput(
					 	"euclid_cluster", 
					 	"Select Clusters (Euclidean):", 
					 	choices = NULL, 
					 	selected = NULL, 
					 	multiple = TRUE
					 ),
					 checkboxInput("show_noise_euclid", "Show Noise (Cluster -1):", value = TRUE),
					 plotlyOutput("scatter_plot_euclid", height = "600px")
		),
		column(6, 
					 selectizeInput(
					 	"network_cluster", 
					 	"Select Clusters (Network):", 
					 	choices = NULL, 
					 	selected = NULL, 
					 	multiple = TRUE
					 ),
					 checkboxInput("show_noise_network", "Show Noise (Cluster -1):", value = TRUE),
					 plotlyOutput("scatter_plot_network", height = "600px")
		)
	),
	fluidRow(
		column(12, leafletOutput("flow_map", height = "600px"))
	),
	fluidRow(
		column(6, 
					 sliderInput("min_cluster_size", "Min Cluster Size:", min = 5, max = 100, value = 30)
		),
		column(6, 
					 sliderInput("knn_slider", "Number of Nearest Neighbors (k):", min = 5, max = 500, value = 50)
		),
		column(6, 
					 radioButtons("show_clusters", "Show Clusters:", choices = c("Yes", "No"), selected = "No", inline = TRUE)
		)
	),
	fluidRow(
		column(6, textOutput("euclid_cluster_info")),
		column(6, textOutput("network_cluster_info"))
	)
)

server <- function(input, output, session) {
	# Transformiere die Geometrien ins WGS 84 (EPSG:4326)
	sf_network_transformed <- st_transform(sf_network, crs = 4326)
	sf_trips_sub_transformed <- sf_trips_sub %>% 
		mutate(line_geom = st_transform(line_geom, crs = 4326))
	
	# Reactive Values for Clustering
	reactive_clusters <- reactiveValues(
		df_pacmap_euclid = NULL,
		df_pacmap_network = NULL
	)
	
	# Reactive Clustering
	observe({
		reactive_clusters$df_pacmap_euclid <- py_hdbscan(np, hdbscan, matrix_pacmap_euclid, input$min_cluster_size)
		updateSelectizeInput(session, "euclid_cluster", choices = unique(reactive_clusters$df_pacmap_euclid$cluster), server = TRUE)
		
		reactive_clusters$df_pacmap_network <- py_hdbscan(np, hdbscan, matrix_pacmap_network, input$min_cluster_size)
		updateSelectizeInput(session, "network_cluster", choices = unique(reactive_clusters$df_pacmap_network$cluster), server = TRUE)
	})
	
	# Zufällige Farben für die Cluster
	random_colors <- function(n) {
		rgb(runif(n), runif(n), runif(n))
	}
	
	cluster_colors_euclid <- reactive({
		setNames(random_colors(length(unique(reactive_clusters$df_pacmap_euclid$cluster))), unique(reactive_clusters$df_pacmap_euclid$cluster))
	})
	cluster_colors_network <- reactive({
		setNames(random_colors(length(unique(reactive_clusters$df_pacmap_network$cluster))), unique(reactive_clusters$df_pacmap_network$cluster))
	})
	
	# Reactive: Ausgewählte Punkte für beide Scatterplots
	selected_points <- reactiveVal(NULL)
	
	# KNN-Auswahl
	get_nearest_neighbors <- function(data, selected_id, k) {
		selected_point <- data %>% filter(flow_id == selected_id) %>% select(x, y, z)
		all_points <- data %>% select(x, y, z)
		nn_indices <- FNN::get.knnx(data = all_points, query = selected_point, k = k)$nn.index
		data[nn_indices, ]$flow_id
	}
	
	# Beobachtung für Euclidean Cluster-Auswahl
	observeEvent(input$euclid_cluster, {
		selected_clusters <- as.numeric(input$euclid_cluster)
		if (is.null(selected_clusters) || length(selected_clusters) == 0) {
			selected_points(NULL)
		} else {
			points <- reactive_clusters$df_pacmap_euclid %>%
				filter(cluster %in% selected_clusters) %>%
				pull(flow_id)
			selected_points(points)
		}
	})
	
	# Beobachtung für Network Cluster-Auswahl
	observeEvent(input$network_cluster, {
		selected_clusters <- as.numeric(input$network_cluster)
		if (is.null(selected_clusters) || length(selected_clusters) == 0) {
			selected_points(NULL)
		} else {
			points <- reactive_clusters$df_pacmap_network %>%
				filter(cluster %in% selected_clusters) %>%
				pull(flow_id)
			selected_points(points)
		}
	})
	
	# Beobachtung für KNN-Auswahl
	observeEvent(event_data("plotly_click", source = "scatter_euclid"), {
		click_data <- event_data("plotly_click", source = "scatter_euclid")
		if (!is.null(click_data)) {
			clicked_id <- click_data$customdata
			points <- get_nearest_neighbors(reactive_clusters$df_pacmap_euclid, clicked_id, input$knn_slider)
			selected_points(points)
		}
	})
	
	observeEvent(event_data("plotly_click", source = "scatter_network"), {
		click_data <- event_data("plotly_click", source = "scatter_network")
		if (!is.null(click_data)) {
			clicked_id <- click_data$customdata
			points <- get_nearest_neighbors(reactive_clusters$df_pacmap_network, clicked_id, input$knn_slider)
			selected_points(points)
		}
	})
	
	# Scatter Plot für Euclidean Clustering
	output$scatter_plot_euclid <- renderPlotly({
		highlight_ids <- selected_points()
		show_clusters <- input$show_clusters == "Yes"
		show_noise <- input$show_noise_euclid
		
		plot_data <- reactive_clusters$df_pacmap_euclid %>%
			filter(show_noise | cluster != -1) %>%
			mutate(
				color = if (show_clusters) as.character(cluster_colors_euclid()[as.character(cluster)]) else 
					ifelse(flow_id %in% highlight_ids, "blue", "gray")
			)
		
		plot_ly(
			data = plot_data,
			x = ~x,
			y = ~y,
			z = ~z,
			type = "scatter3d",
			mode = "markers",
			marker = list(size = 3),
			color = ~color,
			text = ~paste("Flow ID:", flow_id, "<br>Cluster ID:", cluster),
			customdata = ~flow_id,
			source = "scatter_euclid",
			showlegend = FALSE
		) %>%
			layout(title = "Euclidean Clustering (3D)")
	})
	
	# Scatter Plot für Network Clustering
	output$scatter_plot_network <- renderPlotly({
		highlight_ids <- selected_points()
		show_clusters <- input$show_clusters == "Yes"
		show_noise <- input$show_noise_network
		
		plot_data <- reactive_clusters$df_pacmap_network %>%
			filter(show_noise | cluster != -1) %>%
			mutate(
				color = if (show_clusters) as.character(cluster_colors_network()[as.character(cluster)]) else 
					ifelse(flow_id %in% highlight_ids, "blue", "gray")
			)
		
		plot_ly(
			data = plot_data,
			x = ~x,
			y = ~y,
			z = ~z,
			type = "scatter3d",
			mode = "markers",
			marker = list(size = 3),
			color = ~color,
			text = ~paste("Flow ID:", flow_id, "<br>Cluster ID:", cluster),
			customdata = ~flow_id,
			source = "scatter_network",
			showlegend = FALSE
		) %>%
			layout(title = "Network Clustering (3D)")
	})
	
	# Kennzahlen für Cluster-Informationen
	output$euclid_cluster_info <- renderText({
		req(reactive_clusters$df_pacmap_euclid)
		clusters <- reactive_clusters$df_pacmap_euclid$cluster
		paste(
			"Euclidean Clustering:",
			"Number of Clusters:", length(unique(clusters[clusters != -1])),
			"Noise Flows:", sum(clusters == -1)
		)
	})
	
	output$network_cluster_info <- renderText({
		req(reactive_clusters$df_pacmap_network)
		clusters <- reactive_clusters$df_pacmap_network$cluster
		paste(
			"Network Clustering:",
			"Number of Clusters:", length(unique(clusters[clusters != -1])),
			"Noise Flows:", sum(clusters == -1)
		)
	})
	
	# Flow Map (Leaflet)
	output$flow_map <- renderLeaflet({
		req(selected_points())
		selected_ids <- selected_points()
		req(selected_ids)
		
		# Setze die Geometriespalte explizit auf line_geom
		selected_flows <- sf_trips_sub_transformed %>%
			filter(flow_id_temp %in% selected_ids) %>%
			st_set_geometry("line_geom")  # Setzt line_geom als aktive Geometriespalte
		
		leaflet() %>%
			addProviderTiles(providers$CartoDB.Positron) %>%
			addPolylines(data = sf_network_transformed, color = "gray", weight = 1, opacity = 0.9) %>%
			addPolylines(data = selected_flows, color = "blue", weight = 2, opacity = 1)
	})
}

shinyApp(ui, server)

