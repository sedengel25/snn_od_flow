Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")
#library(coop)

#################################################################################
# Functions
################################################################################
reticulate::use_virtualenv("r-reticulate", required = TRUE)
np <- reticulate::import("numpy")
hdbscan <- reticulate::import("hdbscan")
#################################################################################
# 1. Network data
################################################################################
available_networks <- psql1_get_available_networks(con)
print(available_networks)
char_network <- available_networks[6, "table_name"]
sf_network <- st_read(con, char_network) 
#################################################################################
# 2. Get original OD flow data based on subsets
################################################################################
# subset <- c(15001, 20001)
# char_schema <- paste0("data_", paste0(subset, collapse = "_"))
available_schemas <- psql1_get_schemas(con)
print(available_schemas)
char_schema <- available_schemas[4, "schema_name"]
sf_trips_sub <- st_read(
	con,
	query = paste0("SELECT * FROM ",
								 char_schema,
								 ".data"))

sf_trips_sub$flow_id_temp <- 1:nrow(sf_trips_sub)
# available_mapped_trip_data <- psql1_get_mapped_trip_data(con)
# 
# char_data <- available_mapped_trip_data[2, "table_name"]
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




int_min_cl_size <- 20
snn_k <- 20
# char_schema <- "nb_dd_mickten_min100m_kw7_8_9_10_11_12_wdays1_2_3_4_5m_buffer50000m"
matrix_pacmap_euclid <- np$load(here::here(path_python, 
																		char_schema,
																		"flow_manhattan_pts_euclid",
																		"embedding_3d_2.npy")) 



df_hdbscan_euclid <-  py_hdbscan(np, hdbscan, matrix_pacmap_euclid, int_min_cl_size)

snn_dist_euclid <- proxy::dist(matrix_pacmap_euclid)
df_snn_euclid <- snn_custom(matrix_pacmap_euclid, snn_dist_euclid, snn_k)

matrix_pacmap_network <- np$load(here::here(path_python, 
																		char_schema,
																		"flow_manhattan_pts_network",
																		"embedding_3d_2.npy")) 

snn_dist_network <- proxy::dist(matrix_pacmap_network)
df_snn_network <- snn_custom(matrix_pacmap_network, snn_dist_network, snn_k)
df_hdbscan_network <-  py_hdbscan(np, hdbscan, matrix_pacmap_network, int_min_cl_size)


# sf_trips_sub$cluster_net <- df_hdbscan_network$cluster
# sf_trips_sub$cluster_euclid <- df_hdbscan_euclid$cluster
# sf_trips_sub <- sf_trips_sub %>%
# 	filter(st_geometry_type(o_closest_point) == "POINT", 
# 				 st_geometry_type(d_closest_point) == "POINT")
# cluster_stats_net <- get_length_var_cos_sim(sf_trips_sub, "cluster_net")
# mean(cluster_stats_net %>% filter(cluster_net != -1) %>% pull(sd_trip_distance))
# cluster_stats_euclid <- get_length_var_cos_sim(sf_trips_sub, "cluster_euclid")
# mean(cluster_stats_euclid %>% filter(cluster_euclid != -1) %>% pull(sd_trip_distance))




ui <- fluidPage(
	titlePanel("Interactive Flow Selection"),
	fluidRow(
		column(4, 
					 h3("Euclidean Clustering"),
					 fluidRow(
					 	column(8,
					 				 selectizeInput(
					 				 	"euclid_cluster", 
					 				 	"Select Clusters (Euclidean):", 
					 				 	choices = NULL, 
					 				 	selected = NULL, 
					 				 	multiple = TRUE
					 				 )
					 	),
					 	column(4,
					 				 actionButton("go_button_euclid", "Go (Euclidean)", style = "margin-top: 25px;")
					 	)
					 ),
					 textOutput("euclid_cluster_output"),
					 textInput("flow_id_input_euclid", "Select Flow ID for Euclidean KNN:", value = "", placeholder = "Enter Flow ID"),
					 plotlyOutput("scatter_plot_euclid", height = "600px")
		),
		column(4, 
					 h3("Common Settings"),
					 sliderInput("min_cluster_size", "Min Cluster Size:", min = 5, max = 100, value = 30),
					 textInput("knn_text", "Number of Nearest Neighbors (k):", value = "50", placeholder = "Enter k value"),
					 radioButtons("show_clusters", "Show Clusters:", choices = c("Yes", "No"), selected = "No", inline = TRUE),
					 checkboxInput("show_noise", "Show Noise (Cluster -1):", value = TRUE)
		),
		column(4, 
					 h3("Network Clustering"),
					 fluidRow(
					 	column(8,
					 				 selectizeInput(
					 				 	"network_cluster", 
					 				 	"Select Clusters (Network):", 
					 				 	choices = NULL, 
					 				 	selected = NULL, 
					 				 	multiple = TRUE
					 				 )
					 	),
					 	column(4,
					 				 actionButton("go_button_network", "Go (Network)", style = "margin-top: 25px;")
					 	)
					 ),
					 textOutput("network_cluster_output"),
					 textInput("flow_id_input_network", "Select Flow ID for Network KNN:", value = "", placeholder = "Enter Flow ID"),
					 plotlyOutput("scatter_plot_network", height = "600px"),
					 downloadButton("download", "Download Selected")
		)
	),
	fluidRow(
		column(12, leafletOutput("flow_map", height = "600px"))
	)
)


server <- function(input, output, session) {
	# Transformiere die Geometrien ins WGS 84 (EPSG:4326)
	sf_network_transformed <- st_transform(sf_network, crs = 4326)
	sf_trips_sub_transformed <- sf_trips_sub %>%
		mutate(line_geom = st_transform(line_geom, crs = 4326))
	
	# Reactive Values for Clustering
	reactive_clusters <- reactiveValues(
		df_hdbscan_euclid = NULL,
		df_hdbscan_network = NULL
	)
	
	# Temporäre Speicherung der Cluster-Auswahl
	temp_selected_clusters <- reactiveValues(
		euclid = NULL,
		network = NULL
	)
	
	# Reactive Clustering
	observe({
		reactive_clusters$df_hdbscan_euclid <- py_hdbscan(np, hdbscan, matrix_pacmap_euclid, input$min_cluster_size)
		updateSelectizeInput(session, "euclid_cluster", choices = unique(reactive_clusters$df_hdbscan_euclid$cluster), server = TRUE)
		
		reactive_clusters$df_hdbscan_network <- py_hdbscan(np, hdbscan, matrix_pacmap_network, input$min_cluster_size)
		updateSelectizeInput(session, "network_cluster", choices = unique(reactive_clusters$df_hdbscan_network$cluster), server = TRUE)
	})
	
	# Temporäre Speicherung der Cluster-Auswahl
	observeEvent(input$euclid_cluster, {
		temp_selected_clusters$euclid <- as.numeric(input$euclid_cluster)
	})
	
	observeEvent(input$network_cluster, {
		temp_selected_clusters$network <- as.numeric(input$network_cluster)
	})
	
	# Zufällige Farben für die Cluster
	random_colors <- function(n) {
		rgb(runif(n), runif(n), runif(n))
	}
	
	cluster_colors_euclid <- reactive({
		setNames(random_colors(length(unique(reactive_clusters$df_hdbscan_euclid$cluster))), unique(reactive_clusters$df_hdbscan_euclid$cluster))
	})
	cluster_colors_network <- reactive({
		setNames(random_colors(length(unique(reactive_clusters$df_hdbscan_network$cluster))), unique(reactive_clusters$df_hdbscan_network$cluster))
	})
	
	# Reactive: Ausgewählte Punkte für beide Scatterplots
	selected_points <- reactiveVal(NULL)
	
	# Reactive: Ausgewählte KNN-Flows für die Karte
	selected_knn_flows <- reactiveVal(NULL)
	
	# KNN-Auswahl
	get_nearest_neighbors <- function(data, query_point, k) {
		all_points <- data %>% select(x, y, z)
		nn_indices <- FNN::get.knnx(data = as.matrix(all_points), query = as.matrix(query_point), k = k)$nn.index
		data[nn_indices, ]$flow_id
	}
	
	update_selected_points <- function(sel_flow_id, k, data_source) {
		if (!is.null(sel_flow_id) && sel_flow_id != "") {
			query_point <- data_source %>% filter(flow_id == sel_flow_id) %>% select(x, y, z)
			if (nrow(query_point) > 0) {
				
				k <- suppressWarnings(as.numeric(k))
				if (is.na(k) || k <= 0) {
					cat("Invalid k value. Must be a positive number.\n")
					selected_points(NULL)
					selected_knn_flows(NULL)
					return()
				}
				
				points <- get_nearest_neighbors(data_source, query_point, k)
				selected_points(points)
				selected_knn_flows(points)
			} else {
				selected_points(NULL)
				selected_knn_flows(NULL)
			}
		}
	}
	

	
	observeEvent(input$go_button_euclid, {
		input_id <- input$flow_id_input_euclid
		selected_clusters <- temp_selected_clusters$euclid
		
		if (!is.null(input_id) && input_id != "") {
			cat("Flow ID (Euclidean) eingegeben: ", input_id, "\n")
			update_selected_points(input_id, input$knn_text, reactive_clusters$df_hdbscan_euclid)
		} else if (!is.null(selected_clusters) && length(selected_clusters) > 0) {
			points <- reactive_clusters$df_hdbscan_euclid %>%
				filter(cluster %in% selected_clusters) %>%
				pull(flow_id)
			selected_points(points)
			cat("Cluster-Auswahl (Euclidean) bestätigt: ", selected_clusters, "\n")
		} else {
			selected_points(NULL)
			cat("Keine gültige Auswahl für Euclidean bestätigt.\n")
		}
	})
	
	# Anzeige nach Bestätigung für Network
	observeEvent(input$go_button_network, {
		input_id <- input$flow_id_input_network
		selected_clusters <- temp_selected_clusters$network
		
		if (!is.null(input_id) && input_id != "") {
			cat("Flow ID (Network) eingegeben: ", input_id, "\n")
			update_selected_points(input_id, input$knn_text, reactive_clusters$df_hdbscan_network)
		} else if (!is.null(selected_clusters) && length(selected_clusters) > 0) {
			points <- reactive_clusters$df_hdbscan_network %>%
				filter(cluster %in% selected_clusters) %>%
				pull(flow_id)
			selected_points(points)
			cat("Cluster-Auswahl (Network) bestätigt: ", selected_clusters, "\n")
		} else {
			selected_points(NULL)
			cat("Keine gültige Auswahl für Network bestätigt.\n")
		}
	})
	
	# Beobachtung für KNN-Auswahl (Per Klick im Scatterplot)
	observeEvent(event_data("plotly_click", source = "scatter_euclid"), {
		click_data <- event_data("plotly_click", source = "scatter_euclid")
		if (!is.null(click_data)) {
			clicked_id <- click_data$customdata
			update_selected_points(clicked_id, input$knn_text, reactive_clusters$df_hdbscan_euclid)
		}
	})
	
	observeEvent(event_data("plotly_click", source = "scatter_network"), {
		click_data <- event_data("plotly_click", source = "scatter_network")
		if (!is.null(click_data)) {
			clicked_id <- click_data$customdata
			update_selected_points(clicked_id, input$knn_text, reactive_clusters$df_hdbscan_network)
		}
	})
	
	# Scatter Plot für Euclidean Clustering
	output$scatter_plot_euclid <- renderPlotly({
		highlight_ids <- selected_points()
		show_clusters <- input$show_clusters == "Yes"
		show_noise <- input$show_noise
		
		plot_data <- reactive_clusters$df_hdbscan_euclid %>%
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
		show_noise <- input$show_noise
		
		plot_data <- reactive_clusters$df_hdbscan_network %>%
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
	output$euclid_cluster_output <- renderText({
		req(reactive_clusters$df_hdbscan_euclid)
		mean_size <- reactive_clusters$df_hdbscan_euclid %>%
			filter(cluster != -1) %>%   # Ignoriere Noise-Cluster
			group_by(cluster) %>%
			summarise(size = n()) %>%
			summarise(mean_size = mean(size)) %>%
			pull(mean_size)
		
		clusters <- reactive_clusters$df_hdbscan_euclid$cluster
		paste(
			"Number of Clusters:", length(unique(clusters[clusters != -1])),
			"\nMean Cluster Size:", mean_size,
			"\nNoise Flows:", sum(clusters == -1)
		)
	})
	
	output$network_cluster_output <- renderText({
		req(reactive_clusters$df_hdbscan_network)
		mean_size <- reactive_clusters$df_hdbscan_network %>%
			filter(cluster != -1) %>%   # Ignoriere Noise-Cluster
			group_by(cluster) %>%
			summarise(size = n()) %>%
			summarise(mean_size = mean(size)) %>%
			pull(mean_size)
		clusters <- reactive_clusters$df_hdbscan_network$cluster
		paste(
			"Number of Clusters:", length(unique(clusters[clusters != -1])),
			"\nMean Cluster Size:",mean_size,
			"\nNoise Flows:", sum(clusters == -1)
		)
	})
	
	
	# Flow Map (Leaflet)
	output$flow_map <- renderLeaflet({
		req(selected_points())
		selected_ids <- selected_points()
		req(selected_ids)
		
		selected_flows <- sf_trips_sub_transformed %>%
			filter(flow_id_temp %in% selected_ids) %>%
			st_set_geometry("line_geom")
		
		leaflet() %>%
			addProviderTiles(providers$CartoDB.Positron) %>%
			addPolylines(data = sf_network_transformed, color = "gray", weight = 1, opacity = 0.7) %>%
			addPolylines(data = selected_flows, color = "blue", weight = 2, opacity = 1)
	})
	
	output$download <- downloadHandler(
		filename = function() {
			paste0(char_schema, "_sub.rds")
		},
		content = function(file) {
			selected_clusters <- temp_selected_clusters$network
			if (!is.null(selected_clusters) && length(selected_clusters) > 0) {
				selected_flow_ids <- reactive_clusters$df_hdbscan_network %>%
					filter(cluster %in% selected_clusters) %>%
					pull(flow_id)
				print(head(sf_trips_sub_transformed))
				cat("Number of flows in selected cluster: ", length(selected_flow_ids), "\n")
				data_to_download <- sf_trips_sub_transformed %>%
					filter(flow_id_temp %in% selected_flow_ids)
				print(head(data_to_download))
				write_rds(data_to_download, file)
			}
		}
	)
}

shinyApp(ui, server)



