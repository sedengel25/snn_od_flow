Rcpp::sourceCpp("./src/helper_functions.cpp")
source("./src/config.R")
source("./main_functions.R")


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
char_schema <- available_schemas[4, "schema_name"]
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






matrix_pacmap_euclid <- np$load(here::here(path_python, 
																		char_schema,
																		"flow_manhattan_pts_euclid",
																		"embedding_3d_1.npy")) 
df_pacmap_euclid <-  py_hdbscan(np, hdbscan, matrix_pacmap_euclid, 20)

matrix_pacmap_network <- np$load(here::here(path_python, 
																		char_schema,
																		"flow_manhattan_pts_network",
																		"embedding_3d_5.npy")) 
	
df_pacmap_network <-  py_hdbscan(np, hdbscan, matrix_pacmap_network, 20)




library(shiny)
library(plotly)
library(ggplot2)
library(sf)
library(leaflet)

# Shiny App
ui <- fluidPage(
	titlePanel("Interactive Flow Selection"),
	fluidRow(
		column(6, plotlyOutput("scatter_plot_euclid", height = "600px")),
		column(6, plotlyOutput("scatter_plot_network", height = "600px"))
	),
	fluidRow(
		column(12, leafletOutput("flow_map", height = "600px"))
	),
	fluidRow(
		column(12, sliderInput("knn_slider", "Number of Nearest Neighbors (k):", min = 5, max = 500, value = 50))
	)
)

server <- function(input, output, session) {
	# Funktion, um die k nächsten Nachbarn zu berechnen
	get_nearest_neighbors <- function(data, selected_id, k) {
		selected_point <- data %>% filter(flow_id == selected_id) %>% select(x, y, z)
		all_points <- data %>% select(x, y, z)
		nn_indices <- nn2(data = all_points, query = selected_point, k = k)$nn.idx
		data[nn_indices, ]$flow_id
	}
	
	# Transformiere die Geometrien ins WGS 84 (EPSG:4326)
	sf_network_transformed <- st_transform(sf_network, crs = 4326)
	sf_trips_sub_transformed <- sf_trips_sub %>% 
		mutate(line_geom = st_transform(line_geom, crs = 4326))
	print(head(sf_trips_sub_transformed))
	print(head(sf_network_transformed))
	# Reactive: Ausgewählte Punkte und Modus (2D oder 3D)
	is_3d <- reactive({ "z" %in% colnames(df_pacmap_euclid) })
	
	selected_points <- reactiveVal()
	
	observeEvent(event_data("plotly_selected", source = "scatter_euclid"), {
		if (!is_3d()) {
			points <- event_data("plotly_selected", source = "scatter_euclid")
			selected_points(points$customdata)
		}
	})
	
	observeEvent(event_data("plotly_click", source = "scatter_euclid"), {
		if (is_3d()) {
			click_data <- event_data("plotly_click", source = "scatter_euclid")
			if (!is.null(click_data)) {
				clicked_id <- click_data$customdata
				selected_ids <- get_nearest_neighbors(df_pacmap_euclid, clicked_id, input$knn_slider)
				selected_points(selected_ids)
			}
		}
	})
	
	observeEvent(event_data("plotly_selected", source = "scatter_network"), {
		if (!is_3d()) {
			points <- event_data("plotly_selected", source = "scatter_network")
			selected_points(points$customdata)
		}
	})
	
	observeEvent(event_data("plotly_click", source = "scatter_network"), {
		if (is_3d()) {
			click_data <- event_data("plotly_click", source = "scatter_network")
			if (!is.null(click_data)) {
				clicked_id <- click_data$customdata
				selected_ids <- get_nearest_neighbors(df_pacmap_network, clicked_id, input$knn_slider)
				selected_points(selected_ids)
			}
		}
	})
	
	# Scatter Plot für Euclidean Clustering
	output$scatter_plot_euclid <- renderPlotly({
		highlight_ids <- selected_points()
		
		plot_data <- df_pacmap_euclid %>%
			mutate(color = ifelse(flow_id %in% highlight_ids, "Selected", "Default"))
		
		plot_type <- if (is_3d()) "scatter3d" else "scatter"
		
		plot_ly(
			data = plot_data,
			x = ~x,
			y = ~y,
			z = if (is_3d()) ~z else NULL,
			type = plot_type,
			mode = "markers",
			marker = list(size = 3),
			color = ~color,
			colors = c("Default" = "gray", "Selected" = "blue"),
			text = ~paste("Flow ID:", flow_id),
			customdata = ~flow_id,
			source = "scatter_euclid"
		) %>%
			layout(title = "Euclidean Clustering")
	})
	
	# Scatter Plot für Network Clustering
	output$scatter_plot_network <- renderPlotly({
		highlight_ids <- selected_points()
		
		plot_data <- df_pacmap_network %>%
			mutate(color = ifelse(flow_id %in% highlight_ids, "Selected", "Default"))
		
		plot_type <- if (is_3d()) "scatter3d" else "scatter"
		
		plot_ly(
			data = plot_data,
			x = ~x,
			y = ~y,
			z = if (is_3d()) ~z else NULL,
			type = plot_type,
			mode = "markers",
			marker = list(size = 3),
			color = ~color,
			colors = c("Default" = "gray", "Selected" = "red"),
			text = ~paste("Flow ID:", flow_id),
			customdata = ~flow_id,
			source = "scatter_network"
		) %>%
			layout(title = "Network Clustering")
	})
	
	# Flow Map (Leaflet)
	# Flow Map (Leaflet)
	output$flow_map <- renderLeaflet({
		req(selected_points())
		selected_ids <- selected_points()
		
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
