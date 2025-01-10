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
char_network <- available_networks[1, "table_name"]
sf_network <- st_read(con, char_network) 
#################################################################################
# 2. Get original OD flow data based on subsets
################################################################################
subset <- c(15001, 20001)
char_schema <- paste0("data_", paste0(subset, collapse = "_"))
available_mapped_trip_data <- psql1_get_mapped_trip_data(con)
print(available_mapped_trip_data)
char_data <- available_mapped_trip_data[1, "table_name"]
sf_trips <- st_read(con, char_data) %>%
	rename("origin_id" = "id_edge_origin",
				 "dest_id" = "id_edge_dest") 

sf_trips <- sf_trips %>%
	arrange(start_datetime) %>%
	filter(trip_distance >= 100)

sf_trips_sub <- sf_trips %>%
	mutate(flow_id = 1:nrow(sf_trips)) %>%
	filter(flow_id >= min(subset) & flow_id <= max(subset))
sf_trips_sub$flow_id_temp <- 1:nrow(sf_trips_sub)






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


# Shiny App
ui <- fluidPage(
	titlePanel("Interactive Flow Selection"),
	fluidRow(
		column(6, plotlyOutput("scatter_plot_euclid")),
		column(6, plotlyOutput("scatter_plot_network"))
	),
	fluidRow(
		column(12, plotOutput("flow_plot"))
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
	
	# Flow Plot (zentral)
	output$flow_plot <- renderPlot({
		req(selected_points())
		selected_ids <- selected_points()
		
		selected_flows <- sf_trips_sub %>% filter(flow_id_temp %in% selected_ids)
		
		ggplot() +
			geom_sf(data = sf_network, aes(geometry = geom_way), color = "gray", size = 0.5) +
			geom_sf(data = selected_flows, aes(geometry = line_geom, color = "Selected Flows"), size = 1) +
			scale_color_manual(values = c("Selected Flows" = "blue")) +
			labs(title = "Selected Flows on Road Network", color = "Flow Type") +
			theme_minimal()
	})
}

shinyApp(ui, server)
