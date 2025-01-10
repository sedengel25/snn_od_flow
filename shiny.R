library(shiny)
library(plotly)
library(dplyr)
library(RANN)  # Für die schnelle Berechnung der k nächsten Nachbarn
library(ggplot2)
library(sf)

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
	is_3d <- reactive({ "z" %in% colnames(df_cluster_euclid) })
	
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
				selected_ids <- get_nearest_neighbors(df_cluster_euclid, clicked_id, input$knn_slider)
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
				selected_ids <- get_nearest_neighbors(df_cluster_network, clicked_id, input$knn_slider)
				selected_points(selected_ids)
			}
		}
	})
	
	# Scatter Plot für Euclidean Clustering
	output$scatter_plot_euclid <- renderPlotly({
		highlight_ids <- selected_points()
		
		plot_data <- df_cluster_euclid %>%
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
		
		plot_data <- df_cluster_network %>%
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
