library(shiny)
library(leaflet)
library(leaflet.extras)
library(sf)

ui <- fluidPage(
	titlePanel("Polygon-Koordinaten für osmconvert"),
	leafletOutput("map"),
	downloadButton("download", "Download Polygon Coordinates")
)

server <- function(input, output, session) {
	output$map <- renderLeaflet({
		leaflet() %>%
			addTiles() %>%
			addDrawToolbar(
				targetGroup = 'drawnItems',
				polylineOptions = FALSE,
				polygonOptions = drawPolygonOptions(showArea = TRUE),
				circleOptions = FALSE,
				rectangleOptions = FALSE,
				markerOptions = FALSE,
				editOptions = editToolbarOptions()
			)
	})
	
	# Speichern der Koordinaten als reaktives Objekt
	coords_reactive <- reactiveVal()
	
	observeEvent(input$map_draw_new_feature, {
		feature <- input$map_draw_new_feature
		if(feature$geometry$type == "Polygon") {
			coords <- feature$geometry$coordinates[[1]]
			coords_reactive(coords)
		}
	})
	
	# Erstellen der Download-Funktion
	output$download <- downloadHandler(
		filename = function() {
			paste("polygon_coords.txt")
		},
		content = function(file) {
			coords <- coords_reactive()
			if (!is.null(coords)) {
				writeLines(sapply(coords, function(coord) {
					paste(coord[1], coord[2])
				}), con = file)
			}
		}
	)
}

shinyApp(ui, server)
