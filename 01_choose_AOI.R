library(shiny)
library(leaflet)
library(leaflet.extras)
library(sf)

ui <- fluidPage(
	titlePanel("Polygon-Coordinates for osmconvert"),
	textInput("filename", "Name your file:", value = "polygon_coords"),
	leafletOutput("map"),
	downloadButton("download", "Download Polygon Coordinates")
)

server <- function(input, output, session) {
	output$map <- renderLeaflet({
		leaflet() %>%
			setView(lng = 10.4515, lat = 51.1657, zoom = 6) %>% # Zentriert auf Deutschland
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
	
	coords_reactive <- reactiveVal()  # Speichern der Koordinaten als reaktives Objekt
	
	observeEvent(input$map_draw_new_feature, {
		feature <- input$map_draw_new_feature
		if(feature$geometry$type == "Polygon") {
			coords <- feature$geometry$coordinates[[1]]
			coords_reactive(coords)
		}
	})
	
	output$download <- downloadHandler(
		filename = function() {
			paste0(input$filename, ".poly")  # Dateiendung auf .poly setzen
		},
		content = function(file) {
			coords <- coords_reactive()
			if (!is.null(coords)) {
				# Datei auf dem Server im Verzeichnis "bbox_coordinates" speichern
				dir.create("data/bbox_coordinates", showWarnings = FALSE, recursive = TRUE)
				server_file <- file.path("data", "bbox_coordinates", paste0(input$filename, ".poly"))
				
				# Koordinaten in das POLY-Format schreiben
				lines <- c(input$filename, "1")  # Polygon-Namen und ID
				coord_lines <- sapply(coords, function(coord) {
					paste0(" ", coord[1], " ", coord[2])
				})
				lines <- c(lines, coord_lines, "END", "END")  # Polygon- und Dateiabschluss
				
				writeLines(lines, con = server_file)
				
				# Datei zum Download bereitstellen
				file.copy(server_file, file, overwrite = TRUE)
			}
		}
	)
	
}

shinyApp(ui, server)
