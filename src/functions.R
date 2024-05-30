source("./src/utils/cmd.R")
source("./src/utils/psql_1.R")

# Documentation: osmconvert_create_sub_osm_pbf
# Usage: osmconvert_create_sub_osm_pbf()
# Description: Creates a sub osm.pbf-file based on the polygon specified
# Args/Options: ...
# Returns: ...
# Output: ...
# Action: Executes a 'osmconvert' command
osmconvert_create_sub_osm_pbf <- function() {

	
	
	char_cmd_osmconvert <- paste(
		"osmconvert", 
		shQuote(file_ger_osm_pbf),
		paste("-B=", shQuote(char_polygon_file), sep=""), 
		paste("-o=", shQuote(char_output_file), sep="")
	)
	
	print(char_cmd_osmconvert)
	
	
	int_exit_status <- system(char_cmd_osmconvert)
	
	if(int_exit_status == 0){
		print(paste0(chat_output_filename, " successfully created in ", path_osm_pbf))
	}
}


# Documentation: osm2po_create_routable_network
# Usage: osm2po_create_routable_network()
# Description: Creates in osm2po a sql-file that generates a network based on the
# chosen region in the corresponding or sh-file
# Args/Options: ...
# Returns: ...
# Output: ...
# Action: Executing several cmd- and psql-queries
osm2po_create_routable_network <- function() {
	
	# Prepare system command for executing the sh-file
	char_cmd_osm2po <- paste(
		"java -Xmx1g -jar", 
		shQuote(path_osm2po_jar),
		paste("prefix=", shQuote(char_region_abb), sep=""),
		paste("tileSize=", shQuote("x"), sep=""),
		shQuote(char_output_file),
		"postp.0.class=de.cm.osm2po.plugins.postp.PgRoutingWriter"
	)
	
	print(char_cmd_osm2po)
	# Use 'prossex'-lib to control system processes in the backrgound
	p <- processx::process$new("bash", 
														 args = c("-c", char_cmd_osm2po),
														 stdout = "|", stderr = "|")
	proc_time_1 <- proc.time()
	
	while (!p$is_alive() || p$is_alive()) {
		
		Sys.sleep(1)
		timeout <- 180
		# Get output of the process each second
		output <- p$read_output_lines()
		error_output <- p$read_error_lines()
		finished_message <- "INFO  All Services STARTED. Press Ctrl-C to stop them."
		# If output contains the finishing message...
		if(length(output) > 0 && finished_message %in% output){
			
			#...and if the sql-file got created...
			if(char_sql_filename %in% list.files(path = char_region_abb,
																					 pattern = "*.sql")){
				print("SQL file for routable road network successfully created")
				#...the process is killed the while-loop breaks.
				p$kill()
				break
			}
			
		}
		proc_time_2 <- proc.time()
		time_diff <- ((proc_time_2 - proc_time_1) %>% as.numeric)[[3]]  
		# If the sql-file is not created after 3 minutes a timeout is thrown
		if(time_diff > timeout){
			print("Timeout")
			p$kill()
		}
	}
	
	
	# Execute SQL file to write table in PSQL database
	bash_execute_sql_file(path_to_sql_file = here::here(char_region_abb, 
																											char_sql_filename))
	
	
	
	# Transform coordinate system
	srid <- psql1_get_srid(con, table = char_network)
	psql1_set_srid(con, table = char_network, srid)
	psql1_transform_coordinates(con, table = char_network)
	psql1_update_srid(con, table = char_network, crs = 32632)
}
