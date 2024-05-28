source("./src/utils/cmd.R")
source("./src/utils/psql_1.R")

# Documentation: osm2po_create_routable_network
# Usage: osm2po_create_routable_network()
# Description: Creates in osm2po a sql-file that generates a network based on the
# chosen region in the corresponding or sh-file
# Args/Options: ...
# Returns: ...
# Output: ...
# Action: Executing several cmd- and psql-queries
osm2po_create_routable_network <- function() {
	
	# Prepare system-command to be executed...
	char_cmd_chmod <- paste("chmod +x", shQuote(file.path(external_path_osm2po,
																												char_sh_file)))
	
	# ...to permit access to sh-file (necessary on Linux-systems).
	int_exit_status <- system(char_cmd_chmod)
	
	if(int_exit_status != 0){
		stop("No Permission access for ", char_sh_file,"\n")
	}
	
	# Prepare system command for executing the sh-file
	char_cmd_osm2po <- paste0("cd ", shQuote(external_path_osm2po), 
														" && ./", shQuote(char_sh_file))
	
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
			if(char_sql_filename %in% list.files(path = char_path_region,
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
	bash_execute_sql_file(path_to_sql_file = here::here(char_path_region, 
																											char_sql_filename))
	
	
	
	# Transform coordinate system
	srid <- psql1_get_srid(con, table = char_network)
	psql1_set_srid(con, table = char_network, srid)
	psql1_transform_coordinates(con, table = char_network)
	psql1_update_srid(con, table = char_network, crs = 32632)
}