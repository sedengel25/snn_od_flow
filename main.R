################################################################################
# Source
################################################################################
source("./src/config.R")
source("./src/functions.R")

################################################################################
# Configuration
################################################################################

char_path_osm2po <- "/home/sebastiandengel"

char_path_osm2po <- here::here(char_path_osm2po, "osm2po-5.5.5")

list.files(path = char_path_osm2po, pattern = "sh")

char_region <- "sachsen"

char_sh_file <- paste0(char_region, ".sh")

char_cmd_chmod <- paste("chmod +x", shQuote(file.path(char_path_osm2po,
																											char_sh_file)))

int_exit_status <- system(char_cmd_chmod)

if(int_exit_status == 0){
	cat("Permission access for ", char_sh_file,"successful\n")
}

char_cmd_osm2po <- paste0("cd ", shQuote(char_path_osm2po), 
										 " && ./", shQuote(char_sh_file))

char_lines <- readLines(file.path(char_path_osm2po,
														 char_sh_file))
char_prefix_value <- sub(".*prefix=([^ ]+).*", "\\1", char_lines)

char_path_region <- here::here(char_path_osm2po, char_prefix_value)

p <- processx::process$new("bash", 
													 args = c("-c", char_cmd_osm2po),
													 stdout = "|", stderr = "|")

proc_time_1 <- proc.time()

while (!p$is_alive() || p$is_alive()) {

	Sys.sleep(1)
	timeout <- 180
	output <- p$read_output_lines()
	error_output <- p$read_error_lines()
	finished_message <- "INFO  All Services STARTED. Press Ctrl-C to stop them."
	if(length(output) > 0 && finished_message %in% output){

		char_sql_filename <- paste0(char_prefix_value, "_2po_4pgr.sql")
		
		if(char_sql_filename %in% list.files(path = char_path_region,
																				 pattern = "*.sql")){
			print("SQL file for routable road network successfully created")
			p$kill()
			break
		}

	}
	proc_time_2 <- proc.time()
	time_diff <- ((proc_time_2 - proc_time_1) %>% as.numeric)[[3]]  
	if(time_diff > timeout){
		print("Timeout")
		p$kill()
	}
}








