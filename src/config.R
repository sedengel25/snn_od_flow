################################################################################
# Postgresql-DB connection
################################################################################
library(dplyr)
library(here)


################################################################################
# Postgresql-DB connection
################################################################################
config = yaml.load_file("config.yml")
host <- config$psql_db$host
user <- config$psql_db$user
pw <- config$psql_db$password
dbname <- config$psql_db$dbname
port <- config$psql_db$port

if(!exists("con")){
	con <- dbConnect(RPostgres::Postgres(), 
									 dbname = dbname, 
									 host = host, 
									 user = user, 
									 password = pw,
									 port = port,
									 options="-c client_min_messages=warning")
}


char_os <-   Sys.info()["sysname"] %>% as.character()
int_cores <- detectCores()-2