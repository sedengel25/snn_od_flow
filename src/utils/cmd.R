# Documentation: bash_execute_sql_file
# Usage: bash_execute_sql_file(path_to_sql_file)
# Description: Runs sql file
# Args/Options: path_to_sql_file
# Returns: ...
# Output: ...
# Action: Executing a cmd-statement to run a sql-file
bash_execute_sql_file <- function(path_to_sql_file) {
	Sys.setenv(PGPASSWORD = pw)
	bash_psql <- sprintf('%s -h %s -p %s -d %s -U %s -f %s', 
											 shQuote(external_path_psql), 
											 shQuote(host), 
											 shQuote(port), 
											 shQuote(dbname), 
											 shQuote(user), 
											 shQuote(path_to_sql_file))
	
	system(bash_psql)
	Sys.unsetenv("PGPASSWORD")
}