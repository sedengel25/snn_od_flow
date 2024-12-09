# Documentation: psql2_get_name_of_geom_col
# Usage: psql2_get_name_of_geom_col(con, table)
# Description: Get name of the geometry column
# Args/Options: con, table
# Returns: ...
# Output: ...
# Action: Execute psql-query 
psql2_get_name_of_geom_col <- function(con, table, schema) {
# 	query <- paste0("SELECT column_name
#   FROM information_schema.columns
#   WHERE table_name = '",
# 									table,
# 									"' AND udt_name = 'geometry';")
# 	
	query <- paste0("SELECT f_geometry_column as name
	FROM geometry_columns
	WHERE f_table_schema = '", schema, "' 
	  AND f_table_name = '", table, "';")
	col_name_df <- dbGetQuery(con, query) 
	
	col_name_vec <- paste0(as.character(col_name_df$name), collapse = ", ")

	return(col_name_vec)
}



# Documentation: psql2_get_geometry_type
# Usage: psql2_get_geometry_type(con, table)
# Description: Get type of the geometry column
# Args/Options: con, table
# Returns: ...
# Output: ...
# Action: Execute psql-query 
psql2_get_geometry_type <- function(con, table) {
	name_geom_col <- psql2_get_name_of_geom_col(con, table)
	query <- paste0("SELECT DISTINCT GeometryType(",
									name_geom_col, ")FROM ", table, ";")
	
	geom_types <- dbGetQuery(con, query) %>% as.character()
	return(geom_types)
}


