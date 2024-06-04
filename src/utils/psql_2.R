# Documentation: psql2_get_name_of_geom_col
# Usage: psql2_get_name_of_geom_col(con, table)
# Description: Get name of the geometry column
# Args/Options: con, table
# Returns: ...
# Output: ...
# Action: Execute psql-query 
psql2_get_name_of_geom_col <- function(con, table) {
	query <- paste0("SELECT column_name
  FROM information_schema.columns
  WHERE table_schema = 'public' AND table_name = '",
									table,
									"' AND udt_name = 'geometry';")
	
	col_name_df <- dbGetQuery(con, query) 
	
	col_name_vec <- paste0(as.character(col_name_df$column_name), collapse = ", ")
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


