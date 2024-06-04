source("./src/utils/psql_2.R")
# Documentation: psql1_get_srid
# Usage: psql1_get_srid(con, table)
# Description: Get SRID of psql-table
# Args/Options: con, table
# Returns: ...
# Output: ...
# Action: Execute psql-query 
psql1_get_srid <- function(con, table) {
	name_geom_col <- psql2_get_name_of_geom_col(con, table)
	query <- paste0("SELECT ST_SRID(", name_geom_col, ") FROM ", table, " LIMIT 1;")
	srid <- dbGetQuery(con, query) %>% as.integer
	return(srid)
}


# Documentation: psql1_set_srid
# Usage: psql1_set_srid(con, table)
# Description: Set SRID of psql-table
# Args/Options: con, table
# Returns: ...
# Output: ...
# Action: Execute psql-query 
psql1_set_srid <- function(con, table, srid) {
	name_geom_col <- psql2_get_name_of_geom_col(con, table)
	query <- paste0("UPDATE ",
									table,
									" SET ",
									name_geom_col,
									" = ST_SetSRID(",
									name_geom_col,
									",",
									srid,
									");")
	dbExecute(con, query)
}



# Documentation: psql1_update_srid
# Usage: psql1_update_srid(con, table, crs)
# Description: Update crs of geom-column of chosen table
# Args/Options: con, table, crs
# Returns: ...
# Output: ...
# Action: Executing a psql-query
psql1_update_srid <- function(con, table, crs) {
	name_geom_col <- psql2_get_name_of_geom_col(con, table)
	query <- paste0("SELECT UpdateGeometrySRID('",
									table,
									"','",
									name_geom_col,
									"',",
									crs,
									");")
	dbExecute(con, query)
}

# Documentation: psql1_transform_coordinates
# Usage: psql1_transform_coordinates(con, table)
# Description: Change types of geometry-col of table considered
# Args/Options: con, table
# Returns: ...
# Output: ...
# Action: Execute psql-query
psql1_transform_coordinates <- function(con, table) {
	
	name_geom_col <- psql2_get_name_of_geom_col(con, table)
	type_geom_col <- psql2_get_geometry_type(con, table)
	query <- paste0("ALTER TABLE ", 
									table, 
									" ALTER COLUMN ",
									name_geom_col,
									" TYPE geometry(",
									type_geom_col,
									", ",
									32632, 
									") USING ST_Transform(",
									name_geom_col,
									",",
									32632, ");")
	dbExecute(con, query)
}


# Documentation: psql1_create_spatial_index
# Usage: psql1_create_spatial_index(con, char_table)
# Description: Creates a spatial index for chosen table on chosen column
# Args/Options: con, char_table, col
# Returns: ...
# Output: ...
# Action: psql-query
psql1_create_spatial_index <- function(con, table) {
	
	char_idx <- paste0(table, "_geometry_idx")

	query <- paste0("DROP INDEX IF EXISTS ", char_idx)
	dbExecute(con, query)
	
	
	name_geom_col <- psql2_get_name_of_geom_col(con, table)
	print(name_geom_col)
	query <- paste0("CREATE INDEX ON ",  table, " USING GIST (",
									name_geom_col,");")
	print(query)
	dbExecute(con, query)
}



# Documentation: psql1_create_index
# Usage: psql1_create_index(con, char_table, col)
# Description: Creates a index for chosen table on chosen column
# Args/Options: con, char_table, col
# Returns: ...
# Output: ...
# Action: psql-query
psql1_create_index <- function(con, table, col) {
	
	char_idx <- paste0("idx_", table)
	
	query <- paste0("DROP INDEX IF EXISTS ", char_idx)
	dbExecute(con, query)
	
	query <- paste0("CREATE INDEX ", 
									char_idx,
									" ON ",  
									table, 
									" USING btree (", 
									paste(col, collapse = ", "), 
									");")
	
	dbExecute(con, query)
}

