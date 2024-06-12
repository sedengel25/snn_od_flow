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

# Documentation: psql1_map_od_pts_to_network
# Usage: psql1_map_od_pts_to_network(con, table_network)
# Description: Maps OD points onto network using PostGIS-functions
# Args/Options: con, table_network
# Returns: ...
# Output: ...
# Action: psql-query
psql1_map_od_points_onto_network <- function(con, table_network) {
	query <- paste0("ALTER TABLE mapped_points
  ADD COLUMN o_dist_to_start double precision,
  ADD COLUMN o_dist_to_end double precision,
  ADD COLUMN d_dist_to_start double precision,
  ADD COLUMN d_dist_to_end double precision,
  ADD COLUMN id_edge bigint;")
	dbExecute(con, query)
	
	query <- paste0("
  UPDATE mapped_points mp
  SET 
      origin_geom = sub.origin_geom,
      dest_geom = sub.dest_geom,
      o_dist_to_start = sub.o_dist_to_start,
      o_dist_to_end = sub.o_dist_to_end,
      d_dist_to_start = sub.d_dist_to_start,
      d_dist_to_end = sub.d_dist_to_end,
      id_edge = sub.id
  FROM (
      SELECT
          p.id_new,
          n.id,
          ST_ClosestPoint(n.geom_way, p.origin_geom) AS origin_geom,
          ST_ClosestPoint(n.geom_way, p.dest_geom) AS dest_geom,
          ST_Distance(ST_ClosestPoint(n.geom_way, p.origin_geom), ST_StartPoint(n.geom_way)) AS o_dist_to_start,
          ST_Distance(ST_ClosestPoint(n.geom_way, p.origin_geom), ST_EndPoint(n.geom_way)) AS o_dist_to_end,
          ST_Distance(ST_ClosestPoint(n.geom_way, p.dest_geom), ST_StartPoint(n.geom_way)) AS d_dist_to_start,
          ST_Distance(ST_ClosestPoint(n.geom_way, p.dest_geom), ST_EndPoint(n.geom_way)) AS d_dist_to_end
      FROM
          mapped_points p
  		CROSS JOIN LATERAL
  		  (SELECT id, geom_way
  		   FROM ", table_network, "
  		   ORDER BY geom_way <-> p.origin_geom
  		   LIMIT 1
  		  ) AS n
  ) sub
  WHERE mp.id_new = sub.id_new;")
	cat(query)
	dbExecute(con, query)
}
