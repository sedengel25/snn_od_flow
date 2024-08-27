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
psql1_map_od_points_onto_network <- function(con, table_network, table_trips) {
	# Hinzufügen der notwendigen Spalten
	query <- paste0("ALTER TABLE ", table_trips, "
    ADD COLUMN IF NOT EXISTS o_closest_point geometry(Point, 32632),
    ADD COLUMN IF NOT EXISTS d_closest_point geometry(Point, 32632),
    ADD COLUMN IF NOT EXISTS o_dist_to_start double precision,
    ADD COLUMN IF NOT EXISTS o_dist_to_end double precision,
    ADD COLUMN IF NOT EXISTS d_dist_to_start double precision,
    ADD COLUMN IF NOT EXISTS d_dist_to_end double precision,
    ADD COLUMN IF NOT EXISTS id_edge_origin bigint,
    ADD COLUMN IF NOT EXISTS id_edge_dest bigint,
    ADD COLUMN IF NOT EXISTS trip_distance double precision;")
	dbExecute(con, query)
	
	# Update-Query für origin_geom
	query_origin <- paste0("
    UPDATE ", table_trips, " mp
    SET 
      o_closest_point = sub.o_closest_point,
      o_dist_to_start = sub.o_dist_to_start,
      o_dist_to_end = sub.o_dist_to_end,
      id_edge_origin = sub.id_edge_origin
    FROM (
      SELECT
        p.id_new,
        n.id AS id_edge_origin,
        ST_ClosestPoint(n.geom_way, p.origin_geom) AS o_closest_point,
        ST_Distance(ST_ClosestPoint(n.geom_way, p.origin_geom), ST_StartPoint(n.geom_way)) AS o_dist_to_start,
        ST_Distance(ST_ClosestPoint(n.geom_way, p.origin_geom), ST_EndPoint(n.geom_way)) AS o_dist_to_end
      FROM
        ", table_trips, " p
      CROSS JOIN LATERAL
        (SELECT id, geom_way
         FROM ", table_network, "
         ORDER BY geom_way <-> p.origin_geom
         LIMIT 1
        ) AS n
    ) sub
    WHERE mp.id_new = sub.id_new;")
	cat(query_origin)
	dbExecute(con, query_origin)
	
	# Update-Query für dest_geom
	query_dest <- paste0("
    UPDATE ", table_trips, " mp
    SET 
      d_closest_point = sub.d_closest_point,
      d_dist_to_start = sub.d_dist_to_start,
      d_dist_to_end = sub.d_dist_to_end,
      id_edge_dest = sub.id_edge_dest
    FROM (
      SELECT
        p.id_new,
        n.id AS id_edge_dest,
        ST_ClosestPoint(n.geom_way, p.dest_geom) AS d_closest_point,
        ST_Distance(ST_ClosestPoint(n.geom_way, p.dest_geom), ST_StartPoint(n.geom_way)) AS d_dist_to_start,
        ST_Distance(ST_ClosestPoint(n.geom_way, p.dest_geom), ST_EndPoint(n.geom_way)) AS d_dist_to_end
      FROM
        ", table_trips, " p
      CROSS JOIN LATERAL
        (SELECT id, geom_way
         FROM ", table_network, "
         ORDER BY geom_way <-> p.dest_geom
         LIMIT 1
        ) AS n
    ) sub
    WHERE mp.id_new = sub.id_new;")
	cat(query_dest)
	dbExecute(con, query_dest)
	
	# Update-Query für trip_distance
	query_trip_distance <- paste0("
    UPDATE ", table_trips, "
    SET trip_distance = ST_Distance(origin_geom, dest_geom);")
	cat(query_trip_distance)
	dbExecute(con, query_trip_distance)
}







# Documentation: psql1_calc_nd_diff_roads
# Usage: psql1_calc_nd_diff_roads(con, o_d, table_mapped_points, table_network,
# table_dist_mat, table_nd, id_start, id_end)
# Description: Execute query calculating the NDs between all Os and Ds. It is
# more advanced than 'psql_calc_nd' since the query itself is not only 
# parallelized for the Origin- & dest-Points but for differently sized chunks
# of the IDs to compute
# Args/Options: con, o_d, table_mapped_points, table_network,
# table_dist_mat, table_nd, id_start, id_end
# Returns: ...
# Output: ...
# Action: Executing a psql-query
psql1_calc_nd_diff_roads <- function(con, 
																		 table_pts, 
																		 table_network,
																		 table_dist_mat, 
																		 table_nd, 
																		 id_start, 
																		 id_end) {
	
	con <- dbConnect(RPostgres::Postgres(),
									 dbname = dbname,
									 host = host,
									 port = 5432,
									 user = user,
									 password = pw,
									 options="-c client_min_messages=warning")
	
	
	query <- paste0(
		"INSERT INTO ", table_nd, " SELECT m1.id_new as o_m, m2.id_new as o_n, 
		LEAST(
		m1.dist_to_start + m1.dist_to_start + COALESCE(pi_pk.m, 0),
		m2.dist_to_end + m2.dist_to_end + COALESCE(pj_pl.m, 0),
		m1.dist_to_start + m2.dist_to_end + COALESCE(pi_pl.m, 0),
		m2.dist_to_end + m1.dist_to_start + COALESCE(pj_pk.m, 0)
		) AS nd ",
		"FROM ", table_pts, " m1 ",
		"CROSS JOIN ", table_pts, " m2 ",
		"INNER JOIN ", table_network, " e_ij ON m1.id_edge = e_ij.id ",
		"INNER JOIN ", table_network, " e_kl ON m2.id_edge = e_kl.id ",
		"INNER JOIN ", table_dist_mat, " pi_pk ON pi_pk.source = LEAST(e_ij.source, e_kl.source) AND pi_pk.target = GREATEST(e_ij.source, e_kl.source) ",
		"INNER JOIN ", table_dist_mat, " pj_pl ON pj_pl.source = LEAST(e_ij.target, e_kl.target) AND pj_pl.target = GREATEST(e_ij.target, e_kl.target) ",
		"INNER JOIN ", table_dist_mat, " pi_pl ON pi_pl.source = LEAST(e_ij.source, e_kl.target) AND pi_pl.target = GREATEST(e_ij.source, e_kl.target) ",
		"INNER JOIN ", table_dist_mat, " pj_pk ON pj_pk.source = LEAST(e_ij.target, e_kl.source) AND pj_pk.target = GREATEST(e_ij.target, e_kl.source) ",
		"WHERE m1.id_new >= ", id_start, 
		" AND m1.id_new <= ", id_end, 
		" AND m1.id_new < m2.id_new	AND m1.id_edge != m2.id_edge;")
	cat(query)
	dbExecute(con, query)
	
	
}

# Documentation: psql1_create_visualisable_flows
# Usage: psql1_create_visualisable_flows(con, table_origin, table_dest)
# Description: Binds points to linestrings so flows can be visualized
# Args/Options: con, table_origin, table_dest
# Returns: ...
# Output: ...
# Action: Binds points to linestrings
psql1_create_visualisable_flows <- function(con, table_origin, table_dest, table_vis) {
	geo_col <- psql2_get_name_of_geom_col(con, table_origin)
	query <- paste0("CREATE TABLE ",  table_vis, " AS
  SELECT origin.id,
    ST_MakeLine(origin.", geo_col, ", dest.", geo_col,") AS line_geom
  FROM ",  table_origin, " origin
  INNER JOIN ", table_dest, " dest ON origin.id = dest.id;")
	
	dbExecute(con, query)
}



psql1_get_available_networks <- function(con) {
	query <- paste0("SELECT table_schema, table_name
  FROM information_schema.tables
  WHERE table_name LIKE '%2po_4pgr%'
    AND table_type = 'BASE TABLE'
    AND table_schema NOT IN ('pg_catalog', 'information_schema');
  ")
	available_networks <- dbGetQuery(con, query)
	return(available_networks)
}

psql1_get_raw_trip_data <- function(con) {
	query <- paste0("SELECT table_schema, table_name
  FROM information_schema.tables
  WHERE table_name NOT LIKE '%cluster%'
    AND table_name NOT LIKE '%2po_4pgr%'
     AND table_name NOT LIKE '%mapped%'
    AND table_type = 'BASE TABLE'
    AND table_schema NOT IN ('pg_catalog', 'information_schema');
  ")
	
	available_trip_data <- dbGetQuery(con, query)
	return(available_trip_data)
}


psql1_get_mapped_trip_data <- function(con) {
	query <- paste0("SELECT table_schema, table_name
  FROM information_schema.tables
  WHERE table_name NOT LIKE '%cluster%'
    AND table_name NOT LIKE '%2po_4pgr%'
     AND table_name LIKE '%mapped%'
    AND table_type = 'BASE TABLE'
    AND table_schema NOT IN ('pg_catalog', 'information_schema');
  ")
	
	available_trip_data <- dbGetQuery(con, query)
	return(available_trip_data)
}


psql1_get_cluster_tables <- function(con) {
	query <- paste0("SELECT table_schema, table_name
  FROM information_schema.tables
  WHERE table_name LIKE '%cluster%'
    AND table_type = 'BASE TABLE'
    AND table_schema NOT IN ('pg_catalog', 'information_schema');
  ")
	
	char_availabe_cluster_tables <- dbGetQuery(con, query)
	return(char_availabe_cluster_tables)
}
