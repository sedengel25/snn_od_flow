source("./src/utils/psql_2.R")
# Documentation: psql1_get_srid
# Usage: psql1_get_srid(con, table)
# Description: Get SRID of psql-table
# Args/Options: con, table
# Returns: ...
# Output: ...
# Action: Execute psql-query 
psql1_get_srid <- function(con, table, schema) {
	name_geom_col <- psql2_get_name_of_geom_col(con, table, schema)
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
psql1_set_srid <- function(con, table, srid, schema) {
	name_geom_col <- psql2_get_name_of_geom_col(con, table, schema)
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
psql1_update_srid <- function(con, table, crs, schema) {
	name_geom_col <- psql2_get_name_of_geom_col(con, table, schema)
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
psql1_transform_coordinates <- function(con, table, crs, schema) {
	
	name_geom_col <- psql2_get_name_of_geom_col(con, table, schema)
	type_geom_col <- psql2_get_geometry_type(con, table, schema)
	query <- paste0("ALTER TABLE ", 
									table, 
									" ALTER COLUMN ",
									name_geom_col,
									" TYPE geometry(",
									type_geom_col,
									", ",
									crs, 
									") USING ST_Transform(",
									name_geom_col,
									",",
									crs, ");")
	dbExecute(con, query)
}


# Documentation: psql1_create_spatial_index
# Usage: psql1_create_spatial_index(con, char_table)
# Description: Creates a spatial index for chosen table on chosen column
# Args/Options: con, char_table, col
# Returns: ...
# Output: ...
# Action: psql-query
psql1_create_spatial_index <- function(con, table, schema) {
	
	char_idx <- paste0(table, "_geometry_idx")

	query <- paste0("DROP INDEX IF EXISTS ", char_idx)
	dbExecute(con, query)

	name_geom_col <- psql2_get_name_of_geom_col(con, table, schema)
	query <- paste0("CREATE INDEX ON ",  
									paste0(schema, ".", table),
									" USING GIST (",
									name_geom_col,");")
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
psql1_map_od_points_onto_network <- function(con, table_network, table_trips, crs) {
	# Hinzufügen der notwendigen Spalten
	query <- paste0("ALTER TABLE ", table_trips, "
    ADD COLUMN IF NOT EXISTS o_closest_point geometry(Point, ", crs, "),
    ADD COLUMN IF NOT EXISTS d_closest_point geometry(Point, ", crs, "),
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


# Documentation: psql1_map_od_pts_to_network
# Usage: psql1_map_od_pts_to_network(con, table_network)
# Description: Maps OD points onto network using PostGIS-functions
# Args/Options: con, table_network
# Returns: ...
# Output: ...
# Action: psql-query
psql1_map_points_onto_network <- function(con, table_network, table_trips, crs) {
	# Hinzufügen der notwendigen Spalten
	query <- paste0("ALTER TABLE ", table_trips, "
    ADD COLUMN IF NOT EXISTS closest_point geometry(Point, ", crs, "),
    ADD COLUMN IF NOT EXISTS dist_to_start double precision,
    ADD COLUMN IF NOT EXISTS dist_to_end double precision,
    ADD COLUMN IF NOT EXISTS id_edge bigint;")
	dbExecute(con, query)
	
	query <- paste0("
    UPDATE ", table_trips, " mp
    SET 
      closest_point = sub.closest_point,
      dist_to_start = sub.dist_to_start,
      dist_to_end = sub.dist_to_end,
      id_edge = sub.id_edge
    FROM (
      SELECT
        p.id,
        n.id AS id_edge,
        ST_ClosestPoint(n.geom_way, p.points) AS closest_point,
        ST_Distance(ST_ClosestPoint(n.geom_way, p.points), ST_StartPoint(n.geom_way)) AS dist_to_start,
        ST_Distance(ST_ClosestPoint(n.geom_way, p.points), ST_EndPoint(n.geom_way)) AS dist_to_end
      FROM
        ", table_trips, " p
      CROSS JOIN LATERAL
        (SELECT id, geom_way
         FROM ", table_network, "
         ORDER BY geom_way <-> p.points
         LIMIT 1
        ) AS n
    ) sub
    WHERE mp.id = sub.id;")
	cat(query)
	dbExecute(con, query)
	
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
  	AND table_name NOT LIKE '%dist%'
  	AND table_name NOT LIKE '%convex_hull%'
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
  WHERE table_name NOT LIKE '%2po_4pgr%'
     AND table_name LIKE '%mapped'
    AND table_type = 'BASE TABLE'
    AND table_schema NOT IN ('pg_catalog', 'information_schema');
  ")
	
	available_trip_data <- dbGetQuery(con, query)
	return(available_trip_data)
}

psql1_get_schemas <- function(con) {
	query <- paste0("SELECT schema_name
	FROM information_schema.schemata
	WHERE schema_name NOT IN ('public', 'pg_catalog', 'information_schema')
  AND schema_name NOT LIKE 'pg_toast%'
  AND schema_name NOT LIKE 'pg_temp%';")
	
	char_availabe_schemas <- dbGetQuery(con, query)
	return(char_availabe_schemas)
}



psql1_get_tables_in_schema <- function(con, schema) {
	# Query mit Platzhalter für das Schema
	query <- paste0("
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_name NOT LIKE '%convex%' 
    AND table_type = 'BASE TABLE'
      AND table_schema = '", schema, "';
  ")
	
	# Abfrage ausführen
	char_availabe_cluster_tables <- dbGetQuery(con, query)
	
	# Ergebnis zurückgeben
	return(char_availabe_cluster_tables)
}



psql1_get_point_cluster_tables <- function(con) {
	query <- paste0("SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_name LIKE '%cl2%'
      AND table_type = 'BASE TABLE'
      AND table_schema NOT IN ('pg_catalog', 'information_schema');
  ")
	
	char_availabe_cluster_tables <- dbGetQuery(con, query)
	return(char_availabe_cluster_tables)
}




psql1_get_network_clusters <- function(con) {
	query <- paste0("SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_name LIKE '%cl3%'
      AND table_schema NOT IN ('pg_catalog', 'information_schema');
  ")
	
	char_availabe_cluster_tables <- dbGetQuery(con, query)
	return(char_availabe_cluster_tables)
}

psql1_calc_overlay_distance <- function(con, char_sf_pol){
	query <- paste0("WITH pairs AS (
	  SELECT 
	    a.id AS i,
	    b.id AS j,
	    CASE 
	      WHEN ST_Intersects(a.pol, b.pol) THEN ST_Intersection(a.pol, b.pol)
	      ELSE NULL
	    END AS intersection_geom,
	    ST_Union(a.pol, b.pol) AS union_geom
	  FROM ", char_sf_pol, " a, ", char_sf_pol, " b
	),
	areas AS (
	  SELECT 
	    i, 
	    j,
	    COALESCE(ST_Area(intersection_geom), 0) AS intersection_area,
	    COALESCE(ST_Area(union_geom), 0) AS union_area
	  FROM pairs
	)
	SELECT 
	  i,
	  j,
	  CASE 
	    WHEN intersection_area = 0 THEN 1 -- Kein Überlappungsbereich
	    WHEN union_area > 0 THEN 1 - (intersection_area / union_area)
	    ELSE NULL
	  END AS overlay
	FROM areas;
	")
	return(dbGetQuery(con, query))
}




psql1_sample_points_on_network <- function(con, 
																					 char_network, 
																					 char_network_sampled_pts,
																					 int_dist) {
	
	query <- "DROP TABLE IF EXISTS temp_segmentized_lines"
	dbExecute(con, query)
	query <- paste0("CREATE TEMP TABLE temp_segmentized_lines AS
  SELECT 
    id AS id_edge,
    source,
    target,
    ST_Segmentize(geom_way,", int_dist, ") AS geom_way -- Segmentiere Linie in 2-Meter-Intervalle
  FROM ", char_network, ";")
	dbExecute(con, query)
	
	
	query <- "DROP TABLE IF EXISTS temp_exploded_points"
	dbExecute(con, query)
	query <- paste0("-- Temporäre Tabelle mit den Punkten (fix alle 2 Meter)
  CREATE TEMP TABLE temp_exploded_points AS
  SELECT 
    id_edge,
    source,
    target,
    (ST_DumpPoints(geom_way)).geom AS geom -- Punkte aus den segmentierten Linien extrahieren
  FROM temp_segmentized_lines;")
	dbExecute(con, query)
	
	query <- paste0("DROP TABLE IF EXISTS ", char_network_sampled_pts)
	dbExecute(con, query)
	query <- paste0("CREATE TABLE ", char_network_sampled_pts, " AS
  SELECT 
  	ROW_NUMBER() OVER () AS id,
    temp_exploded_points.id_edge,
    temp_exploded_points.source AS source_node,
    temp_exploded_points.target AS target_node,
    temp_exploded_points.geom AS geometry,
    ST_Length(", char_network, ".geom_way) * 
    ST_LineLocatePoint(", char_network, ".geom_way, temp_exploded_points.geom) AS 
    dist_to_source,
    ST_Length(", char_network, ".geom_way) * 
    (1 - ST_LineLocatePoint(", char_network, ".geom_way, temp_exploded_points.geom)) AS 
    dist_to_target
  FROM temp_exploded_points
  JOIN ", char_network," ON temp_exploded_points.id_edge = ", char_network, ".id;")
	dbExecute(con, query)
}


psql1_create_schema <- function(con, char_schema) {
  query <- paste0("CREATE SCHEMA IF NOT EXISTS ", char_schema)
  cat(query)
  dbExecute(con, query)
}


psql1_calc_distances <- function(char_schema, id_start, id_end, local_con) {
	query <- paste0("INSERT INTO ", char_schema, ".flow_distances
      (
          flow_id_i,
          flow_id_j,
          flow_manhattan_pts_euclid,
          flow_chebyshev_pts_euclid,
          flow_euclid,
          length_similarity,
  				cosine_similarity
      )
      SELECT
          a.flow_id AS flow_id_i,
          b.flow_id AS flow_id_j,
          
          -- Manhattan distance between flows based on euclidean OD points distance
          CAST(ROUND(ST_Distance(a.origin_geom, b.origin_geom)) AS INTEGER) +
          CAST(ROUND(ST_Distance(a.dest_geom, b.dest_geom)) AS INTEGER) AS flow_manhatten_pts_euclid,
  
          -- Chebyshev distance between flows based on euclidean OD points distance
          GREATEST(
        		CAST(ROUND(ST_Distance(a.origin_geom, b.origin_geom)) AS INTEGER),
        		CAST(ROUND(ST_Distance(a.dest_geom, b.dest_geom)) AS INTEGER)
          ) AS flow_chebyshev_pts_euclid,
  				
  				-- Normalized euclidean distance in the 4D space
  				--ROUND(
  				--    (SQRT(
  				--        1 * (
  				--            POW(ST_X(a.origin_geom) - ST_X(b.origin_geom), 2) +
  				--            POW(ST_Y(a.origin_geom) - ST_Y(b.origin_geom), 2)
  				--        ) +
  				--        1 * (
  				--            POW(ST_X(a.dest_geom) - ST_X(b.origin_geom), 2) +
  				--            POW(ST_Y(a.dest_geom) - ST_Y(b.dest_geom), 2)
  				--        )
  				--    ) / (a.trip_distance * b.trip_distance))::numeric, 6
  				--) AS flow_euclid_norm,
  			        
  				-- Euclidean distance in the 4D space
  				CAST(ROUND(
  	            SQRT(
  	                1 * (
  	                    POW(ST_X(a.origin_geom) - ST_X(b.origin_geom), 2) +
  	                    POW(ST_Y(a.origin_geom) - ST_Y(b.origin_geom), 2)
  	                ) +
  	                1 * (
  	                    POW(ST_X(a.dest_geom) - ST_X(b.dest_geom), 2) +
  	                    POW(ST_Y(a.dest_geom) - ST_Y(b.dest_geom), 2)
  	                )
  	            )
  	        ) AS INTEGER) AS flow_euclid,
  	        
  	      -- Length similarity
  				CAST(ROUND(ABS(a.trip_distance - b.trip_distance)) AS INTEGER) AS length_similarity,
  				-- Dot product of the vectors
          1- ((
              ((ST_X(b.origin_geom) - ST_X(b.dest_geom)) * (ST_X(a.origin_geom) - ST_X(a.dest_geom))) +
              ((ST_Y(b.origin_geom) - ST_Y(b.dest_geom)) * (ST_Y(a.origin_geom) - ST_Y(a.dest_geom)))
          ) /
  				(
                  SQRT(POW(ST_X(a.origin_geom) - ST_X(a.dest_geom), 2) + 
                  POW(ST_Y(a.origin_geom) - ST_Y(a.dest_geom), 2)) *
                  SQRT(POW(ST_X(b.origin_geom) - ST_X(b.dest_geom), 2) + 
                  POW(ST_Y(b.origin_geom) - ST_Y(b.dest_geom), 2))
          ))AS cosine_similarity
  
  				
  
      FROM ", char_schema, ".data a
      JOIN ", char_schema, ".data b
      ON a.flow_id < b.flow_id
      WHERE a.flow_id BETWEEN ", id_start, " AND ", id_end, ";")
	
	dbExecute(local_con, query)
}




psql1_norm_col <- function(con, char_schema, char_col, cores, int_min, int_max, n){


	query <- paste0("
    SELECT
        MIN(", char_col, ") AS min_val,
        MAX(", char_col, ") AS max_val
    FROM ", char_schema, ".flow_distances;")

	cat(query, "\n")
	result <- dbGetQuery(con, query)


	min_val <- result$min_val
	max_val <- result$max_val
	print(min_val)
	print(max_val)

	query <- paste0("ALTER TABLE ", char_schema, ".flow_distances DROP COLUMN
	IF EXISTS ", char_col, "_normed_", int_min, "_", int_max, ";")
	cat(query, "\n")
	dbExecute(con, query)


	query <- paste0("
    ALTER TABLE ", char_schema, ".flow_distances
    ADD COLUMN ", char_col, "_normed_", int_min, "_", int_max, " DOUBLE PRECISION;")
	cat(query, "\n")
	dbExecute(con, query)
	t1 <- proc.time()
	chunks <- r1_create_chunks(cores = cores, n = n)
	res <- parallel::mclapply(1:length(chunks), function(i) {

		local_con <- dbConnect(Postgres(),
													 dbname = dbname,
													 host = host,
													 user = user,
													 password = pw,
													 sslmode = "require")
		on.exit(dbDisconnect(local_con), add = TRUE)
		id_start <- ifelse(i == 1, 1, chunks[i - 1] + 1)
		id_end <- chunks[i]

	# 	query <- paste0("
	#     UPDATE ", char_schema, ".flow_distances
	#     SET ", char_col, "_normed_", int_min, "_", int_max, " =
	#     ", int_min, " + ((", char_col, "::DOUBLE PRECISION - ", min_val, ") /
	#     NULLIF((", max_val, "::DOUBLE PRECISION - ", min_val, "), 0)) * (", int_max, " - ", int_min, ")
	#     WHERE flow_id_i BETWEEN ", id_start, " AND ", id_end, ";")
		
		query <- paste0("
		    SELECT
		        flow_id_i,
		        flow_id_j,
		        ", int_min, " + ((", char_col, "::DOUBLE PRECISION - ", min_val, ") /
		        NULLIF((", max_val, "::DOUBLE PRECISION - ", min_val, "), 0)) * (", int_max, " - ", int_min, ") AS 
		        ", char_col, "_normed_", int_min, "_", int_max, "
		    FROM ", char_schema, ".flow_distances
		    WHERE flow_id_i BETWEEN ", id_start, " AND ", id_end, ";
		")
		cat(query)
		

		dbGetQuery(local_con, query)

	}, mc.cores = cores)
	t2 <- proc.time()
	print(t2-t1)
	return(res)
}
