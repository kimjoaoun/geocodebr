
lookup_cases <- function(con,
                         relevant_cols,
                         case,
                         lookup_vector,
                         input_states,
                         input_municipio){

  # read correspondind parquet file
  key_cols <- gsub('_padr', '', relevant_cols)
  table_name <- paste(key_cols, collapse = "_")
  table_name <- gsub('bairro', 'localidade', table_name)
  y <- table_name <- gsub('estado_municipio', 'municipio', table_name)

  # build path to local file
  path_to_parquet <- paste0(geocodebr::get_cache_dir(), "/", table_name, ".parquet")

  # filter cnefe to include only states and municipalities
  # present in the input table, reducing the search scope and consequently
  # reducing processing time and memory usage

  # Load CNEFE data and write to DuckDB
  filtered_cnefe <- arrow::open_dataset( path_to_parquet ) |>
    dplyr::filter(estado %in% input_states) |>
    dplyr::filter(municipio %in% input_municipio) |>
    dplyr::compute()


  # register filtered_cnefe to db
  duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)


  join_condition <- paste(
    glue::glue("standard_locations.{relevant_cols} = filtered_cnefe.{lookup_vector[relevant_cols]}"),
    collapse = " AND "
  )

#  cnefe_cols <- paste(lookup_vector[relevant_cols], collapse = ", ")


  query_lookup <- glue::glue(
      "UPDATE standard_locations ",
      "SET lat = filtered_cnefe.lat,
           lon = filtered_cnefe.lon,
           match_type = '{case}',
           matched_address = filtered_cnefe.endereco_completo",
      " FROM ",
      "filtered_cnefe ",
      "WHERE standard_locations.numero_padr IS NOT NULL AND standard_locations.match_type IS NULL AND {join_condition}"
    )

    if (case %in% possible_match_types_no_number) {
      query_lookup <- gsub("standard_locations.numero_padr IS NOT NULL AND ", "", query_lookup)
    }

    n_rows_affected <- DBI::dbExecute(con, query_lookup)

    duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")

  return(n_rows_affected)
}





lookup_weighted_cases <- function(con,
                         relevant_cols,
                         case,
                         lookup_vector,
                         input_states,
                         input_municipio){


  # read correspondind parquet file
  key_cols <- gsub('_padr', '', relevant_cols)
  table_name <- paste(key_cols, collapse = "_")
  table_name <- gsub('bairro', 'localidade', table_name)
  table_name <- gsub('logradouro', 'logradouro_numero', table_name)
  y <- table_name <- gsub('estado_municipio', 'municipio', table_name)

  # build path to local file
  path_to_parquet <- paste0(geocodebr::get_cache_dir(), "/", table_name, ".parquet")

  # filter cnefe to include only states and municipalities
  # present in the input table, reducing the search scope and consequently
  # reducing processing time and memory usage

  # Load CNEFE data and write to DuckDB
  filtered_cnefe <- arrow::open_dataset( path_to_parquet ) |>
    dplyr::filter(estado %in% input_states) |>
    dplyr::filter(municipio %in% input_municipio) |>
    dplyr::compute()

  # register filtered_cnefe to db
  duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)


  # first left join
  join_condition <- paste(
    glue::glue("standard_locations.{relevant_cols} = filtered_cnefe.{lookup_vector[relevant_cols]}"),
    collapse = " AND "
  )

  # Construct the SQL match query
  query_match <- glue::glue(
    "CREATE OR REPLACE TEMPORARY TABLE temp_join AS
      SELECT standard_locations.tempidgeocodebr, standard_locations.numero_padr,
             filtered_cnefe.numero AS numero_db, filtered_cnefe.lat,
             filtered_cnefe.lon, filtered_cnefe.endereco_completo
      FROM standard_locations
      LEFT JOIN filtered_cnefe
      ON {join_condition}
      WHERE standard_locations.numero_padr IS NOT NULL AND standard_locations.match_type IS NULL AND filtered_cnefe.numero IS NOT NULL;"
  )

  DBI::dbExecute(con, query_match)


  # summarize
  query_aggregate <- glue::glue(
    "CREATE OR REPLACE TEMPORARY TABLE tempdb AS
    SELECT tempidgeocodebr,
    SUM((1/ABS(numero_padr - numero_db) * lat)) / SUM(1/ABS(numero_padr - numero_db)) AS lat,
    SUM((1/ABS(numero_padr - numero_db) * lon)) / SUM(1/ABS(numero_padr - numero_db)) AS lon,
    REGEXP_REPLACE(FIRST(endereco_completo), ', \\d+ -', CONCAT(', ', FIRST(numero_padr), ' (aprox) -')) AS matched_address
    FROM temp_join
    GROUP BY tempidgeocodebr;"
  )
  temp_n <- DBI::dbExecute(con, query_aggregate)


  # update output
  query_lookup <- glue::glue(
    "UPDATE standard_locations ",
    "SET lat = tempdb.lat,
         lon = tempdb.lon,
         match_type = '{case}',
         matched_address = tempdb.matched_address",
    " FROM ",
    "tempdb ",
    "WHERE standard_locations.match_type IS NULL AND standard_locations.tempidgeocodebr = tempdb.tempidgeocodebr"
  )


  n_rows_affected <- DBI::dbExecute(con, query_lookup)

  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")

  return(n_rows_affected)

  }
