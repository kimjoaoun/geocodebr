match_weighted_cases <- function(con,
                                 x,
                                 y,
                                 output_tb,
                                 key_cols,
                                 match_type,
                                 full_results
                                 ){


  # read correspondind parquet file
  table_name <- paste(key_cols, collapse = "_")
  table_name <- gsub('estado_municipio', 'municipio', table_name)
  table_name <- gsub('logradouro_sem_numero', 'logradouro_numero', table_name)
  y <- table_name

  # build path to local file
  path_to_parquet <- paste0(geocodebr::get_cache_dir(), "/", table_name, ".parquet")

  # determine geographical scope of the search
  input_states <- DBI::dbGetQuery(con, "SELECT DISTINCT estado FROM input_padrao_db;")$estado
  input_municipio <- DBI::dbGetQuery(con, "SELECT DISTINCT municipio FROM input_padrao_db;")$municipio

  # Load CNEFE data and write to DuckDB
  # filter cnefe to include only states and municipalities
  # present in the input table, reducing the search scope
  filtered_cnefe <- arrow::open_dataset( path_to_parquet ) |>
    dplyr::filter(estado %in% input_states) |>
    dplyr::filter(municipio %in% input_municipio) |>
    dplyr::compute()

  # register filtered_cnefe to db
  duckdb::duckdb_register_arrow(con, "filtered_cnefe", filtered_cnefe)

  y <- 'filtered_cnefe'

  # Create the JOIN condition by concatenating the key columns
  join_condition <- paste(
    glue::glue("{y}.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )

  cols_not_null <-  paste(
    glue::glue("{x}.{key_cols} IS NOT NULL"),
    collapse = ' AND '
  )


  # Construct the SQL match query
  query_match <- glue::glue(
    "CREATE OR REPLACE TEMPORARY VIEW temp_db AS",
      " SELECT {x}.tempidgeocodebr, {x}.numero, {y}.numero AS numero_db,
               {y}.lat, {y}.lon, {y}.endereco_completo",
      " FROM {x}",
      " LEFT JOIN {y}",
      " ON {join_condition}",
      " WHERE {cols_not_null} AND {x}.numero IS NOT NULL AND {y}.numero IS NOT NULL;"
  )

  # whether to keep all columns in the result
  if (isFALSE(full_results)) {
    query_match <- gsub(", filtered_cnefe.endereco_completo", "", query_match)
  }

  DBI::dbExecute(con, query_match)


  # summarize
  query_aggregate <- glue::glue(
    "CREATE TEMPORARY TABLE {output_tb} AS
    SELECT tempidgeocodebr,
    SUM((1/ABS(numero - numero_db) * lon)) / SUM(1/ABS(numero - numero_db)) AS lon,
    SUM((1/ABS(numero - numero_db) * lat)) / SUM(1/ABS(numero - numero_db)) AS lat,
    '{match_type}' AS match_type,
     REGEXP_REPLACE(FIRST(endereco_completo), ', \\d+ -', CONCAT(', ', FIRST(numero), ' (aprox) -')) AS matched_address
    FROM temp_db
    GROUP BY tempidgeocodebr;"
  )

  if (isFALSE(full_results)) {
      query_aggregate <- glue::glue(
        "CREATE TEMPORARY TABLE {output_tb} AS
        SELECT tempidgeocodebr,
        SUM((1/ABS(numero - numero_db) * lon)) / SUM(1/ABS(numero - numero_db)) AS lon,
        SUM((1/ABS(numero - numero_db) * lat)) / SUM(1/ABS(numero - numero_db)) AS lat,
        '{match_type}' AS match_type
        FROM temp_db
        GROUP BY tempidgeocodebr;"
        )
    }

  temp_n <- DBI::dbExecute(con, query_aggregate)

  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")


  # UPDATE input_padrao_db: Remove observations found in previous step
  update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )



  return(temp_n)
}

