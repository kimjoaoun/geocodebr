#' Match aggregated cases with left_join
#'
#' @param con A db connection
#' @param x String. Name of a table written in con
#' @param y String. Name of a table written in con
#' @param output_tb Name of the new table to be written in con
#' @param key_cols Vector. Vector with the names of columns to perform left join
#' @param match_type Character.
#' @param input_states Vector. Passed from above
#' @param input_municipio Vector. Passed from above
#'
#' @return Writes the result of the left join as a new table in con
#'
#' @keywords internal
match_weighted_cases <- function(con,
                                 x,
                                 y,
                                 output_tb,
                                 key_cols,
                                 match_type,
                                 keep_matched_address,
                                 input_states,
                                 input_municipio){


  # read correspondind parquet file
  table_name <- paste(key_cols, collapse = "_")
  table_name <- gsub('estado_municipio', 'municipio', table_name)
  table_name <- gsub('logradouro_sem_numero', 'logradouro_numero', table_name)
  y <- table_name

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

  y <- 'filtered_cnefe'

  # Create the JOIN condition by concatenating the key columns
  join_condition <- paste(
    glue::glue("{y}.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )

  # # TO DO: match probabilistico
  # # isso eh um teste provisorio
  # if( match_type %in% probabilistic_logradouro_match_types) {
  #   join_condition <- gsub("= input_padrao_db.logradouro_sem_numero", "LIKE '%' || input_padrao_db.logradouro_sem_numero || '%'", join_condition)
  #   }

  # Construct the SQL match query
  query_match <- glue::glue(
    "CREATE OR REPLACE TEMPORARY VIEW temp_db AS
      SELECT {x}.tempidgeocodebr, {x}.numero, {y}.numero as numero_db, {y}.lat, {y}.lon, {y}.endereco_completo
      FROM {x}
      LEFT JOIN {y}
      ON {join_condition}
      WHERE {x}.numero IS NOT NULL AND {y}.numero IS NOT NULL;"
  )

  if (isFALSE(keep_matched_address)) {
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

  if (isFALSE(keep_matched_address)) {
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

