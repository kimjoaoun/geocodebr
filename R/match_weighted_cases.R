#' Match aggregated cases with left_join
#'
#' @param con A db connection
#' @param x String. Name of a table written in con
#' @param y String. Name of a table written in con
#' @param output_tb Name of the new table to be written in con
#' @param key_cols Vector. Vector with the names of columns to perform left join
#' @param match_type Integer. An integer
#'
#' @return Writes the result of the left join as a new table in con
#'
#' @keywords internal
match_weighted_cases <- function(con, x, y, output_tb, key_cols, match_type){

  # Create the JOIN condition by concatenating the key columns
  join_condition <- paste(
    glue::glue("{y}.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )


#   # Build the dynamic select and group statement
#   cols_select <- c('tempidgeocodebr', key_cols, 'numero')
#   cols_select <- paste( glue::glue("{x}.{cols_select}"), collapse = ', ')
#   cols_group <- paste(c('tempidgeocodebr', key_cols, 'numero'), collapse = ", ")

  # Construct the SQL match query
  query_match <- glue::glue(
    "CREATE OR REPLACE TEMPORARY VIEW temp_db AS
      SELECT {x}.tempidgeocodebr, {x}.numero, {y}.numero as numero_db, {y}.lat, {y}.lon
      FROM {x}
      LEFT JOIN {y}
      ON {join_condition}
      WHERE {x}.numero IS NOT NULL AND {y}.numero IS NOT NULL;"
  )
  DBI::dbExecute(con, query_match)


  # summarize
  query_aggregate <- glue::glue(
    "CREATE TEMPORARY TABLE {output_tb} AS
    SELECT tempidgeocodebr,
    SUM((1/ABS(numero - numero_db) * lon)) / SUM(1/ABS(numero - numero_db)) AS lon,
    SUM((1/ABS(numero - numero_db) * lat)) / SUM(1/ABS(numero - numero_db)) AS lat,
    {match_type} as match_type
    FROM temp_db
    GROUP BY tempidgeocodebr;"
    )
  temp_n <- DBI::dbExecute(con, query_aggregate)

  # UPDATE input_padrao_db: Remove observations found in previous step
  update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )

  return(temp_n)
}


#' Match aggregated cases with left_join
#'
#' @param con A db connection
#' @param x String. Name of a table written in con
#' @param y String. Name of a table written in con
#' @param output_tb Name of the new table to be written in con
#' @param key_cols Vector. Vector with the names of columns to perform left join
#' @param match_type Integer. An integer
#' @param input_states Vector. Passed from above
#' @param input_municipio Vector. Passed from above
#'
#' @return Writes the result of the left join as a new table in con
#'
#' @keywords internal
match_weighted_cases_arrow <- function(con,
                                       x,
                                       y,
                                       output_tb,
                                       key_cols,
                                       match_type,
                                       input_states,
                                       input_municipio){

  # read correspondind parquet file
  table_name <- paste(c(key_cols, 'numero'), collapse = "_")
  table_name <- gsub('estado_municipio_logradouro_sem_numero', 'logradouro', table_name)
  y <- table_name

  path_to_parquet <- paste0("C:/Users/r1701707/AppData/Local/R/cache/R/geocodebr/test_db/", table_name, '.parquet')
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


  # Construct the SQL match query
  query_match <- glue::glue(
    "CREATE OR REPLACE TEMPORARY VIEW temp_db AS
      SELECT {x}.tempidgeocodebr, {x}.numero, {y}.numero as numero_db, {y}.lat, {y}.lon
      FROM {x}
      LEFT JOIN {y}
      ON {join_condition}
      WHERE {x}.numero IS NOT NULL AND {y}.numero IS NOT NULL;"
  )
  DBI::dbExecute(con, query_match)


  # summarize
  query_aggregate <- glue::glue(
    "CREATE TEMPORARY TABLE {output_tb} AS
    SELECT tempidgeocodebr,
    SUM((1/ABS(numero - numero_db) * lon)) / SUM(1/ABS(numero - numero_db)) AS lon,
    SUM((1/ABS(numero - numero_db) * lat)) / SUM(1/ABS(numero - numero_db)) AS lat,
    {match_type} as match_type
    FROM temp_db
    GROUP BY tempidgeocodebr;"
  )

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

