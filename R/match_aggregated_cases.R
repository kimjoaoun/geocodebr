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
match_aggregated_cases <- function(con, x, y, output_tb, key_cols, match_type){

  # table table - 7.993404
  # view table--- 7.047993
  # table view -- 8.874689
  # view view --- 7.780372

  # Build the dynamic select and group statement
  cols_select <- paste0(paste(key_cols, collapse = ", "),",")
  cols_group <- paste(key_cols, collapse = ", ")

  # pre-aggregate cnefe
  query_aggregate <- glue::glue(
    "CREATE OR REPLACE TEMPORARY VIEW pre_aggregated_cnefe AS
        SELECT {cols_select} AVG(lon) AS lon, AVG(lat) AS lat
        FROM {y}
        WHERE {y}.numero != 'S/N'
        GROUP BY {cols_group};"
      )

  if (match_type %in% 5:12) {
    query_aggregate <- gsub("WHERE filtered_cnefe.numero != 'S/N'", "", query_aggregate)
  }

  DBI::dbExecute(con, query_aggregate)



  # Create the JOIN condition by concatenating the key columns
  join_condition <- paste(
    glue::glue("pre_aggregated_cnefe.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )

  # LIKE pulo do gato aqui 6666666666666
  # join_condition <- gsub("= input_padrao_db.logradouro_sem_numero", "LIKE '%' || input_padrao_db.logradouro_sem_numero || '%'", join_condition)


  # query for left join
  query_match <- glue::glue(
    "CREATE TEMPORARY TABLE {output_tb} AS
      SELECT {x}.id, pre_aggregated_cnefe.lon, pre_aggregated_cnefe.lat, {match_type} as match_type
      FROM {x}
      LEFT JOIN pre_aggregated_cnefe
      ON {join_condition}
      WHERE {x}.numero != 'S/N' AND pre_aggregated_cnefe.lon IS NOT NULL;"
    )

  if (match_type %in% 5:12) {
    query_match <- gsub("input_padrao_db.numero != 'S/N' AND", "", query_match)
    }

  temp_n <- DBI::dbExecute(con, query_match)


  # # add match_type column to output
  # add_precision_col(
  #   con,
  #   update_tb = output_tb,
  #   match_type = match_type
  # )

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
#'
#' @return Writes the result of the left join as a new table in con
#'
#' @keywords internal
match_aggregated_cases_arrow <- function(con,
                                          x,
                                          y,
                                          output_tb,
                                          key_cols,
                                          match_type,
                                          input_states,
                                          input_municipio
                                          ){

  table_name <- paste(key_cols, collapse = "_")
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


  # Create the JOIN condition by concatenating the key columns
  join_condition <- paste(
    glue::glue("filtered_cnefe.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )

  # pulo do gato aqui 6666666666666
  # join_condition <- gsub("= input_padrao_db.logradouro_sem_numero", "LIKE '%' || input_padrao_db.logradouro_sem_numero || '%'", join_condition)

  # Build the dynamic select and group statement
  cols_select <- paste0(paste(key_cols, collapse = ", "),",")
  cols_group <- paste(key_cols, collapse = ", ")


  # query for left join
  query_match <- glue::glue(
    "CREATE TEMPORARY TABLE {output_tb} AS
      SELECT {x}.id, filtered_cnefe.lon, filtered_cnefe.lat, {match_type} as match_type
      FROM {x}
      LEFT JOIN filtered_cnefe
      ON {join_condition}
      WHERE {x}.numero IS NOT NULL AND filtered_cnefe.lon IS NOT NULL;"
  )

  if (match_type %in% 5:12) {
    query_match <- gsub("input_padrao_db.numero IS NOT NULL AND", "", query_match)
  }

  temp_n <- DBI::dbExecute(con, query_match)

  # # add match_type column to output
  # add_precision_col(
  #   con,
  #   update_tb = output_tb,
  #   match_type = match_type
  # )

  # UPDATE input_padrao_db: Remove observations found in previous step
  update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )

  duckdb::duckdb_unregister_arrow(con, "filtered_cnefe")

  return(temp_n)
}
