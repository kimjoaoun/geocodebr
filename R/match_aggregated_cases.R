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

  # x = 'input_padrao_db'
  # y = 'filtered_cnefe'
  # output_tb = 'output_caso_01'
  # key_cols <- c("estado", "municipio", "logradouro_sem_numero", "numero", "cep", "localidade")
  # match_type = 1L


  # Create the JOIN condition by concatenating the key columns
  join_condition <- paste(
    glue::glue("pre_aggregated_cnefe.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )

  # Build the dynamic select and group statement
  cols_select <- paste0(paste(key_cols, collapse = ", "),",")
  cols_group <- paste(key_cols, collapse = ", ")

  # pre-aggregate cnefe
  query_aggregate <- glue::glue(
    "CREATE OR REPLACE VIEW pre_aggregated_cnefe AS
        SELECT {cols_select} AVG(lon) AS lon, AVG(lat) AS lat
        FROM {y}
        GROUP BY {cols_group};"
    )

  temp_n <- DBI::dbExecute(con, query_aggregate)

  # Build the query using the provided parameters
  query_match <- glue::glue(
    "CREATE TEMPORARY TABLE {output_tb} AS
      SELECT {x}.id, pre_aggregated_cnefe.lon, pre_aggregated_cnefe.lat
      FROM {x}
      LEFT JOIN pre_aggregated_cnefe
      ON {join_condition}
      WHERE pre_aggregated_cnefe.lon IS NOT NULL;"
  )

  # parse(query_match_case)
  temp_n <- DBI::dbExecute(con, query_match)


  # add match_type column to output
  add_precision_col(
    con,
    update_tb = output_tb,
    match_type = match_type
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
match_aggregated_cases_like <- function(con, x, y, output_tb, key_cols, match_type){

  # x = 'input_padrao_db'
  # y = 'filtered_cnefe'
  # output_tb = 'output_caso_01'
  # key_cols <- c("estado", "municipio", "logradouro_sem_numero", "numero", "cep", "localidade")
  # match_type = 1L


  # Create the JOIN condition by concatenating the key columns
  join_condition <- paste(
    glue::glue("pre_aggregated_cnefe.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )

  join_condition <- gsub("= input_padrao_db.logradouro_sem_numero", "LIKE '%' || input_padrao_db.logradouro_sem_numero || '%'", join_condition)

  # Build the dynamic select and group statement
  cols_select <- paste0(paste(key_cols, collapse = ", "),",")
  cols_group <- paste(key_cols, collapse = ", ")

  # pre-aggregate cnefe
  query_aggregate <- glue::glue(
    "CREATE OR REPLACE VIEW pre_aggregated_cnefe AS
        SELECT {cols_select} AVG(lon) AS lon, AVG(lat) AS lat
        FROM {y}
        GROUP BY {cols_group};"
  )
  temp_n <- DBI::dbExecute(con, query_aggregate)

  # Build the query using the provided parameters
  query_match <- glue::glue(
    "CREATE TEMPORARY TABLE {output_tb} AS
      SELECT {x}.id, pre_aggregated_cnefe.lon, pre_aggregated_cnefe.lat
      FROM {x}
      LEFT JOIN pre_aggregated_cnefe
      ON {join_condition}
      WHERE pre_aggregated_cnefe.lon IS NOT NULL;"
  )

  # parse(query_match_case)
  temp_n <- DBI::dbExecute(con, query_match)

  # add match_type column to output
  add_precision_col(
    con,
    update_tb = output_tb,
    match_type = match_type
  )

  return(temp_n)
}
