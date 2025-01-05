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
match_aggregated_cases_weighted <- function(con, x, y, output_tb, key_cols, match_type){

  # x = 'input_padrao_db'
  # y = 'filtered_cnefe'
  # output_tb = 'output_caso_44'
  # key_cols <- c("estado", "municipio", "logradouro_sem_numero")
  # match_type = 44L

  # TEMPORARY FIX
  # convert numero to numeric
  DBI::dbExecute(
    con,
    glue::glue("UPDATE {x} SET numero = TRY_CAST(numero AS INTEGER);
               ALTER TABLE {x} ALTER COLUMN numero TYPE INTEGER USING TRY_CAST(numero AS INTEGER);")
  )

  # this is necessary to use match with weighted cases
  # filtered_cnefe <- dplyr::collect(filtered_cnefe)
  # duckdb::dbWriteTable(con, "filtered_cnefe", filtered_cnefe,
  #                      temporary = TRUE, overwrite = TRUE)

  DBI::dbExecute(
    con,
    glue::glue("UPDATE {y} SET numero = TRY_CAST(numero AS INTEGER);
               ALTER TABLE {y} ALTER COLUMN numero TYPE INTEGER USING TRY_CAST(numero AS INTEGER);")
  )

  # Create the JOIN condition by concatenating the key columns
  join_condition <- paste(
    glue::glue("{y}.{key_cols} = {x}.{key_cols}"),
    collapse = ' AND '
  )


  # Build the dynamic select and group statement
  cols_select <- c('id', key_cols, 'numero')
  cols_select <- paste( glue::glue("{x}.{cols_select}"), collapse = ', ')
  cols_group <- paste(c('id', key_cols, 'numero'), collapse = ", ")

  # Construct the SQL match query
  query_match <- glue::glue(
    "CREATE OR REPLACE TEMPORARY VIEW temp_db AS
      SELECT {x}.id, {x}.numero, {y}.numero as numero_1, {y}.lat, {y}.lon
      FROM {x}
      LEFT JOIN {y}
      ON {join_condition}
      WHERE {x}.numero IS NOT NULL AND {y}.numero IS NOT NULL;"
  )
  temp_n <- DBI::dbExecute(con, query_match)


  # summarize
  query_aggregate <- glue::glue(
    "CREATE TEMPORARY TABLE {output_tb} AS
    SELECT id,
    SUM((1/ABS(numero - numero_1) * lon)) / SUM(1/ABS(numero - numero_1)) AS lon,
    SUM((1/ABS(numero - numero_1) * lat)) / SUM(1/ABS(numero - numero_1)) AS lat
    FROM temp_db
    GROUP BY id;"
    )
  temp_n <- DBI::dbExecute(con, query_aggregate)


  # add match_type column to output
  add_precision_col(
    con,
    update_tb = output_tb,
    match_type = match_type
  )


  # UPDATE input_padrao_db: Remove observations found in previous step
  update_input_db(
    con,
    update_tb = x,
    reference_tb = output_tb
  )

  return(temp_n)
}

