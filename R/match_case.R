#' Match cases with left_join
#'
#' @param con A db connection
#' @param x String. Name of a table written in con
#' @param y String. Name of a table written in con
#' @param output_tb Name of the new table to be written in con
#' @param key_cols Vector. Vector with the names of columns to perform left join
#' @param precision Integer. An integer
#'
#' @return Writes the result of the left join as a new table in con
#'
#' @keywords internal
match_case <- function(con, x, y, output_tb, key_cols, precision){

  # x = 'input_padrao_db'
  # y = 'filtered_cnefe_cep'
  # output_tb = 'output_caso_01'
  # key_cols <- c("estado", "municipio", "logradouro_sem_numero", "numero", "cep", "bairro")

  # Build the dynamic select statement to keep ID and key columns from `x`
  cols_select <- paste(paste0(x, ".ID"),
                       paste0("AVG(", y, ".lon) AS lon"),
                       paste0("AVG(", y, ".lat) AS lat"),
                       sep = ", ")

  # Build the dynamic group by statement
  cols_group <- paste(paste0(x, ".", c('ID', key_cols)), collapse = ", ")

  # Create dynamic ON condition for matching key columns between `x` and `y`
  match_conditions <- paste(
    paste0(x, ".", key_cols, " = ", y, ".", key_cols),
    collapse = " AND "
  )

  # Construct the SQL match query
  query_match_case <- sprintf("
  CREATE TABLE %s AS
  SELECT %s
  FROM %s
  LEFT JOIN %s
  ON %s
  GROUP BY %s.ID
  HAVING AVG(lon) IS NOT NULL;",
                              output_tb,          # Name of output table
                              cols_select,        # Columns to select (ID, lon, lat)
                              x,                  # Left table
                              y,                  # Right table
                              match_conditions,   # Dynamic matching conditions based on key columns
                              x                  # Group by ID
  )

  # parse(query_match_case)
  DBI::dbExecute(con, query_match_case)

  # # REMOVE cases not found
  # query_remove_null_lon <- sprintf("
  # DELETE FROM %s
  # WHERE lon IS NULL;",
  #                     output_tb # Name of output table
  # )
  # DBI::dbExecute(con, query_remove_null_lon)

  # add precision column to output
  add_precision_col(
    con,
    update_tb = output_tb,
    precision = precision
  )
}






#' Match cases with left_join
#'
#' @param con A db connection
#' @param x String. Name of a table written in con
#' @param y String. Name of a table written in con
#' @param output_tb Name of the new table to be written in con
#' @param key_cols Vector. Vector with the names of columns to perform left join
#' @param precision Integer. An integer
#'
#' @return Writes the result of the left join as a new table in con
#'
#' @keywords internal
exact_match_case <- function(con, x, y, output_tb, key_cols, precision){

  # x = 'input_padrao_db'
  # y = 'filtered_cnefe_cep'
  # output_tb = 'output_caso_01'
  # key_cols <- c("estado", "municipio", "logradouro_sem_numero", "numero", "cep", "bairro")

  # Build the dynamic select statement to keep ID and key columns from `x`
  cols_select <- paste(paste0(x, ".ID"),
                       paste0(y, ".lon"),
                       paste0(y, ".lat"),
                       sep = ", ")

  # Build the dynamic group by statement
  cols_group <- paste(paste0(x, ".", c('ID', key_cols)), collapse = ", ")

  # Create dynamic ON condition for matching key columns between `x` and `y`
  match_conditions <- paste(
    paste0(x, ".", key_cols, " = ", y, ".", key_cols),
    collapse = " AND "
  )

  # Construct the SQL match query
  query_match_case <- sprintf("
  CREATE TABLE %s AS
  SELECT %s
  FROM %s
  LEFT JOIN %s
  ON %s
  WHERE lon IS NOT NULL;",
                              output_tb,          # Name of output table
                              cols_select,        # Columns to select (ID, lon, lat)
                              x,                  # Left table
                              y,                  # Right table
                              match_conditions   # Dynamic matching conditions based on key columns
  )

  # parse(query_match_case)
  DBI::dbExecute(con, query_match_case)

  # # REMOVE cases not found
  # query_remove_null_lon <- sprintf("
  # DELETE FROM %s
  # WHERE lon IS NULL;",
  #                     output_tb # Name of output table
  # )
  # DBI::dbExecute(con, query_remove_null_lon)

  # add precision column to output
  add_precision_col(
    con,
    update_tb = output_tb,
    precision = precision
  )
}

