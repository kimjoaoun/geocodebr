#' Match aggregated cases with left_join
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
match_aggregated_cases <- function(con, x, y, output_tb, key_cols, precision){

  # x = 'input_padrao_db'
  # y = 'filtered_cnefe_cep'
  # output_tb = 'output_caso_01'
  # key_cols <- c("estado", "municipio", "logradouro_sem_numero", "numero", "cep", "bairro")


  # Create the JOIN condition by concatenating the key columns
  join_condition <- paste(
    sprintf("%s.%s = pre_aggregated_cnefe.%s", x, key_cols, key_cols),
    collapse = " AND "
  )

  # Build the dynamic select and group statement
  cols_select <- paste0(paste(key_cols, collapse = ", "),",")
  cols_group <- paste(key_cols, collapse = ", ")

  # Build the query using the provided parameters
  query_aggregate_and_match <- sprintf(
    "CREATE TABLE %s AS
    WITH pre_aggregated_cnefe AS (
      SELECT %s AVG(lon) AS lon, AVG(lat) AS lat
      FROM %s
      GROUP BY %s
    )
    SELECT %s.ID, pre_aggregated_cnefe.lon, pre_aggregated_cnefe.lat
    FROM %s AS %s
    LEFT JOIN pre_aggregated_cnefe
    ON %s
    WHERE pre_aggregated_cnefe.lon IS NOT NULL;",
    output_tb,     # new table
    cols_select,   # select
    y,             # from
    cols_group,    # group
    x,             # select
    x, x,          # from
    join_condition # on
  )

  # parse(query_match_case)
  temp_n <- DBI::dbExecute(con, query_aggregate_and_match)

  # add precision column to output
  add_precision_col(
    con,
    update_tb = output_tb,
    precision = precision
  )

  return(temp_n)
}
