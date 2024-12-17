
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
match_case_single_string <- function(con, x, y, output_tb, key_cols, precision){

  # x = 'input_padrao_db'
  # y = 'filtered_cnefe_cep'
  # output_tb = 'output_caso_01'
  # key_cols <- c("estado", "municipio", "logradouro", "numero", "cep", "bairro")


  # drop_conact_col <- function(tb){
  #
  #   query_colnames <- sprintf("SELECT column_name
  #         FROM information_schema.columns
  #         WHERE table_name = '%s'", tb)
  #
  #   column_names <- DBI::dbGetQuery(con, query_colnames)
  #   col_exists <- "concat_key" %in% column_names$column_name
  #
  #   if(col_exists){
  #     query_drop_column <- paste0("ALTER TABLE ", tb, " DROP COLUMN concat_key;")
  #     DBI::dbExecute(con, query_drop_column)
  #   }
  # }
  #
  # drop_conact_col(x)
  # drop_conact_col(y)

  query_add_txt_column <- paste0(
    "ALTER TABLE ", x, " ADD COLUMN concat_key TEXT;"
  )
  DBI::dbExecute(con, query_add_txt_column)

  query_add_txt_column <- paste0(
    "ALTER TABLE ", y, " ADD COLUMN concat_key TEXT;"
  )
  DBI::dbExecute(con, query_add_txt_column)

    # Create a new column in both tables that concatenates the key columns
  fun <- function(tb){
    query_create_key_x <- paste0(
      "UPDATE ", tb, " SET concat_key = CONCAT_WS('_',",
      paste(key_cols, collapse = ', '),");")
    return(query_create_key_x)
    }

  DBI::dbExecute(con, fun(x), overwrite=TRUE)
  DBI::dbExecute(con, fun(y), overwrite=TRUE)


    # Build the dynamic select statement to keep ID and key columns from `x`
    cols_select <- paste(paste0(x, ".ID"),
                         paste0("AVG(", y, ".lon) AS lon"),
                         paste0("AVG(", y, ".lat) AS lat"),
                         sep = ", ")

    # Create the ON condition for matching the concatenated key columns
    match_conditions <- paste(
      paste0(x, ".concat_key", " = ", y, ".concat_key"))

    # Construct the SQL query for the match case
    query_match_case <- sprintf("
  CREATE TEMPORARY TABLE %s AS
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
                                x                   # Group by ID
    )


    # parse(query_match_case) 3.09 sec elapsed
    DBI::dbExecute(con, query_match_case)

    # add precision column to output
    add_precision_col(
      con,
      update_tb = output_tb,
      precision = precision
    )

}

