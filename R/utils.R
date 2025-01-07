#' Safely use arrow to open a Parquet file
#'
#' This function handles some failure modes, including if the Parquet file is
#' corrupted.
#'
#' @param filename A local Parquet file
#' @return An `arrow::Dataset`
#'
#' @keywords internal
arrow_open_dataset <- function(filename){

  tryCatch(
    arrow::open_dataset(filename),
    error = function(e){
      msg <- paste(
        "File cached locally seems to be corrupted. Please download it again using 'cache = FALSE'.",
        sprintf("Alternatively, you can remove the corrupted file with 'geocodebr::geocodebr_cache(delete_file = \"%s\")'", basename(filename)),
        sep = "\n"
      )
      stop(msg)
    }
  )
}

#' Message when caching file
#'
#' @param local_file The address of a file passed from the download_file function.
#' @param cache Logical. Whether the cached data should be used.

#' @return A message
#'
#' @keywords internal
cache_message <- function(local_file = parent.frame()$local_file,
                          cache = parent.frame()$cache){

#  local_file <- 'C:\\Users\\user\\AppData\\Local/R/cache/R/geocodebr_v0.1/2010_deaths.parquet'

  # name of local file
  file_name <- basename(local_file[1])
  dir_name <- dirname(local_file[1])

  ## if file already exists
    # YES cache
    if (file.exists(local_file) & isTRUE(cache)) {
       message('Reading data cached locally.')
       }

    # NO cache
    if (file.exists(local_file) & isFALSE(cache)) {
       message('Overwriting data cached locally.')
       }

  ## if file does not exist yet
  # YES cache
  if (!file.exists(local_file) & isTRUE(cache)) {
     message(paste("Downloading data and storing it locally for future use."))
     }

  # NO cache
  if (!file.exists(local_file) & isFALSE(cache)) {
     message(paste("Downloading data. Setting 'cache = TRUE' is strongly recommended to speed up future use. File will be stored locally at:", dir_name))
     }
  }




#' Add a column of state abbreviations
#'
#' @param con A db connection
#' @param update_tb String. Name of a table to be updated in con
#'
#' @return Adds a new column to a table in con
#'
#' @keywords internal
add_abbrev_state_col <- function(con, update_tb = "input_padrao_db"){
  # Assuming `con` is your DuckDB connection and `input_padrao` already exists as a table

  # # Step 1: Add a new empty column to the existing table
  # DBI::dbExecute(con, sprintf("ALTER TABLE %s ADD COLUMN abbrev_state VARCHAR", update_tb))

  # Step 2: Update the new column with the state abbreviations using CASE WHEN
  query_update <- sprintf("
  UPDATE %s
  SET estado = CASE
    WHEN estado = 'RONDONIA' THEN 'RO'
    WHEN estado = 'ACRE' THEN 'AC'
    WHEN estado = 'AMAZONAS' THEN 'AM'
    WHEN estado = 'RORAIMA' THEN 'RR'
    WHEN estado = 'PARA' THEN 'PA'
    WHEN estado = 'AMAPA' THEN 'AP'
    WHEN estado = 'TOCANTINS' THEN 'TO'
    WHEN estado = 'MARANHAO' THEN 'MA'
    WHEN estado = 'PIAUI' THEN 'PI'
    WHEN estado = 'CEARA' THEN 'CE'
    WHEN estado = 'RIO GRANDE DO NORTE' THEN 'RN'
    WHEN estado = 'PARAIBA' THEN 'PB'
    WHEN estado = 'PERNAMBUCO' THEN 'PE'
    WHEN estado = 'ALAGOAS' THEN 'AL'
    WHEN estado = 'SERGIPE' THEN 'SE'
    WHEN estado = 'BAHIA' THEN 'BA'
    WHEN estado = 'MINAS GERAIS' THEN 'MG'
    WHEN estado = 'ESPIRITO SANTO' THEN 'ES'
    WHEN estado = 'RIO DE JANEIRO' THEN 'RJ'
    WHEN estado = 'SAO PAULO' THEN 'SP'
    WHEN estado = 'PARANA' THEN 'PR'
    WHEN estado = 'SANTA CATARINA' THEN 'SC'
    WHEN estado = 'RIO GRANDE DO SUL' THEN 'RS'
    WHEN estado = 'MATO GROSSO DO SUL' THEN 'MS'
    WHEN estado = 'MATO GROSSO' THEN 'MT'
    WHEN estado = 'GOIAS' THEN 'GO'
    WHEN estado = 'DISTRITO FEDERAL' THEN 'DF'
    ELSE NULL
  END",
    update_tb)

  # Execute the update query
  DBI::dbExecute(con, query_update)
}


#' Update input_padrao_db to remove observations previously matched
#'
#' @param con A db connection
#' @param update_tb String. Name of a table to be updated in con
#' @param reference_tb A table written in con used as reference
#'
#' @return Drops observations from input_padrao_db
#'
#' @keywords internal
update_input_db <- function(con, update_tb = 'input_padrao_db', reference_tb){

  # 6666666666 possible improvment
  # the filter could be done in the query join
  # without the need to update the table, which requires more io operations

  # update_tb = 'input_padrao_db'
  # reference_tb = 'output_caso_1'

  query_remove_matched <- sprintf("
    DELETE FROM %s
    WHERE tempidgeocodebr IN (SELECT tempidgeocodebr FROM %s)", update_tb, reference_tb)
  DBI::dbExecute(con, query_remove_matched)
}


#' Add a column with info of geocode match_type
#'
#' @param con A db connection
#' @param update_tb String. Name of a table to be updated in con
#' @param match_type Integer. An integer
#'
#' @return Adds a new column to a table in con
#'
#' @keywords internal
add_precision_col <- function(con, update_tb = NULL, match_type = NULL){

  # update_tb = "output_caso_01"
  # match_type = 1L

  DBI::dbExecute(
    con,
    glue::glue(
      "ALTER TABLE {update_tb} ADD COLUMN match_type INTEGER DEFAULT {match_type}"
      )
    )
}


merge_results <- function(con, x, y, key_column, select_columns){

  # x = 'output_db'
  # y = 'output_caso_01'
  # key_column = 'tempidgeocodebr'
  select_columns_y = c('lon', 'lat', 'match_type')

  # drop temp id column
  select_columns <- select_columns[select_columns!='tempidgeocodebr']

  # Create the SELECT clause dynamically
  # select_x <- paste0(x, '.', c('lon', 'lat', 'match_type '), collapse = ', ')
  select_x <- paste0(x, '.', c(select_columns), collapse = ', ')

  select_clause <- paste0(
    select_x, ',',
    paste0(y, ".", select_columns_y, collapse = ", ")
    )

  # Create the SQL query
  query <- sprintf("
    SELECT %s
    FROM %s
    LEFT JOIN %s
    ON %s.%s = %s.%s",
                   select_clause, # Selected columns
                   x,             # Left table
                   y,             # Right table
                   x, key_column, # Left table and key column
                   y, key_column  # Right table and key column
  )


  # Execute the query and fetch the merged data
  merged_data <- DBI::dbGetQuery(con, query)
  return(merged_data)
  }




#' create index
#'
#' @keywords internal
create_index <- function(con, tb, cols, operation, overwrite=TRUE){

  idx <- paste0('idx_', tb)
  cols_group <- paste(cols, collapse = ", ")

  # check if table already has index
  i <- DBI::dbGetQuery(
    con,
    sprintf("SELECT * FROM duckdb_indexes WHERE table_name = '%s';", tb)
  )

  if (nrow(i) > 0 & isFALSE(overwrite)) { return(NULL) }
  if (nrow(i) > 0 & isTRUE(overwrite)) {
    DBI::dbExecute(con, sprintf('DROP INDEX IF EXISTS %s',idx))
  }

  query_index <- sprintf('%s INDEX %s ON %s(%s);', operation, idx, tb, cols_group)
  DBI::dbExecute(con, query_index)
}


get_relevant_cols_rafa <- function(case) {
  relevant_cols <- if (case == 1) {
    c("estado", "municipio", "logradouro_sem_numero", "numero", "cep", "localidade")
  } else if (case == 2) {
    c("estado", "municipio", "logradouro_sem_numero", "numero", "cep")
  } else if (case == 3) {
    c("estado", "municipio", "logradouro_sem_numero", "numero", "localidade")
  } else if (case == 4) {
    c("estado", "municipio", "logradouro_sem_numero", "numero")
  } else if (case == 44) {
    c("estado", "municipio", "logradouro_sem_numero")
  } else if (case == 5) {
    c("estado", "municipio", "logradouro_sem_numero", "cep", "localidade")
  } else if (case == 6) {
    c("estado", "municipio", "logradouro_sem_numero", "cep")
  } else if (case == 7) {
    c("estado", "municipio", "logradouro_sem_numero", "localidade")
  } else if (case == 8) {
    c("estado", "municipio", "logradouro_sem_numero")
  } else if (case == 9) {
    c("estado", "municipio", "cep", "localidade")
  } else if (case == 10) {
    c("estado", "municipio", "cep")
  } else if (case == 11) {
    c("estado", "municipio", "localidade")
  } else if (case == 12) {
    c("estado", "municipio")
  }

  return(relevant_cols)
}
