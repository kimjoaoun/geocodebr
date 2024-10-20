#' Download file from url
#'
#' @param file_url String. A url.
#' @param showProgress Logical.
#' @param cache Logical.

#' @return A string to the address of the file
#'
#' @keywords internal
download_file <- function(file_url = parent.frame()$file_url,
                          showProgress = parent.frame()$showProgress,
                          cache = parent.frame()$cache){ # nocov start

  # check input
  checkmate::assert_logical(showProgress)
  checkmate::assert_logical(cache)

  # name of local file
  file_name <- basename(file_url)

  # create local dir
  if (isTRUE(cache) & !dir.exists(geocodebr_env$cache_dir)) { dir.create(geocodebr_env$cache_dir, recursive=TRUE) }

  # path to local file
  local_file <- fs::path(geocodebr_env$cache_dir,"/",file_name)

  # # cache message
  # cache_message(local_file, cache)

  # this is necessary to silence download message when reading local file
  if (all(file.exists(local_file)) & isTRUE(cache)){
    showProgress <- FALSE
  }

  # download files
  try(silent = TRUE,
        downloaded_files <- curl::multi_download(
          urls = file_url,
          destfiles = local_file,
          progress = showProgress,
          resume = cache
        )
      )

  # if anything fails, return NULL (fail gracefully)
  if (any(!downloaded_files$success | is.na(downloaded_files$success))) {
        msg <- paste(
        "File cached locally seems to be corrupted. Please download it again using 'cache = FALSE'. Alternatively, you can remove the corrupted file with 'geocodebr::geocodebr_cache(delete_file = file_name)")
        message(msg)
        return(invisible(NULL))
        }


  return(TRUE)
  } # nocov end


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
                          cache = parent.frame()$cache){ # nocov start

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
  } # nocov end




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

  # Step 1: Add a new empty column to the existing table
  DBI::dbExecute(con, sprintf("ALTER TABLE %s ADD COLUMN abbrev_state VARCHAR", update_tb))

  # Step 2: Update the new column with the state abbreviations using CASE WHEN
  query_update <- sprintf("
  UPDATE %s
  SET abbrev_state = CASE
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
  # withou the need to update the table, which requires more io operations

  # update_tb = 'input_padrao_db'
  # reference_tb = 'output_caso_1'

  query_remove_matched <- sprintf("
    DELETE FROM %s
    WHERE ID IN (SELECT ID FROM %s)", update_tb, reference_tb)
  DBI::dbExecute(con, query_remove_matched)
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
match_case <- function(con, x, y, output_tb, key_cols, precision){

  # x = 'input_padrao_db'
  # y = 'filtered_cnefe_cep'
  # output_tb = 'output_caso_01'
  # key_cols <- c("estado", "municipio", "logradouro", "numero", "cep", "bairro")

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

  # parse(query_match_case)

  DBI::dbExecute(con, query_match_case)

  # add precision column to output
  add_precision_col(
    con,
    update_tb = output_tb,
    precision = precision
  )
}






#' Add a column with info of geocode precision
#'
#' @param con A db connection
#' @param update_tb String. Name of a table to be updated in con
#' @param precision Integer. An integer
#'
#' @return Adds a new column to a table in con
#'
#' @keywords internal
add_precision_col <- function(con, update_tb = NULL, precision = NULL){

  # update_tb = "output_caso_01"
  # precision = 1L

  # create empty column
  DBI::dbExecute(con,
                 sprintf("ALTER TABLE %s ADD COLUMN precision INTEGER", update_tb)
  )

  DBI::dbExecute(con,
                 sprintf("UPDATE %s SET precision = %s", update_tb, precision)
                 )
}













#' Merge results of spatial coordinates and precision info for output
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
merge_results <- function(con, x, y, key_column, select_columns){

  # x = 'output_db'
  # y = 'output_caso_01'
  # key_column = 'ID'
  # select_columns = c('lon', 'lat', 'precision')


  # Create the SELECT clause dynamically
  # select_x <- paste0(x, '.', c('lon', 'lat', 'precision '), collapse = ', ')
  select_x <- paste0(x, '.', c('* '), collapse = ', ')
  select_clause <- paste0(
    select_x,
    paste0(", ", y, ".", select_columns, collapse = ", ")
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




