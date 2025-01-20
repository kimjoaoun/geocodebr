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

  # update_tb = 'input_padrao_db'
  # reference_tb = 'output_caso_1'

  query_remove_matched <- glue::glue("
    DELETE FROM {update_tb}
    WHERE tempidgeocodebr IN (SELECT tempidgeocodebr FROM {reference_tb});")

  DBI::dbExecute(con, query_remove_matched)
}


#' Add a column with info of geocode match_type
#'
#' @param con A db connection
#' @param update_tb String. Name of a table to be updated in con
#'
#' @return Adds a new column to a table in con
#'
#' @keywords internal
add_precision_col <- function(con, update_tb = NULL){

  # update_tb = "output_db"

  # add empty column
  DBI::dbExecute(
    con,
    glue::glue("ALTER TABLE {update_tb} ADD COLUMN precision TEXT;")
  )

  # populate column
  query_precision <- glue::glue("
  UPDATE {update_tb}
  SET precision = CASE
  WHEN match_type IN ('en01', 'en02', 'en03', 'en04',
                      'pn01', 'pn02', 'pn03', 'pn04') THEN 'number'
  WHEN match_type IN ('ei01', 'ei02', 'ei03', 'ei04',
                      'pi01', 'pi02', 'pi03', 'pi04') THEN 'number_approximation'
  WHEN match_type IN ('er01', 'er02', 'er03', 'er04',
                      'pr01', 'pr02', 'pr03', 'pr04') THEN 'street'
  WHEN match_type IN ('ec01', 'ec02') THEN 'cep'
  WHEN match_type = 'eb01' THEN 'neighborhood'
  WHEN match_type = 'em01' THEN 'municipality'
  ELSE NULL
  END;")

  DBI::dbExecute( con, query_precision )
}





merge_results <- function(con,
                          x,
                          y,
                          key_column,
                          select_columns,
                          full_results){

  # x = 'output_db'
  # y = 'output_caso_01'
  # key_column = 'tempidgeocodebr'
  select_columns_y = c('lat', 'lon', 'match_type', 'precision', 'matched_address')

  if (isFALSE(full_results)) {
    select_columns_y <- select_columns_y[select_columns_y != 'matched_address']
  }

  # drop temp id column
  select_columns <- select_columns[select_columns!='tempidgeocodebr']

  # Create the SELECT clause dynamically
  select_x <- paste0(x, '.', c(select_columns), collapse = ', ')

  select_clause <- paste0(
    select_x, ',',
    paste0(y, ".", select_columns_y, collapse = ", ")
    )

  join_condition <- paste(
    glue::glue("{x}.{key_column} = {y}.{key_column}"),
    collapse = ' ON '
  )

  # Create the SQL query
  query <- glue::glue(
    "SELECT {select_clause}
      FROM {x}
      LEFT JOIN {y}
      ON {join_condition} "
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


get_relevant_cols_arrow <- function(case) {
  relevant_cols <- if (case %in% c('en01', 'pn01') ) {
    c("estado", "municipio", "logradouro_sem_numero", "numero", "cep", "localidade")
  } else if (case %in% c('en02', 'pn02')) {
    c("estado", "municipio", "logradouro_sem_numero", "numero", "cep")
  } else if (case %in% c('en03', 'pn03')) {
    c("estado", "municipio", "logradouro_sem_numero", "numero", "localidade")
  } else if (case %in% c('en04', 'pn04')) {
    c("estado", "municipio", "logradouro_sem_numero", "numero")
  } else if (case %in% c('er01', 'pr01', 'ei01', 'pi01')) {
    c("estado", "municipio", "logradouro_sem_numero", "cep", "localidade")
  } else if (case %in% c('er02', 'pr02', 'ei02', 'pi02')) {
    c("estado", "municipio", "logradouro_sem_numero", "cep")
  } else if (case %in% c('er03', 'pr03', 'ei03', 'pi03')) {
    c("estado", "municipio", "logradouro_sem_numero", "localidade")
  } else if (case %in% c('er04', 'pr04', 'ei04', 'pi04')) {
    c("estado", "municipio", "logradouro_sem_numero")
  } else if (case == 'ec01') {
    c("estado", "municipio", "cep", "localidade")
  } else if (case == 'ec02') {
    c("estado", "municipio", "cep")
  } else if (case == 'eb01') {
    c("estado", "municipio", "localidade")
  } else if (case == 'em01') {
    c("estado", "municipio")
  }

  return(relevant_cols)
}


all_possible_match_types <- c(
  "en01", "en02", "en03", "en04",
  "ei01", "ei02", "ei03", "ei04",
  "er01", "er02", "er03", "er04",
  #  "pn01", "pn02", "pn03", "pn04",  # we're not working with probabilistic matching yet
  #  "pi01", "pi02", "pi03", "pi04",  # we're not working with probabilistic matching yet
  #  "pr01", "pr02", "pr03", "pr04",  # we're not working with probabilistic matching yet
  "ec01", "ec02", "eb01", "em01"
)

number_interpolation_types <- c(
  "ei01", "ei02", "ei03", "ei04",
  "pi01", "pi02", "pi03", "pi04"
)

number_exact_types <- c(
  "en01", "en02", "en03", "en04",
  "pn01", "pn02", "pn03", "pn04"
)

possible_match_types_no_number <- c(
  "er01", "er02", "er03", "er04",
  "pr01", "pr02", "pr03", "pr04",
  "ec01", "ec02", "eb01", "em01"
)

possible_match_types_no_logradouro <- c(
  "ec01", "ec02", "eb01", "em01"
)


probabilistic_logradouro_match_types <- c(
  "pn01", "pn02", "pn03", "pn04",  # we're not working with probabilistic matching yet
  "pi01", "pi02", "pi03", "pi04",  # we're not working with probabilistic matching yet
  "pr01", "pr02", "pr03", "pr04"  # we're not working with probabilistic matching yet
)




assert_and_assign_address_fields <- function(address_fields, addresses_table) {
  possible_fields <- c(
    "logradouro", "numero", "cep", "bairro", "municipio", "estado"
  )

  col <- checkmate::makeAssertCollection()
  checkmate::assert_names(
    names(address_fields),
    type = "unique",
    subset.of = possible_fields,
    add = col
  )
  checkmate::assert_names(
    address_fields,
    subset.of = names(addresses_table),
    add = col
  )
  checkmate::reportAssertions(col)

  missing_fields <- setdiff(possible_fields, names(address_fields))

  missing_fields_list <- vector(mode = "list", length = length(missing_fields))
  names(missing_fields_list) <- missing_fields

  complete_fields_list <- append(as.list(address_fields), missing_fields_list)

  return(complete_fields_list)
}



get_relevant_cols <- function(case) {
  relevant_cols <- if (case == 1) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "numero_padr", "cep_padr", "bairro_padr")
  } else if (case == 2) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "numero_padr", "cep_padr")
  } else if (case == 3) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "numero_padr", "bairro_padr")
  } else if (case == 4) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "numero_padr")
  } else if (case == 5) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "cep_padr", "bairro_padr")
  } else if (case == 6) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "cep_padr")
  } else if (case == 7) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "bairro_padr")
  } else if (case == 8) {
    c("estado_padr", "municipio_padr", "logradouro_padr")
  } else if (case == 9) {
    c("estado_padr", "municipio_padr", "cep_padr", "bairro_padr")
  } else if (case == 10) {
    c("estado_padr", "municipio_padr", "cep_padr")
  } else if (case == 11) {
    c("estado_padr", "municipio_padr", "bairro_padr")
  } else if (case == 12) {
    c("estado_padr", "municipio_padr")
  }

  return(relevant_cols)
}





get_relevant_cols_dani_arrow <- function(case) {
  relevant_cols <- if (case %in% c('en01', 'pn01') ) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "numero_padr", "cep_padr", "bairro_padr")
  } else if (case %in% c('en02', 'pn02')) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "numero_padr", "cep_padr")
  } else if (case %in% c('en03', 'pn03')) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "numero_padr", "bairro_padr")
  } else if (case %in% c('en04', 'pn04')) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "numero_padr")
  } else if (case %in% c('er01', 'pr01', 'ei01', 'pi01')) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "cep_padr", "bairro_padr")
  } else if (case %in% c('er02', 'pr02', 'ei02', 'pi02')) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "cep_padr")
  } else if (case %in% c('er03', 'pr03', 'ei03', 'pi03')) {
    c("estado_padr", "municipio_padr", "logradouro_padr", "bairro_padr")
  } else if (case %in% c('er04', 'pr04', 'ei04', 'pi04')) {
    c("estado_padr", "municipio_padr", "logradouro_padr")
  } else if (case == 'ec01') {
    c("estado_padr", "municipio_padr", "cep_padr", "bairro_padr")
  } else if (case == 'ec02') {
    c("estado_padr", "municipio_padr", "cep_padr")
  } else if (case == 'eb01') {
    c("estado_padr", "municipio_padr", "bairro_padr")
  } else if (case == 'em01') {
    c("estado_padr", "municipio_padr")
  }

  return(relevant_cols)
}
