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
#' @param input_padrao A data.table with standardized addresses.
#' @return Adds a new column in place
#'
#' @keywords internal
add_abbrev_state_col <- function(dt){

  data.table::setDT(dt)

  # add abbrev state
  dt[, abbrev_state := data.table::fcase(
    estado == "RONDONIA", "RO",
    estado == "ACRE", "AC",
    estado == "AMAZONAS", "AM",
    estado == "RORAIMA", "RR",
    estado == "PARA", "PA",
    estado == "AMAPA", "AP",
    estado == "TOCANTINS", "TO",
    estado == "MARANHAO", "MA",
    estado == "PIAUI", "PI",
    estado == "CEARA", "CE",
    estado == "RIO GRANDE DO NORTE", "RN",
    estado == "PARAIBA", "PB",
    estado == "PERNAMBUCO", "PE",
    estado == "ALAGOAS", "AL",
    estado == "SERGIPE", "SE",
    estado == "BAHIA", "BA",
    estado == "MINAS GERAIS", "MG",
    estado == "ESPIRITO SANTO", "ES",
    estado == "RIO DE JANEIRO", "RJ",
    estado == "SAO PAULO", "SP",
    estado == "PARANA", "PR",
    estado == "SANTA CATARINA", "SC",
    estado == "RIO GRANDE DO SUL", "RS",
    estado == "MATO GROSSO DO SUL", "MS",
    estado == "MATO GROSSO", "MT",
    estado == "GOIAS", "GO",
    estado == "DISTRITO FEDERAL", "DF"
  )]

}




#' Update input_padrao to remove observations previously matched
#'
#' @param con A db connection
#' @param remove_from A table written in con
#' @return Drops observations from input_padrao
#'
#' @keywords internal
update_input_db <- function(con, remove_from = NULL){ # remove_from = 'output_caso_1'
  query_remove_matched <- sprintf("
    DELETE FROM input_padrao
    WHERE ID IN (SELECT ID FROM %s)", remove_from)
  DBI::dbExecute(con, query_remove_matched)
}
