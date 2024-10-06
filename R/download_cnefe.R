#' Download the geocodebr version of the CNEFE data set
#'
#' @description
#' Downloads an enriched version of the CNEFE data set that has been
#' purposefully built to be used in the geocodebr package.
#'
#' @param abbrev_state description year
#' @template showProgress
#' @template cache
#'
#' @return A directory path where the data was saved.
#' @export
#' @family Support
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#' # download CNEFE for a single state
#' download_cnefe(abbrev_state = "AC",
#'                showProgress = FALSE)
#'
#' # download CNEFE for multiple states
#' download_cnefe(abbrev_state = c("AC", "AL"),
#'                showProgress = FALSE)
#'
#' # download CNEFE for all states
#' download_cnefe(abbrev_state = "all",
#'                showProgress = FALSE)
#'
download_cnefe <- function(abbrev_state = NULL,
                           showProgress = TRUE,
                           cache = TRUE
                           ){

  ### check inputs
  checkmate::assert_logical(showProgress)
  checkmate::assert_logical(cache)

  all_abbrev_states <- c("RO", "AC", "AM", "RR", "PA", "AP", "TO", "MA", "PI",
                         "CE", "RN", "PB", "PE", "AL", "SE", "BA", "MG", "ES",
                         "RJ", "SP", "PR", "SC", "RS", "MS", "MT", "GO", "DF")

  # data available for the years:
  if (isFALSE(all(abbrev_state %in% all_abbrev_states))) { stop(paste0("Data currently only available for the states ",
                                             paste(all_abbrev_states, collapse = " ")))}

  # if null, get all states
  if (is.null(abbrev_state)) {abbrev_state <- all_abbrev_states}

  ### Get url
  file_url <- paste0("https://github.com/ipeaGIT/geocodebr/releases/download/data_",
                     geocodebr_env$data_release, "/abbrev_state_",abbrev_state, ".zip")


  # if cache == TRUE, ignore urls of states that exist locally
  if (isTRUE(cache)) {
    existing_local_dirs <- fs::path(list.files(geocodebr_env$cache_dir, full.names = TRUE))
    url_of_local_states <- substring(basename(existing_local_dirs), 14,15)
    url_of_local_states <- paste0("https://github.com/ipeaGIT/geocodebr/releases/download/data_",
              geocodebr_env$data_release, "/abbrev_state_",url_of_local_states, ".zip")

    # ignore urls
    file_url <- file_url[! file_url %in% url_of_local_states]
  }

  # if cache == FALSE, remove subfolders of states that exist locally
  if (isFALSE(cache)) {
    existing_local_dirs <- fs::path(list.files(geocodebr_env$cache_dir, full.names = TRUE))

    for (i in existing_local_dirs){
      state_dir <- substr(i, nchar(i)-1, nchar(i))
      if (state_dir %in% abbrev_state) {unlink(i, recursive = TRUE)}
    }
  }

  ### Download
  local_files <- download_file(file_url = file_url,
                              showProgress = showProgress,
                              cache = cache)

  # check if download worked
  if(is.null(local_files)) { return(invisible(NULL)) }

  # unzip
  lapply(X=local_files,
         FUN = archive::archive_extract,
         dir = geocodebr_env$cache_dir)

  # remove zipped files
  unlink(local_files, recursive = TRUE)

  # get path to parquet subdirs
  parquet_dirs <- fs::dir_ls(
    path = geocodebr_env$cache_dir,
    type = 'directory'
    )

  parquet_dirs <- lapply(abbrev_state,
         FUN = function(uf){
           temp <- parquet_dirs[data.table::like(parquet_dirs, uf)]
           return(temp)
           }
         )

  parquet_dirs <- unlist(parquet_dirs) |> unname()
  return(parquet_dirs)
}

