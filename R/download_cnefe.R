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
#' download_cnefe(abbrev_state = c("AC", "AL", "RJ"),
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

  # data available for the states:
  if (isFALSE(all(abbrev_state %in% all_abbrev_states))) { stop(paste0("Data currently only available for the states ",
                                             paste(all_abbrev_states, collapse = " ")))}

  # if null, get all states
  if (is.null(abbrev_state)) {abbrev_state <- all_abbrev_states}

  ### build url of requested states
  file_url <- paste0("https://github.com/ipeaGIT/geocodebr/releases/download/data_",
                     geocodebr_env$data_release, "/abbrev_state_",abbrev_state, ".zip")


  # determine states that have already been downloaded
  existing_local_dirs <- fs::path(list.files(geocodebr_env$cache_dir, full.names = TRUE))
  existing_local_states <- substring(basename(existing_local_dirs), 14,15)


  ## if cache == FALSE, remove subfolders of states that exist locally
  # remove directories with parquet files
  if (isFALSE(cache)) {
    for (i in abbrev_state){
      temp <- existing_local_dirs[which(existing_local_dirs %like% i)]
      unlink(temp, recursive = TRUE)
    }
  }

  # if cache == TRUE, ignore urls of states that exist locally
  if (isTRUE(cache)) {
    url_of_local_states <- paste0("https://github.com/ipeaGIT/geocodebr/releases/download/data_",
              geocodebr_env$data_release, "/abbrev_state_",existing_local_states, ".zip")

    # ignore urls
    file_url <- file_url[! file_url %in% url_of_local_states]
  }


  ### Download
  if (length(file_url)>0){
    download_success <- download_file(
      file_url = file_url,
      showProgress = showProgress,
      cache = cache
      )
  }

  # check if download worked
  if (isFALSE(download_success)) { return(invisible(NULL)) }

  # unzip
  zipped_locals <- list.files(
    geocodebr_env$cache_dir,
    pattern = '.zip',
    full.names = TRUE
    )

  if (length(zipped_locals)>0) {
    lapply(X=zipped_locals,
         FUN = archive::archive_extract,
         dir = fs::path(geocodebr_env$cache_dir))

    # remove zipped files
    unlink(zipped_locals, recursive = TRUE)
  }

  return(download_success)
}

