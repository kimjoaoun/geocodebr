#' Download the CNEFE data set
#'
#' Downloads an enriched version of the CNEFE (National Registry of Addresses
#' for Statistical Purposes, in portuguese) data set, purposefully built to be
#' used with this package.
#'
#' @param abbrev_state A character vector. The states whose CNEFE data should be
#'   downloaded. Either `"all"` (the default), in which case the data for all
#'   states is downloaded, or a vector with the state abbreviations (e.g.
#'   `c("RJ", "DF")` to download the data for Rio de Janeiro and the Federal
#'   District).
#' @template showProgress
#' @template cache
#'
#' @return A directory path where the data was saved.
#'
#' @family Support
#'
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#' # download CNEFE for a single state
#' download_cnefe(abbrev_state = "AC",
#'                showProgress = FALSE)
#'
#' # download CNEFE for multiple states
#' download_cnefe(abbrev_state = c("AC", "AL", "RJ"),
#'                showProgress = FALSE)
#'
#' # # download CNEFE for all states
#' # download_cnefe(abbrev_state = "all",
#' #                showProgress = FALSE)
#'
#' @export
download_cnefe <- function(abbrev_state = "all",
                           showProgress = TRUE,
                           cache = TRUE){
  checkmate::assert_logical(showProgress, any.missing = FALSE, len = 1)
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1)
  abbrev_state <- assert_and_assign_abbrev_state(abbrev_state)

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
  } else { download_success <- TRUE}

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

assert_and_assign_abbrev_state <- function(abbrev_state) {
  all_abbrev_states <- c(
    "RO", "AC", "AM", "RR", "PA", "AP", "TO", "MA", "PI", "CE", "RN", "PB",
    "PE", "AL", "SE", "BA", "MG", "ES", "RJ", "SP", "PR", "SC", "RS", "MS",
    "MT", "GO", "DF"
  )

  checkmate::assert_names(
    abbrev_state,
    subset.of = c("all", all_abbrev_states)
  )

  if ("all" %in% abbrev_state) abbrev_state <- all_abbrev_states

  return(abbrev_state)
}
